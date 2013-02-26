/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Declarations for ApplicationQueue.
 * This file contains type declarations for ApplcationQueue, a base class for
 * an application queue.
 */

#ifndef HYPERTABLE_APPLICATIONQUEUE_H
#define HYPERTABLE_APPLICATIONQUEUE_H

#include <cassert>
#include <list>
#include <map>
#include <vector>

#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Thread.h"
#include "Common/Mutex.h"
#include "Common/HashMap.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"
#include "Common/Logger.h"

#include "ApplicationQueueInterface.h"
#include "ApplicationHandler.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /**
   * Application queue.  
   * Helper class for use by server applications that are driven by messages
   * received over the network.  This class can be used in conjunction with the
   * ApplicationHandler class to implement an incoming request queue.  Worker
   * threads pull handlers (requests) off the queue and carry them out.  The
   * following features are supported:
   *
   * <b>Groups</b>
   *
   * Because a set of worker threads pull requests from the queue and carry
   * them out independently, it is possible for requests to get executed out
   * of order relative to the order in which they arrived in the queue.  This
   * can cause problems for certain request sequences such as appending data
   * to a file in the DfsBroker, or fetching scanner results from a scanner
   * using multiple readahead requests.  <i>Groups</i> are a way to give
   * applications the ability to serialize a set of requests.  Each request
   * has a <i>group ID</i> that is returned by 
   * the ApplicationHandler#get_group_id method.  Requests that have the
   * same group ID will get executed in series, in the order in which they
   * arrived in the application queue.  Requests with group ID 0 don't belong to
   * any group and will get executed independently with no serialization
   * order.
   *
   * <b>Prioritization</b>
   *
   * The ApplicationQueue supports two-level request prioritization.  Requests
   * can be designated as <i>urgent</i> which will cause them to be executed
   * before other non-urgent requests.  Urgent requests will also be executed
   * even when the ApplicationQueue has been paused.  In Hypertable, METADATA
   * scans and updates are marked urgent which allows them procede and prevent
   * deadlocks when the application queue gets paused due to low memory
   * condition in the RangeServer.  The ApplicationHandler#is_urgent
   * method is used to signal if a request is urgent.
   */
  class ApplicationQueue : public ApplicationQueueInterface {

    /** Tracks group execution state.
     * A GroupState object is created for each unique group ID to track the
     * queue execution state of requests in the group.
     */
    class GroupState {
    public:
      GroupState() : group_id(0), running(false), outstanding(1) { return; }
      uint64_t group_id;    //!< Group ID
      bool     running;     //!< <i>True</i> if a request from this group is
                            //!< being executed
      int      outstanding; //!< Number of outstanding (uncompleted) requests
                            //!< in queue for this group
    };

    /** Hash map of thread group ID to GroupState
     */ 
    typedef hash_map<uint64_t, GroupState *> GroupStateMap;

    /** Request record.
     */
    class RequestRec {
    public:
      RequestRec(ApplicationHandler *arh) : handler(arh), group_state(0) { return; }
      ~RequestRec() { delete handler; }
      ApplicationHandler *handler; //!< Pointer to ApplicationHandler
      GroupState *group_state;     //!< Pointer to GroupState to which request belongs
    };

    /** Individual request queue
     */
    typedef std::list<RequestRec *> RequestQueue;

    /** Application queue state object shared among worker threads.
     */
    class ApplicationQueueState {
    public:
      ApplicationQueueState() : threads_available(0), shutdown(false),
                                paused(false) { }
      RequestQueue           queue;
      RequestQueue           urgent_queue;
      GroupStateMap       group_state_map;
      Mutex               mutex;
      boost::condition    cond;
      boost::condition    quiesce_cond;
      size_t              threads_available;
      size_t              threads_total;
      bool                shutdown;
      bool                paused;
    };

    /** Application queue worker thread function (functor)
     */
    class Worker {

    public:
      Worker(ApplicationQueueState &qstate, bool one_shot=false) 
      : m_state(qstate), m_one_shot(one_shot) { return; }

      void operator()() {
        RequestRec *rec = 0;
        RequestQueue::iterator iter;

        while (true) {
          {
            ScopedLock lock(m_state.mutex);

            m_state.threads_available++;
            while ((m_state.paused || m_state.queue.empty()) &&
                   m_state.urgent_queue.empty()) {
              if (m_state.shutdown) {
                m_state.threads_available--;
                return;
              }
              if (m_state.threads_available == m_state.threads_total)
                m_state.quiesce_cond.notify_all();
              m_state.cond.wait(lock);
            }

            if (m_state.shutdown) {
              m_state.threads_available--;
              return;
            }

            rec = 0;

            iter = m_state.urgent_queue.begin();
            while (iter != m_state.urgent_queue.end()) {
              rec = (*iter);
              if (rec->group_state == 0 || !rec->group_state->running) {
                if (rec->group_state)
                  rec->group_state->running = true;
                m_state.urgent_queue.erase(iter);
                break;
              }
              if (!rec->handler || rec->handler->is_expired()) {
                iter = m_state.urgent_queue.erase(iter);
                remove_expired(rec);
              }
              rec = 0;
              iter++;
            }

            if (rec == 0 && !m_state.paused) {
              iter = m_state.queue.begin();
              while (iter != m_state.queue.end()) {
                rec = (*iter);
                if (rec->group_state == 0 || !rec->group_state->running) {
                  if (rec->group_state)
                    rec->group_state->running = true;
                  m_state.queue.erase(iter);
                  break;
                }
                if (!rec->handler || rec->handler->is_expired()) {
                  iter = m_state.queue.erase(iter);
                  remove_expired(rec);
                }
                rec = 0;
                iter++;
              }
            }

            if (rec == 0 && !m_one_shot) {
              if (m_state.shutdown) {
                m_state.threads_available--;
                return;
              }
              m_state.cond.wait(lock);
              if (m_state.shutdown) {
                m_state.threads_available--;
                return;
              }
            }

            m_state.threads_available--;
          }

          if (rec) {
            if (rec->handler)
              rec->handler->run();
            remove(rec);
            if (m_one_shot)
              return;
          }
          else if (m_one_shot)
            return;
        }

        HT_INFO("thread exit");
      }

    private:

      void remove(RequestRec *rec) {
        if (rec->group_state) {
          ScopedLock ulock(m_state.mutex);
          rec->group_state->running = false;
          rec->group_state->outstanding--;
          if (rec->group_state->outstanding == 0) {
            m_state.group_state_map.erase(rec->group_state->group_id);
            delete rec->group_state;
          }
        }
        delete rec;
      }

      void remove_expired(RequestRec *rec) {
        if (rec->group_state) {
          rec->group_state->outstanding--;
          if (rec->group_state->outstanding == 0) {
            m_state.group_state_map.erase(rec->group_state->group_id);
            delete rec->group_state;
          }
        }
        delete rec;
      }

      ApplicationQueueState &m_state;
      bool m_one_shot;
    };

    ApplicationQueueState  m_state;
    ThreadGroup            m_threads;
    std::vector<Thread::id> m_thread_ids;
    bool joined;
    bool m_dynamic_threads;

  public:

    /**
     * Default constructor used by derived classes only
     */
    ApplicationQueue() : joined(true) {}

    /**
     * Constructor initialized with worker thread count.
     * This constructor sets up the application queue with a number of worker
     * threads specified by <code>worker_count</code>.
     * @param worker_count Number of worker threads to create
     * @param dynamic_threads Dynamically create temporary thread to carry out
     * requests if none available.
     */
    ApplicationQueue(int worker_count, bool dynamic_threads=true) 
      : joined(false), m_dynamic_threads(dynamic_threads) {
      m_state.threads_total = worker_count;
      Worker Worker(m_state);
      assert (worker_count > 0);
      for (int i=0; i<worker_count; ++i) {
        m_thread_ids.push_back(m_threads.create_thread(Worker)->get_id());
      }
      //threads
    }

    /** Destructor.
     */
    virtual ~ApplicationQueue() {
      if (!joined) {
        shutdown();
        join();
      }
    }

    /**
     * Returns all the thread IDs for this threadgroup
     * @return vector of Thread::id
     */
    std::vector<Thread::id> get_thread_ids() const {
      return m_thread_ids;
    }

    /**
     * Shuts down the application queue.  All outstanding requests are carried
     * out and then all threads exit.  #join can be called to wait for
     * completion of the shutdown.
     */
    void shutdown() {
      m_state.shutdown = true;
      m_state.cond.notify_all();
    }

    /** Wait for queue to become idle (with timeout).
     * @param deadline Return by this time if queue does not become idle
     * @param reserve_threads Number of threads that can be active when queue is
     * idle
     * @return <i>false</i> if <code>deadline</code> was reached before queue
     * became idle, <i>true</i> otherwise
     */
    bool wait_for_idle(boost::xtime &deadline, int reserve_threads=0) {
      ScopedLock lock(m_state.mutex);
      while (m_state.threads_available < (m_state.threads_total-reserve_threads)) {
        if (!m_state.quiesce_cond.timed_wait(lock, deadline))
          return false;
      }
      return true;
    }

    /**
     * Waits for a shutdown to complete.  This method returns when all
     * application queue threads exit.
     */
    void join() {
      if (!joined) {
        m_threads.join_all();
        joined = true;
      }
    }

    /** Starts application queue.
     */
    void start() {
      ScopedLock lock(m_state.mutex);
      m_state.paused = false;
      m_state.cond.notify_all();
    }

    /** Stops (pauses) application queue, preventing non-urgent requests from
     * being executed.  Any requests that are being executed at the time of the
     * call will complete.
     */
    void stop() {
      ScopedLock lock(m_state.mutex);
      m_state.paused = true;
    }

    /**
     * Adds a request (application request handler) to the application queue.
     * The request queue is designed to support the serialization of related
     * requests.  Requests are related by the thread group ID value in the
     * ApplicationHandler.  This thread group ID is constructed in the
     * Event object
     */
    virtual void add(ApplicationHandler *app_handler) {
      GroupStateMap::iterator uiter;
      uint64_t group_id = app_handler->get_group_id();
      RequestRec *rec = new RequestRec(app_handler);
      rec->group_state = 0;

      HT_ASSERT(app_handler);

      if (group_id != 0) {
        ScopedLock ulock(m_state.mutex);
        if ((uiter = m_state.group_state_map.find(group_id))
            != m_state.group_state_map.end()) {
          rec->group_state = (*uiter).second;
          rec->group_state->outstanding++;
        }
        else {
          rec->group_state = new GroupState();
          rec->group_state->group_id = group_id;
          m_state.group_state_map[group_id] = rec->group_state;
        }
      }

      {
        ScopedLock lock(m_state.mutex);
        if (app_handler->is_urgent()) {
          m_state.urgent_queue.push_back(rec);
          if (m_dynamic_threads && m_state.threads_available == 0) {
            Worker worker(m_state, true);
            Thread t(worker);
          }
        }
        else
          m_state.queue.push_back(rec);
        m_state.cond.notify_one();
      }
    }

    virtual void add_unlocked(ApplicationHandler *app_handler) {
      add(app_handler);
    }
  };

  /// Smart pointer to ApplicationQueue object
  typedef boost::intrusive_ptr<ApplicationQueue> ApplicationQueuePtr;
  /** @}*/
} // namespace Hypertable

#endif // HYPERTABLE_APPLICATIONQUEUE_H
