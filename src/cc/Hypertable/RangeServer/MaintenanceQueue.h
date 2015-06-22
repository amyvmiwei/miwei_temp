/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

/// @file
/// Declarations for MaintenanceQueue
/// This file contains the type declarations for the MaintenanceQueue

#ifndef Hypertable_RangeServer_MaintenanceQueue_h
#define Hypertable_RangeServer_MaintenanceQueue_h

#include "MaintenanceTask.h"
#include "MaintenanceTaskMemoryPurge.h"

#include <AsyncComm/Clock.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Thread.h>

#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <set>

#define MAX_LEVELS 4

namespace Hypertable {

  /** @addtogroup RangeServer
   *  @{
   */

  /** Queue for periodic maintenance work
   */
  class MaintenanceQueue {

    static int ms_pause;
    static std::condition_variable ms_cond;

    struct LtMaintenanceTask {
      bool
      operator()(const MaintenanceTask *sm1, const MaintenanceTask *sm2) const {
	if (sm1->level != sm2->level)
	  return sm1->level > sm2->level;
	if (sm1->priority != sm2->priority)
	  return sm1->priority > sm2->priority;
        return sm1->start_time >= sm2->start_time;
      }
    };

    typedef std::priority_queue<MaintenanceTask *,
            std::vector<MaintenanceTask *>, LtMaintenanceTask> TaskQueue;

    class MaintenanceQueueState {
    public:
      MaintenanceQueueState() {
        memset(inflight_levels, 0, MAX_LEVELS*sizeof(int));
        return;
      }
      TaskQueue queue;
      std::mutex mutex;
      std::condition_variable cond;
      std::condition_variable empty_cond;
      bool shutdown {};
      std::set<Range *>  ranges;
      uint32_t inflight_levels[MAX_LEVELS];
      uint32_t inflight {};
      int64_t generation {};
    };

    class Worker {

    public:

      Worker(MaintenanceQueueState &state) : m_state(state) { return; }

      void operator()() {
        MaintenanceTask *task = 0;

        while (true) {

          {
            std::unique_lock<std::mutex> lock(m_state.mutex);
            uint32_t inflight_level = lowest_inflight_level();

            auto now = std::chrono::steady_clock::now();

	    // Block in the following circumstances:
	    // 1. Queue is empty
	    // 2. Level of task on front of queue is greater than (e.g. lower
	    //    priority) the level of the tasks currently being executed
	    // 3. Start time of task on front of queue is in the future

            while (m_state.queue.empty() || 
		   (m_state.inflight && ((m_state.queue.top())->level > inflight_level)) ||
                   (m_state.queue.top())->start_time > now) {

              if (m_state.shutdown)
                return;

              if (m_state.queue.empty() || 
		  (m_state.inflight && (m_state.queue.top())->level > inflight_level))
                m_state.cond.wait(lock);
              else
                m_state.cond.wait_until(lock, (m_state.queue.top())->start_time);

              inflight_level = lowest_inflight_level();
              now = std::chrono::steady_clock::now();
            }

            task = m_state.queue.top();
	    m_state.queue.pop();

            HT_ASSERT(task->level < MAX_LEVELS);

	    m_state.inflight++;
            m_state.inflight_levels[task->level]++;
          }

          try {

            // maybe pause
            {
              std::unique_lock<std::mutex> lock(m_state.mutex);
              ms_cond.wait(lock, [](){ return !ms_pause; });
            }

            if (m_state.shutdown)
              return;

            task->execute();

          }
          catch(Hypertable::Exception &e) {
            if (e.code() != Error::RANGESERVER_RANGE_NOT_ACTIVE &&
                dynamic_cast<MaintenanceTaskMemoryPurge *>(task) == 0) {
              bool message_logged = false;
              if (e.code() != Error::RANGESERVER_RANGE_BUSY) {
                HT_ERROR_OUT << e << HT_END;
                message_logged = true;
              }
              if (task->retry()) {
                std::lock_guard<std::mutex> lock(m_state.mutex);
                HT_INFOF("Maintenance Task '%s' aborted, will retry in %u "
                         "milliseconds ...", task->description().c_str(),
                        task->get_retry_delay());
                task->start_time = std::chrono::steady_clock::now();
                task->start_time += std::chrono::milliseconds(task->get_retry_delay());
                m_state.queue.push(task);
                HT_ASSERT(m_state.inflight_levels[task->level] > 0);
                m_state.inflight_levels[task->level]--;
		m_state.inflight--;
                m_state.cond.notify_one();
                continue;
              }
              if (!message_logged)
                HT_INFOF("Maintenance Task '%s' failed because range is busy, dropping task ...",
                         task->description().c_str());
            }
          }

          {
            std::lock_guard<std::mutex> lock(m_state.mutex);
            HT_ASSERT(m_state.inflight_levels[task->level] > 0);
            m_state.inflight_levels[task->level]--;
            m_state.inflight--;
	    m_state.generation++;
	    if (task->get_range())
	      m_state.ranges.erase(task->get_range());
	    if (m_state.queue.empty()) {
              if (m_state.inflight == 0)
                m_state.empty_cond.notify_all();
            }
            else if ((m_state.queue.top())->level > task->level)
              m_state.cond.notify_all();              
          }

          delete task;
        }
      }

    private:

      /** Determine the lowest level of the tasks currently being executed.
       * This method iterates through the MaintenanceQueueState#inflight_levels
       * array and stops at the first non-zero count and returns the
       * corresponding level.
       * @return Lowest level of tasks currently being executed.
       */
      uint32_t lowest_inflight_level() {
        for (uint32_t i=0; i<MAX_LEVELS; i++) {
          if (m_state.inflight_levels[i] > 0)
            return i;
        }
        return MAX_LEVELS;
      }

      MaintenanceQueueState &m_state;
    };

    MaintenanceQueueState  m_state;
    ThreadGroup m_threads;
    int  m_worker_count;
    bool joined;

  public:

    /** Constructor to set up the maintenance queue.  It creates a number
     * of worker threads specified by the worker_count argument.
     *
     * @param worker_count number of worker threads to create
     */
    MaintenanceQueue(int worker_count) : m_worker_count(worker_count),
					 joined(false) {
      Worker Worker(m_state);
      assert (worker_count > 0);
      for (int i=0; i<worker_count; ++i)
        m_threads.create_thread(Worker);
      //threads
    }

    /** Shuts down the maintenance queue.  All "in flight" requests are carried
     * out and then all threads exit.  #join can be called to wait for
     * completion of the shutdown.
     */
    void shutdown() {
      m_state.shutdown = true;
      m_state.cond.notify_all();
    }

    /** Waits for a shutdown to complete.  This method returns when all
     * maintenance queue threads exit.
     */
    void join() {
      if (!joined) {
        m_threads.join_all();
        joined = true;
      }
    }

    /** Stops (suspends) queue processing
     */
    void stop() {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      HT_INFO("Stopping maintenance queue");
      ms_pause++;
    }

    /** Starts queue processing
     */
    void start() {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      HT_ASSERT(ms_pause > 0);
      ms_pause--;
      if (ms_pause == 0) {
        ms_cond.notify_all();
        HT_INFO("Starting maintenance queue");
      }
    }

    /// Drops range maintenance tasks from the queue.
    /// @tparam _Function Predicate function accepting Range *
    /// @param __f Unary predicate function to determine which range tasks to
    /// drop
    template<typename _Function>
    void drop_range_tasks(_Function __f) {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      TaskQueue filtered_queue;
      MaintenanceTask *task = 0;
      while (!m_state.queue.empty()) {
	task = m_state.queue.top();
        m_state.queue.pop();
	if (task->get_range() && __f(task->get_range())) {
          m_state.ranges.erase(task->get_range());
	  delete task;
          m_state.generation++;
        }
	else
	  filtered_queue.push(task);
      }
      m_state.queue = filtered_queue;
    }

    /** Returns <i>true</i> if queue contains a maintenance task for
     * <code>range</code>.
     * @param range Pointer to Range object
     * @return <i>true</i> if queue contains a maintenance task for
     * <code>range</code>, <i>false</i> otherwise.
     */
    bool contains(Range *range) {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      return m_state.ranges.count(range) > 0;
    }

    /** Adds a maintenance task to the queue.  If the task has an associated
     * range, then it adds the range to the MaintenanceQueueState#ranges set.
     * @param task Maintenance task to add
     */
    void add(MaintenanceTask *task) {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      m_state.queue.push(task);
      if (task->get_range())
	m_state.ranges.insert(task->get_range());
      m_state.cond.notify_one();
    }

    /** Returns the size of the queue.
     * The size is computed as the queue size plus the number of tasks
     * <i>in flight</i>.
     * @return Size of queue
     */
    size_t size() {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      return (size_t)m_state.queue.size() + (size_t)m_state.inflight;
    }

    /** Returns queue generation number.
     * When the queue is created, the generation number is initialized to
     * zero.  Each time a task is successfully excecuted and removed from the
     * queue, the generation number is incremented by one.
     * @return Generation number
     */
    int64_t generation() {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      return m_state.generation;
    }

    /** Returns <i>true</i> if any tasks are in queue or all worker threads
     * are busy executing tasks.
     * @return <i>true</i> if queue is full, <i>false</i> otherwise
     */
    bool full() {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      return (m_state.queue.size() + (size_t)m_state.inflight) >=
        (size_t)m_worker_count;
    }

    /** Returns <i>true</i> if maintenance queue is empty.
     * @return <i>true</i> if queue is empty, <i>false</i> otherwise
     */
    bool empty() {
      std::lock_guard<std::mutex> lock(m_state.mutex);
      return m_state.queue.empty() && m_state.inflight == 0;
    }

    /** Waits for queue to become empty
     */
    void wait_for_empty() {
      std::unique_lock<std::mutex> lock(m_state.mutex);
      m_state.empty_cond.wait(lock, [this](){
          return m_state.queue.empty() && m_state.inflight == 0; });
    }

    /** Waits for queue to become empty with deadline
     * @param deadline Return if queue not empty by this absolute time
     * @return <i>true</i> if queue empty, <i>false</i> if deadline reached
     */
    bool wait_for_empty(ClockT::time_point deadline) {
      std::unique_lock<std::mutex> lock(m_state.mutex);
      return m_state.empty_cond.wait_until(lock, deadline, [this](){
          return m_state.queue.empty() && m_state.inflight == 0; });
    }

    /** Returns the number of worker threads configured for the queue.
     * @return Number of configured worker threads
     */
    int worker_count() { return m_worker_count; }

  };

  /// Smart pointer to MaintenanceQueue
  typedef std::shared_ptr<MaintenanceQueue> MaintenanceQueuePtr;

  /** @}*/

} // namespace Hypertable

#endif // Hypertable_RangeServer_MaintenanceQueue_h
