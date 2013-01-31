/*
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
 * Declarations for Reactor.
 * This file contains type declarations for Reactor, a class to manage state for
 * a polling thread.
 */

#ifndef HYPERTABLE_REACTOR_H
#define HYPERTABLE_REACTOR_H

#include <queue>
#include <set>
#include <vector>

#include <boost/thread/thread.hpp>

extern "C" {
#include <poll.h>
}

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "PollTimeout.h"
#include "RequestCache.h"
#include "ExpireTimer.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Socket descriptor poll state for use with POSIX <code>poll()</code>
   */
  typedef struct {
    /// Descriptor state structure passed into <code>poll()</code>
    struct pollfd pollfd;
    /// I/O handler associated with descriptor
    IOHandler *handler;
  } PollDescriptorT;

  /** Manages reactor (polling thread) state including poll interest, request cache,
   * and timers.
   */
  class Reactor : public ReferenceCount {

    friend class ReactorFactory;

  public:

    /** Poll interest abstraction constants.
     */
    enum PollInterest {
      READ_READY  = 0x01, /**< Read ready polling interest */
      WRITE_READY = 0x02  /**< Write ready polling interest */
    };

    /** Constructor.
     * Initializes polling interface and creates interrupt socket.
     * If ReactorFactory::use_poll is set to <i>true</i>, then the reactor will
     * use the POSIX <code>poll()</code> interface, otherwise <code>epoll</code>
     * is used on Linux, <code>kqueue</code> on OSX and FreeBSD, and
     * <code>port_associate</code> on Solaris.  For polling mechanisms that
     * do not provide an interface for breaking out of the poll wait, a UDP
     * socket #m_interrupt_sd is created (and connected to itself) and
     * added to the poll set.
     */
    Reactor();

    /** Destructor.
     */
    ~Reactor() {
      poll_loop_interrupt();
    }

    /** Adds a request to request cache and adjusts poll timeout if necessary.
     * @param id Request ID
     * @param handler I/O handler with which request is associated
     * @param dh Event handler for response MESSAGE events
     * @param expire Absolute expiration time
     */
    void add_request(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                     boost::xtime &expire) {
      ScopedLock lock(m_mutex);
      m_request_cache.insert(id, handler, dh, expire);
      if (m_next_wakeup.sec == 0 || xtime_cmp(expire, m_next_wakeup) < 0)
        poll_loop_interrupt();
    }

    /** Removes request associated with <code>id</code>
     * @return Pointer to request event handler
     */
    DispatchHandler *remove_request(uint32_t id) {
      ScopedLock lock(m_mutex);
      return m_request_cache.remove(id);
    }

    /** Cancels outstanding requests associated with <code>handler</code>
     * @param handler I/O handler for which outstanding requests are to be
     * cancelled
     * @param error Error code to deliver with ERROR events
     */
    void cancel_requests(IOHandler *handler, int32_t error=Error::COMM_BROKEN_CONNECTION) {
      ScopedLock lock(m_mutex);
      m_request_cache.purge_requests(handler, error);
    }

    /** Adds a timer.
     * Pushes timer onto #m_timer_heap and interrupts the polling loop so that
     * the poll timeout can be adjusted if necessary.
     * @param timer Reference to ExpireTimer object
     */
    void add_timer(ExpireTimer &timer) {
      ScopedLock lock(m_mutex);
      m_timer_heap.push(timer);
      poll_loop_interrupt();
    }

    /** Cancels timers associated with <code>handler</code>.
     * @param handler Event handler for which associated timers are to be cancelled
     */
    void cancel_timer(DispatchHandler *handler) {
      ScopedLock lock(m_mutex);
      typedef TimerHeap::container_type container_t;
      container_t container;
      container.reserve(m_timer_heap.size());
      ExpireTimer timer;
      while (!m_timer_heap.empty()) {
        timer = m_timer_heap.top();
        if (timer.handler != handler)
          container.push_back(timer);
        m_timer_heap.pop();
      }
      foreach_ht (const ExpireTimer &t, container)
        m_timer_heap.push(t);
    }

    /** Schedules <code>handler</code> for removal.
     * This method schedules an I/O handler for removal.  It should be called
     * only when the handler has been decomissioned in the HandlerMap and when
     * there are no outstanding references to the handler.  The handler is
     * added to the #m_removed_handlers set and a timer is set for 200
     * milliseconds in the future so that the ReactorRunner will wake up and
     * complete the removal by removing it completely from the HandlerMap and
     * delivering a DISCONNECT event if it is for a TCP socket.
     * @param handler I/O handler to schedule for removal
     */
    void schedule_removal(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      m_removed_handlers.insert(handler);
      ExpireTimer timer;
      boost::xtime_get(&timer.expire_time, boost::TIME_UTC_);
      timer.expire_time.nsec += 200000000LL;
      timer.handler = 0;
      m_timer_heap.push(timer);
      poll_loop_interrupt();
    }

    /** Returns set of I/O handlers scheduled for removal.
     * This is a one shot method adds the handlers that have been added to
     * #m_removed_handlers and then clears the #m_removed_handlers set.
     * @param dst reference to set filled in with removed handlers
     */
    void get_removed_handlers(std::set<IOHandler *> &dst) {
      ScopedLock lock(m_mutex);
      dst = m_removed_handlers;
      m_removed_handlers.clear();
    }

    /** Processes request timeouts and timers.
     * This method removes timed out requests from the request cache, delivering
     * ERROR events (with error == Error::REQUEST_TIMEOUT) via each request's
     * event handler.  It also processes expired timers by removing them from
     * #m_timer_heap and delivering a TIMEOUT event via the timer handler if
     * it exsists.
     * @param next_timeout Set to next earliest timeout of active requests and
     * timers
     */
    void handle_timeouts(PollTimeout &next_timeout);

#if defined(__linux__) || defined (__sun__)
    /// Poll descriptor for <code>epoll</code> or <code>port_associate</code>
    int poll_fd;
#elif defined (__APPLE__) || defined(__FreeBSD__)
    /// Pointer to <code>kqueue</code> object for kqueue polling
    int kqd;
#endif

    /** Add poll interest for socket (POSIX <code>poll</code> only).
     * This method is only called when <code>ReactorFactory::use_poll</code> is
     * set to <i>true</i>.  It modifies #m_polldata accordingly and then calls
     * #poll_loop_interrupt.
     * @param sd Socket descriptor for which to add poll interest
     * @param events Bitmask of poll events (see POSIX <code>poll()</code>)
     * @param handler Pointer to I/O handler associated with socket
     * @return Error code returned by #poll_loop_interrupt
     */
    int add_poll_interest(int sd, short events, IOHandler *handler);

    /** Remove poll interest for socket (POSIX <code>poll</code> only).
     * This method is only called when <code>ReactorFactory::use_poll</code> is
     * set to <i>true</i>.  It modifies #m_polldata accordingly and then calls
     * #poll_loop_interrupt.
     * @param sd Socket descriptor for which to modify poll interest
     * @return Error code returned by #poll_loop_interrupt
     */
    int remove_poll_interest(int sd);

    /** Modify poll interest for socket (POSIX <code>poll</code> only).
     * This method is only called when <code>ReactorFactory::use_poll</code> is
     * set to <i>true</i>.  It modifies #m_polldata accordingly and then calls
     * #poll_loop_interrupt.
     * @param sd Socket descriptor for which to add poll interest
     * @param events Bitmask of poll events (see POSIX <code>poll()</code>)
     * @return Error code returned by #poll_loop_interrupt
     */
    int modify_poll_interest(int sd, short events);

    /** Fetches poll state vectors (POSIX <code>poll</code> only).
     * @param fdarray Vector of <code>pollfd</code> structures to be passed
     * into <code>poll()</code>
     * @param handlers Vector of corresponding I/O handlers
     */
    void fetch_poll_array(std::vector<struct pollfd> &fdarray,
			  std::vector<IOHandler *> &handlers);

    /** Forces polling interface wait call to return.
     * @return Error::OK on success, or Error code on failure
     */
    int poll_loop_interrupt();

    /** Reset state after call to #poll_loop_interrupt.
     * After calling #poll_loop_interrupt and handling the interrupt, this
     * method should be called to reset back to normal polling state.
     * @return Error::OK on success, or Error code on failure
     */
    int poll_loop_continue();

    /** Returns interrupt socket.
     * @return Interrupt socket
     */
    int interrupt_sd() { return m_interrupt_sd; }

  protected:

    /** Priority queue for timers.
     */
    typedef std::priority_queue<ExpireTimer,
      std::vector<ExpireTimer>, LtTimerHeap> TimerHeap;

    Mutex m_mutex;                //!< Mutex to protect members
    Mutex m_polldata_mutex;       //!< Mutex to protect #m_polldata member
    RequestCache m_request_cache; //!< Request cache
    TimerHeap m_timer_heap;       //!< ExpireTimer heap
    int m_interrupt_sd;           //!< Interrupt socket

    /// Set to <i>true</i> if poll loop interrupt in progress
    bool m_interrupt_in_progress;

    /// Vector of poll descriptor state structures for use with POSIX
    /// <code>poll()</code>.
    std::vector<PollDescriptorT> m_polldata;

    /// Next polling interface wait timeout (absolute)
    boost::xtime m_next_wakeup;

    /// Set of IOHandler objects scheduled for removal
    std::set<IOHandler *> m_removed_handlers;
  };

  /// Smart pointer to Reactor
  typedef intrusive_ptr<Reactor> ReactorPtr;
  /** @}*/
} // namespace Hypertable

#endif // HYPERTABLE_REACTOR_H
