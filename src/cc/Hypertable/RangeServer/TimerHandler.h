/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

/** @file
 * Declarations for TimerHandler.
 * This file contains type declarations for TimerHandler, a class for
 * managing the maintenance timer.
 */

#ifndef HYPERSPACE_TIMERHANDLER_H
#define HYPERSPACE_TIMERHANDLER_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Time.h"

#include "AsyncComm/DispatchHandler.h"

namespace Hypertable {

  class RangeServer;

  /** @addtogroup RangeServer
   *  @{
   */

  /** Timer interrupt handler.  Currently, the primary purpose of this class
   * is to handle maintenance scheduling.  Usually, it adds a
   * RequestHandlerDoMaintenance object onto the application queue each
   * time it is called.  However, it also tests for low memory and if
   * detected, will cause the maintenance prioritization algorithm to change
   * to aggressively free memory.  It also pauses the application queue
   * to allow maintenance to catch up if it gets behind.
   */
  class TimerHandler : public DispatchHandler {

  public:
    /** Constructor.
     * Initializes the timer handler by setting #m_query_cache_memory to the
     * property <code>Hypertable.RangeServer.QueryCache.MaxMemory</code>,
     * #m_userlog_size_threshold to 20% larger than the maximum log prune
     * threshold, m_max_app_queue_pause to the property
     * <code>Hypertable.RangeServer.Maintenance.MaxAppQueuePause</code>,
     * and #m_timer_interval to the lesser of
     * <code>Hypertable.RangeServer.Timer.Interval</code> or
     * <code>Hypertable.RangeServer.Maintenance.Interval</code>.
     * It also initializes #m_last_schedule to the current time,
     * registers itself with the RangeServer with a call to
     * RangeServer::register_timer, and then schedules a timer interrupt
     * immediately.
     */
    TimerHandler(Comm *comm, RangeServer *range_server);

    /** Timer event handler callback method.
     * If #m_shutdown is <i>true</i>, this method sets #m_shutdown_complete to
     * <i>true</i>, signals #m_shutdown_cond, and returns immediately.
     * Otherwise, if the application queue is paused, it will restart it if 
     * low memory mode was set and the system is no longer low on memory, or
     * the maintenance queue generation has reached #m_restart_generation
     * (meaning all of the maintenance that was last scheduled has been
     * carried out), or #m_max_app_queue_pause milliseconds have elapsed since
     * since the queue was paused.  Also, if application queue is paused,
     * it disables the maintenance schedule operation (unless
     * #m_immediate_maintenance_scheduled is <i>true</i>).  After the paused
     * queue handling, if #m_immediate_maintenance_scheduled is <i>true</i>
     * it will disable low memory mode so that the normal maintenance
     * prioritization algorithm will be used, otherwise a low memory check
     * is performed and #m_low_memory_mode is set accordingly.  If a maintenance
     * schedule is not outstanding (#m_schedule_outstanding == <i>false</i>) and
     * the application queue is not paused, it will schedule maintenance by
     * adding a RequestHandlerDoMaintenance object to the application queue and
     * set #m_schedule_outstanding to <i>true</i> and the timer will not be
     * installed (because it was scheduled as a one-shot).  Otherwise if
     * maintenance was <b>not</b> scheduled, the timer is registered for
     * #m_current_interval milliseconds in the future.  If maintenance was
     * scheduled, the #maintenance_scheduled_notify method will handle
     * re-registering the timer.
     * @param event Event object
     */
    void handle(Hypertable::EventPtr &event);

    /** Force maintenance to be scheduled immediately.
     * If #m_immediate_maintenance_scheduled and #m_schedule_outstanding are set
     * to <i>false</i>, the timer is registered immediately and
     * #m_immediate_maintenance_scheduled is set to <i>true</i>.
     */
    void schedule_immediate_maintenance();

    /** Signal maintenance scheduling complete.
     * This method is called by the RangeServer when it has finished scheduling
     * maintenance.  It sets #m_schedule_outstanding to <i>false</i>,
     * sets #m_last_schedule to the current time, and then if #RangeServer
     * replay has finished, it will do the following:
     *   - If the size of the user commit log is greater than
     *     #m_userlog_size_threshold, or the system is low on memory, then
     *     the application queue will get paused.
     *   - Else-if, the application queue is paused, it will get restarted
     *   - Else, low memory mode is turned off and the timer interval is
     *     set back to normal
     *
     * If #m_immediate_maintenance_scheduled is <i>true</i>, it is set to
     * <i>false</i>, otherwise, the timer is re-registered for
     * #m_current_interval milliseconds in the future.
     */
    void maintenance_scheduled_notify();

    /** Test for low memory mode.
     * @return <i>true</i> if in low memory mode, <i>false</i> otherwise.
     */
    bool low_memory_mode() { return m_low_memory_mode; }

    /** Start shutdown sequence.
     * Sets #m_shutdown to <i>true</i>, cancels current timer, and
     * registers timer immediately.  The #handle method will complete
     * the shutdown sequence.
     */
    void shutdown();

    /** Wait for shutdown to complete.
     * This method waits for #m_shutdown_complete to become <i>true</i>
     * indicating that the timer is no longer active (there are no
     * outstanding timer events).
     */
    void wait_for_shutdown() {
      ScopedLock lock(m_mutex);
      while (!m_shutdown_complete)
        m_shutdown_cond.wait(lock);
    }

  private:

    /// %Mutex for serializing access
    Mutex m_mutex;

    /// Condition variable to signal shutdown complete
    boost::condition m_shutdown_cond;

    /// Comm object
    Comm *m_comm;

    /// RangeServer
    RangeServer *m_range_server;

    /// Application queue
    ApplicationQueuePtr m_app_queue;

    /// Query cache max size (Hypertable.RangeServer.QueryCache.MaxMemory)
    int64_t m_query_cache_memory;
    
    /// Pause app queue if USER log exceeds this size
    int64_t m_userlog_size_threshold;

    /// Generation of maintenance queue signalling application queue restart
    int64_t m_restart_generation;

    /// Last time maintenance was scheduled
    boost::xtime m_last_schedule;

    /// Last time application queue was paused
    boost::xtime m_pause_time;

    /// Timer interval
    int32_t m_timer_interval;

    /// Current timer interval (set to 500 when app queue is paused)
    int32_t m_current_interval;

    /// Maximum time to keep application queue paused each time it is paused
    int32_t m_max_app_queue_pause;

    /// Indicates that a shutdown is in progress
    bool m_shutdown;

    /// Shutdown has completed,
    bool m_shutdown_complete;

    /// An <i>immediate</i> maintenance timer has been scheduled
    bool m_immediate_maintenance_scheduled;

    /// Application queue is paused
    bool m_app_queue_paused;

    /// Low memory mode
    bool m_low_memory_mode;

    /// A maintenance scheduling operation is outstanding
    bool m_schedule_outstanding;

    /** Pauses the application queue.
     * Restarts the application queue, sets #m_low_memory_mode to <i>false</i>,
     * and resets the timer interval back to normal
     */
    void pause_app_queue();

    /** Restarts the application queue.
     * Restarts the application queue, sets #m_low_memory_mode to <i>false</i>,
     * and resets the timer interval back to normal
     */
    void restart_app_queue();

    /** Checks for low memory.
     * @return <i>true</i> if low on memory, <i>false</i> otherwise.
     */
    bool low_memory();
  };

  /// Smart pointer to TimerHandler
  typedef boost::intrusive_ptr<TimerHandler> TimerHandlerPtr;
  /** @}*/
}

#endif // HYPERSPACE_TIMERHANDLER_H

