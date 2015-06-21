/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * Declarations for TableMutatorIntervalHandler.
 * This file contains declarations for TableMutatorIntervalHandler, a class that
 * is used as the timer handler for periodically flushing a shared mutator.
 */

#ifndef Hypertable_Lib_TableMutatorIntervalHandler_h
#define Hypertable_Lib_TableMutatorIntervalHandler_h

#include <AsyncComm/DispatchHandler.h>

#include <condition_variable>
#include <mutex>

namespace Hypertable {

  class Comm;
  class TableMutatorShared;

  /** @addtogroup libHypertable
   * @{
   */

  /** %Timer handler for periodically flushing a shared mutator.
   * This class is used as the timer handler for periodically flushing a shared
   * mutator.  The class is initialized with a shared mutator (#shared_mutator)
   * which it flushes on a periodic interval.  It registers itself as the timer handler in
   * a call to Comm::set_timer(), using #shared_mutator->flush_interval()
   * as the duration, and causes the shared mutator to get flushed each time
   * the handle() method is called.
   */
  class TableMutatorIntervalHandler : public DispatchHandler {
  public:

    /** Constructor.
     * Initializes members #comm, #app_queue, and #shared_mutator with the
     * <code>comm</code>, <code>app_queue</code>, and
     * <code>shared_mutator</code> parameters, respectively.  It also picks a
     * random number between 0 and #shared_mutator->flush_interval() and
     * schedules a timer interrupt for that many milliseconds in the future.
     * It does this to smooth out the flush times of potentially hundreds
     * or thousands of clients that start up at the same time.
     * @param comm Pointer to AsyncComm subsystem
     * @param app_queue Application queue
     * @param shared_mutator Shared mutator
     */
    TableMutatorIntervalHandler(Comm *comm,ApplicationQueueInterface *app_queue,
				TableMutatorShared *shared_mutator);

    /// Starts interval timer
    void start();

    /** Handles the timer interrupt.
     * If #active is <i>true</i> then a TableMutatorFlushHandler object will
     * get created and enqueued on #app_queue and then timer will be
     * re-registered with a call to Comm::set_timer() using <i>this</i> object
     * as the handler and #shared_mutator->flush_interval() for the duration.
     * Otherwise, if #active is <i>false</i>, #complete will get set to
     * <i>true</i> and #cond will be notified.
     * @param event Event object generated from AsyncComm (usually TIMER event)
     */
    virtual void handle(EventPtr &event);

    /** Flushes shared mutator #shared_mutator.
     * If #active is <i>true</i>, this method flushes #shared_mutator,
     * otherwise it does nothing.  This method is typically called from
     * a TableMutatorFlushHandler object that gets enqueued on #app_queue from
     * the handle() method.
     */
    void flush();

    /** Stops the periodic flushing of the shared mutator.
     * This method sets #active to <i>false</i> and then waits on #cond
     * until #complete becomes <i>true</i>.
     */
    void stop() {
      std::unique_lock<std::mutex> lock(mutex);
      active = false;
      cond.wait(lock, [this](){ return complete; });
    }

  private:

    /// %Mutex for serializing access to members
    std::mutex mutex;

    /// Condition variable signalled when #complete is set to <i>true</i>
    std::condition_variable cond;

    /// Set to <i>false</i> to deactivate and prevent further timer interrupts
    bool active;

    /// Indicates if final timer interrupt has completed
    bool complete;

    /// Pointer to AsyncComm subsystem
    Comm *comm;

    /// Pointer to application queue
    ApplicationQueueInterface *app_queue;

    /// Shared mutator to be periodically flushed
    TableMutatorShared *shared_mutator;
  };

  /// Smart pointer to TableMutatorIntervalHandler
  typedef std::shared_ptr<TableMutatorIntervalHandler>
  TableMutatorIntervalHandlerPtr;

  /** @}*/

}

#endif /// Hypertable_Lib_TableMutatorIntervalHandler_h
