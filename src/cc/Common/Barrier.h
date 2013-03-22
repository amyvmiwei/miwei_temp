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
 * A Barrier to block execution of code.
 * This barrier works similar to a critical section, but allows threads to
 * execute the code if the barrier is down, and blocks threads while the
 * barrier is up
 */

#ifndef HYPERTABLE_BARRIER_H
#define HYPERTABLE_BARRIER_H

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "Mutex.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * A Barrier to block execution of code.
 */
class Barrier {
  public:
    Barrier() : m_hold(false), m_counter(0) {
    }

    /** Enters the critical section. If the barrier is "up" then this
     * thread will wait till the barrier is "down" again, then it can
     * continue execution.
     */
    void enter() {
      ScopedLock lock(m_mutex);
      while (m_hold)
        m_unblocked_cond.wait(lock);
      m_counter++;
    }

    /** Leaves the critical section; will wake up/notify waiting threads if
     * necessary.
     */
    void exit() {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_counter > 0);
      m_counter--;
      if (m_hold && m_counter == 0)
        m_quiesced_cond.notify_one();
    }

    /** "Puts" the barrier "up". If any threads are currently executing the
     * blocked code then this thread will wait till they are done. Otherwise
     * only the current thread can execute the code.
     */
    void put_up() {
      ScopedLock lock(m_mutex);
      while (m_hold)
        m_unblocked_cond.wait(lock);
      m_hold = true;
      while (m_counter > 0)
        m_quiesced_cond.wait(lock);
    }

    /** "Takes" the barrier "down"; all threads waiting in enter() are allowed
     * to continue.
     */
    void take_down() {
      ScopedLock lock(m_mutex);
      m_hold = false;
      m_unblocked_cond.notify_all();
    }

    /**
     * A helper class to put up a barrier when entering a scope and
     * take it down when leaving the scope
     */
    class ScopedActivator {
      public:
        /** Constructor; "puts up" the barrier
         *
         * @param barrier Reference to the Barrier
         */
        ScopedActivator(Barrier &barrier) : m_barrier(barrier) {
          m_barrier.put_up();
        }

        /** Destructor; "takes down" the barrier */
        ~ScopedActivator() {
          m_barrier.take_down();
        }

      private:
        /** A reference to the Barrier */
        Barrier &m_barrier;
    };

  private:
    /** Mutex to lock access to the members and conditions */
    Mutex            m_mutex;

    /** Condition to wait for when barrier is up */
    boost::condition m_unblocked_cond;

    /** Condition to wait for to take the barrier down */
    boost::condition m_quiesced_cond;

    /** True if the barrier is up */
    bool             m_hold;

    /** Number of threads that have enter()ed (but not exit()ed) the code */ 
    uint32_t         m_counter;
};

/** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_BARRIER_H
