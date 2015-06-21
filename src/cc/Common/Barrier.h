/*
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

/** @file
 * A Barrier to block execution of code.
 * This barrier works similar to a critical section, but allows threads to
 * execute the code if the barrier is down, and blocks threads while the
 * barrier is up
 */

#ifndef Common_Barrier_h
#define Common_Barrier_h

#include <condition_variable>
#include <mutex>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * A Barrier to block execution of code.
 */
class Barrier {
  public:
    Barrier() { }

    /** Enters the critical section. If the barrier is "up" then this
     * thread will wait till the barrier is "down" again, then it can
     * continue execution.
     */
    void enter() {
      std::unique_lock<std::mutex> lock(m_mutex);
      m_unblocked_cond.wait(lock, [this](){ return !m_hold; });
      m_counter++;
    }

    /** Leaves the critical section; will wake up/notify waiting threads if
     * necessary.
     */
    void exit() {
      std::lock_guard<std::mutex> lock(m_mutex);
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
      std::unique_lock<std::mutex> lock(m_mutex);
      m_unblocked_cond.wait(lock, [this](){ return !m_hold; });
      m_hold = true;
      m_quiesced_cond.wait(lock, [this](){ return m_counter == 0; });
    }

    /** "Takes" the barrier "down"; all threads waiting in enter() are allowed
     * to continue.
     */
    void take_down() {
      std::lock_guard<std::mutex> lock(m_mutex);
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
    std::mutex m_mutex;

    /** Condition to wait for when barrier is up */
    std::condition_variable m_unblocked_cond;

    /** Condition to wait for to take the barrier down */
    std::condition_variable m_quiesced_cond;

    /** True if the barrier is up */
    bool m_hold {};

    /** Number of threads that have enter()ed (but not exit()ed) the code */ 
    uint32_t m_counter {};
};

/** @}*/

}

#endif // Common_Barrier_h
