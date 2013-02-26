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
 * Declarations for ReactorFactory.
 * This file contains type declarations for ReactorFactory, a static
 * class used to create and manage I/O reactor threads.
 */


#ifndef HYPERTABLE_REACTORFACTORY_H
#define HYPERTABLE_REACTORFACTORY_H

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <cassert>
#include <set>
#include <vector>

#include "Common/atomic.h"
#include "Reactor.h"


namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Static class used to setup and manage I/O reactors.  Since the I/O reactor
   * threads are a process-wide resource, the methods of this class are static.
   */
  class ReactorFactory {

  public:

    /** Initializes I/O reactors.  This method creates and initializes
     * <code>reactor_count</code> reactors, plus an additional dedicated timer
     * reactor.  It also initializes the #use_poll member based on the
     * <code>Comm.UsePoll</code> property and sets the #ms_epollet
     * ("edge triggered") flag to <i>false</i> if running on Linux version older
     * than 2.6.17.  It also allocates a HandlerMap and initializes
     * ReactorRunner::handler_map to point to it.
     * @param reactor_count number of reactor threads to create
     */
    static void initialize(uint16_t reactor_count);

    /** This method shuts down the reactors
     */
    static void destroy();

    /** This method returns the 'next' reactor.  It returns pointers to
     * reactors in round-robin fashion and is used by the Comm subsystem to
     * evenly distribute descriptors across all of the reactors.  The
     * atomic integer variable #ms_next_reactor is used to keep track
     * of the next reactor in the list.
     * @param reactor Smart pointer reference to returned Reactor
     */
    static void get_reactor(ReactorPtr &reactor) {
      assert(ms_reactors.size() > 0);
      reactor = ms_reactors[atomic_inc_return(&ms_next_reactor)
                            % (ms_reactors.size() - 1)];
    }

    /** This method returns the timer reactor.
     * @param reactor Smart pointer reference to returned Reactor
     */
    static void get_timer_reactor(ReactorPtr &reactor) {
      assert(ms_reactors.size() > 0);
      reactor = ms_reactors.back();
    }

    /// Vector of reactors (last position is timer reactor)
    static std::vector<ReactorPtr> ms_reactors;

    /// Boost thread_group for managing reactor threads
    static boost::thread_group ms_threads;

    static boost::mt19937 rng; //!< Pseudo random number generator
    static bool ms_epollet;    //!< Use "edge triggered" epoll
    static bool use_poll;      //!< Use POSIX poll() as polling mechanism

    /// Set to <i>true</i> if this process is acting as "Proxy Master"
    static bool proxy_master;

  private:

    /// Mutex to serialize calls to #initialize
    static Mutex        ms_mutex;

    /// Atomic integer used for round-robin assignment of reactors
    static atomic_t ms_next_reactor;

  };
  /** @}*/
}

#endif // HYPERTABLE_REACTORFACTORY_H

