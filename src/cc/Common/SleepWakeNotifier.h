/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declaration for SleepWakeNotifier.
/// This file contains the declaration of SleepWakeNotifier, a class for
/// delivering sleep and wakup notifications.


#ifndef Hypertable_Common_SleepWakeNotifier_h
#define Hypertable_Common_SleepWakeNotifier_h

#include "Common/String.h"

#include <functional>
#include <thread>

#if defined(__APPLE__)
#include <IOKit/pwr_mgt/IOPMLib.h>
#endif


namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Delivers sleep and wakeup notifications
  class SleepWakeNotifier {

  public:
    
    /// Constructor.
    /// Creates a polling thread (#m_thread) using thread_func() as the thread
    /// function.  Registers <code>sleep_callback</code> and
    /// <code>wakeup_callback</code> as the sleep and wakeup callbacks,
    /// respectively.
    /// @param sleep_callback Sleep callback
    /// @param wakeup_callback Wakeup callback
    SleepWakeNotifier(std::function<void()> sleep_callback,
                      std::function<void()> wakeup_callback);

    /// Destructor
    /// Does OS-specific deregistering of sleep/wakeup notification interest and
    /// then joins #m_thread.
    ~SleepWakeNotifier();

    /// Called by polling thread to deliver <i>sleep</i> notification
    /// Calls registered sleep callback #m_callback_sleep if it is not null
    void handle_sleep();

    /// Called by polling thread to deliver <i>wakeup</i> notification
    /// Calls registered wakeup callback #m_callback_wakeup if it is not null
    void handle_wakeup();

#if defined(__APPLE__)
    /// Returns OSX root port
    io_connect_t get_root_port() { return m_root_port; }
#endif

  private:

    /// Thread function for event notification polling thread (#m_thread)
    void thread_func();

    /// Event notification polling thread
    std::thread m_thread {};

    /// Registered sleep callback
    std::function<void()> m_callback_sleep {};

    /// Registered wakeup callback
    std::function<void()> m_callback_wakeup {};

#if defined(__APPLE__)
    /// OSX reference to the Root Power Domain IOService
    io_connect_t  m_root_port; 

    /// OSX notification port allocated by IORegisterForSystemPower
    IONotificationPortRef m_notify_port_ref;
 
    /// OSX notifier object, used to deregister later
    io_object_t m_notifier_object;

    /// OSX thread run loop
    CFRunLoopRef m_run_loop;
#endif

  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Common_SleepWakeNotifier_h
