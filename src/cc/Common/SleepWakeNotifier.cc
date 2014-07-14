/*
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

/** @file
 * Error codes, Exception handling, error logging.
 *
 * This file contains all error codes used in Hypertable, the Exception
 * base class and macros for logging and error handling.
 */

#include <Common/Compat.h>

#include "SleepWakeNotifier.h"

#include <Common/Logger.h>

#if defined(__APPLE__)
#include <mach/mach_port.h>
#include <mach/mach_interface.h>
#include <mach/mach_init.h>
#include <IOKit/pwr_mgt/IOPMLib.h>
#include <IOKit/IOMessage.h>
#endif


using namespace Hypertable;

SleepWakeNotifier::SleepWakeNotifier(std::function<void()> sleep_callback,
                                   std::function<void()> wakeup_callback)
  : m_thread(&SleepWakeNotifier::thread_func, this),
    m_callback_sleep(sleep_callback), m_callback_wakeup(wakeup_callback) {
}

void SleepWakeNotifier::handle_sleep() {
  if (m_callback_sleep)
    m_callback_sleep();
}

void SleepWakeNotifier::handle_wakeup() {
  if (m_callback_wakeup)
    m_callback_wakeup();
}


#if defined(__APPLE__)

void system_sleep_callback(void * refCon, io_service_t service,
                           natural_t messageType, void * messageArgument) {
  SleepWakeNotifier *sleep_wake_notifier = (SleepWakeNotifier *)refCon;

  switch ( messageType ) {
 
  case kIOMessageCanSystemSleep:
    /* Idle sleep is about to kick in. This message will not be sent for forced sleep.
       Applications have a chance to prevent sleep by calling IOCancelPowerChange.
       Most applications should not prevent idle sleep.
 
       Power Management waits up to 30 seconds for you to either allow or deny idle
       sleep. If you don't acknowledge this power change by calling either
       IOAllowPowerChange or IOCancelPowerChange, the system will wait 30
       seconds then go to sleep.
    */
 
    //Uncomment to cancel idle sleep
    //IOCancelPowerChange( root_port, (long)messageArgument );
    // we will allow idle sleep
    //sleep_wake_notifier->handle_sleep();
    IOAllowPowerChange( sleep_wake_notifier->get_root_port(), (long)messageArgument );
    break;
 
  case kIOMessageSystemWillSleep:
    /* The system WILL go to sleep. If you do not call IOAllowPowerChange or
       IOCancelPowerChange to acknowledge this message, sleep will be
       delayed by 30 seconds.
 
       NOTE: If you call IOCancelPowerChange to deny sleep it returns
       kIOReturnSuccess, however the system WILL still go to sleep.
    */
    sleep_wake_notifier->handle_sleep(); 
    IOAllowPowerChange( sleep_wake_notifier->get_root_port(), (long)messageArgument );
    break;
 
  case kIOMessageSystemWillPowerOn:
    //System has started the wake up process...
    sleep_wake_notifier->handle_wakeup();
    break;
 
  case kIOMessageSystemHasPoweredOn:
    //System has finished waking up...
    break;
 
  default:
    break;
  }
}

#endif
 

void SleepWakeNotifier::thread_func() {

#if defined(__APPLE__)

  // register to receive system sleep notifications
  m_root_port = IORegisterForSystemPower(this, &m_notify_port_ref,
                                         system_sleep_callback,
                                         &m_notifier_object);
  if ( m_root_port == 0 )  {
    HT_INFO("IORegisterForSystemPower failed");
    return;
  }
 
  // add the notification port to the application runloop
  CFRunLoopAddSource( CFRunLoopGetCurrent(),
                      IONotificationPortGetRunLoopSource(m_notify_port_ref), kCFRunLoopCommonModes );

  // get current thread's run loop
  m_run_loop = CFRunLoopGetCurrent();
 
  // start the run loop to receive sleep notifications.
  CFRunLoopRun();

#else

#endif

}


SleepWakeNotifier::~SleepWakeNotifier() {

#if defined(__APPLE__)
  // remove the sleep notification port from the application runloop
  CFRunLoopRemoveSource( CFRunLoopGetCurrent(),
                         IONotificationPortGetRunLoopSource(m_notify_port_ref),
                         kCFRunLoopCommonModes );
 
  // deregister for system sleep notifications
  IODeregisterForSystemPower( &m_notifier_object );
 
  // IORegisterForSystemPower implicitly opens the Root Power Domain IOService
  // so we close it here
  IOServiceClose( m_root_port );
 
  // destroy the notification port allocated by IORegisterForSystemPower
  IONotificationPortDestroy( m_notify_port_ref );

  // stop run loop
  CFRunLoopStop(m_run_loop);
#endif

  m_thread.join();
}
