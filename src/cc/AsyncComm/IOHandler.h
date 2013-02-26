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


#ifndef HYPERTABLE_IOHANDLER_H
#define HYPERTABLE_IOHANDLER_H

extern "C" {
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <poll.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#elif defined(__linux__)
#include <sys/epoll.h>
#if !defined(POLLRDHUP)
#define POLLRDHUP 0x2000
#endif
#elif defined(__sun__)
#include <port.h>
#include <sys/port_impl.h>
#include <unistd.h>
#endif
}

#include "Common/Logger.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/Time.h"

#include "DispatchHandler.h"
#include "ReactorFactory.h"
#include "ExpireTimer.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Base class for socket descriptor I/O handlers.
   * When a socket is created, an I/O handler object is allocated to handle
   * events that occur on the socket.  Events are encapsulated in the Event
   * class and are delivered to the application through a DispatchHandler.  For
   * example, a TCP socket will have an associated IOHandlerData object that
   * reads messages off the socket and sends then to the application via the
   * installed DispatchHandler object.
   */
  class IOHandler : public ReferenceCount {

  public:

    /** Constructor.
     * Initializes the I/O handler, assigns it a Reactor, and sets m_local_addr
     * to the locally bound address (IPv4:port) of <code>sd</code> (see
     * <code>getsockname</code>).
     * @param sd Socket descriptor
     * @param dhp Dispatch handler
     */
    IOHandler(int sd, DispatchHandlerPtr &dhp)
      : m_reference_count(0), m_free_flag(0), m_error(Error::OK),
        m_sd(sd), m_dispatch_handler(dhp), m_decomissioned(false) {
      ReactorFactory::get_reactor(m_reactor);
      m_poll_interest = 0;
      socklen_t namelen = sizeof(m_local_addr);
      getsockname(m_sd, (sockaddr *)&m_local_addr, &namelen);
      memset(&m_alias, 0, sizeof(m_alias));
    }

    /** Event handler method for Unix <i>poll</i> interface.
     * @param event Pointer to pollfd structure describing event
     * @param arrival_time Arrival time of event
     * @return <i>true</i> if socket should be closed, <i>false</i> otherwise
     */
    virtual bool handle_event(struct pollfd *event, time_t arrival_time=0) = 0;

#if defined(__APPLE__) || defined(__FreeBSD__)
    /** Event handler method for <i>kqueue</i> interface (OSX, FreeBSD).
     * @param event Pointer to <code>kevent</code> structure describing event
     * @param arrival_time Arrival time of event
     * @return <i>true</i> if socket should be closed, <i>false</i> otherwise
     */
    virtual bool handle_event(struct kevent *event, time_t arrival_time=0) = 0;
#elif defined(__linux__)
    /** Event handler method for Linux <i>epoll</i> interface.
     * @param event Pointer to <code>epoll_event</code> structure describing event
     * @param arrival_time Arrival time of event
     * @return <i>true</i> if socket should be closed, <i>false</i> otherwise
     */
    virtual bool handle_event(struct epoll_event *event, time_t arrival_time=0) = 0;
#elif defined(__sun__)
    /** Event handler method for <i>port_associate</i> interface (Solaris).
     * @param event Pointer to <code>port_event_t</code> structure describing event
     * @param arrival_time Arrival time of event
     * @return <i>true</i> if socket should be closed, <i>false</i> otherwise
     */
    virtual bool handle_event(port_event_t *event, time_t arrival_time=0) = 0;
#else
    // Implement me!!!
#endif

    /** Destructor.
     */
    virtual ~IOHandler() {
      HT_ASSERT(m_free_flag != 0xdeadbeef);
      m_free_flag = 0xdeadbeef;
      ::close(m_sd);
      return;
    }

    /** Convenience method for delivering event to application.
     * This method will deliver <code>event</code> to the application via the
     * event handler <code>dh</code> if supplied, otherwise the event will be
     * delivered via the default event handler, or no default event handler
     * exists, it will just log the event.  This method is (and should always)
     * by called from a reactor thread.
     * @param event pointer to Event (deleted by this method)
     * @param dh Event handler via which to deliver event
     */
    void deliver_event(Event *event, DispatchHandler *dh=0) {
      memcpy(&event->local_addr, &m_local_addr, sizeof(m_local_addr));
      if (!dh) {
        if (!m_dispatch_handler) {
          HT_INFOF("%s", event->to_str().c_str());
          delete event;
        }
        else {
          EventPtr event_ptr(event);
          m_dispatch_handler->handle(event_ptr);
        }
      }
      else {
        EventPtr event_ptr(event);
        dh->handle(event_ptr);
      }
    }

    /**
     *
     */
    int start_polling(int mode=Reactor::READ_READY);

    int add_poll_interest(int mode);

    int remove_poll_interest(int mode);

    int reset_poll_interest() {
      return add_poll_interest(m_poll_interest);
    }

    InetAddr get_address() { return m_addr; }

    InetAddr get_local_address() { return m_local_addr; }

    void set_proxy(const String &proxy) {
      ScopedLock lock(m_mutex);
      m_proxy = proxy;
    }

    String get_proxy() {
      ScopedLock lock(m_mutex);
      return m_proxy;
    }

    void set_dispatch_handler(DispatchHandler *dh) {
      ScopedLock lock(m_mutex);
      m_dispatch_handler = dh;
    }

    int get_sd() { return m_sd; }

    void get_reactor(ReactorPtr &reactor) { reactor = m_reactor; }

    void display_event(struct pollfd *event);

#if defined(__APPLE__) || defined(__FreeBSD__)
    void display_event(struct kevent *event);
#elif defined(__linux__)
    void display_event(struct epoll_event *event);
#elif defined(__sun__)
    void display_event(port_event_t *event);
#endif

    friend class HandlerMap;

  protected:

    InetAddr get_alias() {
      return m_alias;
    }

    void set_alias(const InetAddr &alias) {
      m_alias = alias;
    }

    void increment_reference_count() {
      m_reference_count++;
    }

    void decrement_reference_count() {
      HT_ASSERT(m_reference_count > 0);
      m_reference_count--;
      if (m_reference_count == 0 && m_decomissioned)
        m_reactor->schedule_removal(this);
    }

    size_t reference_count() {
      return m_reference_count;
    }

    void decomission() {
      if (!m_decomissioned) {
        m_decomissioned = true;
        if (m_reference_count == 0)
          m_reactor->schedule_removal(this);
      }
    }

    bool is_decomissioned() {
      return m_decomissioned;
    }

    virtual void disconnect() { }

    short poll_events(int mode) {
      short events = 0;
      if (mode & Reactor::READ_READY)
	events |= POLLIN;
      if (mode & Reactor::WRITE_READY)
	events |= POLLOUT;
      return events;
    }

    void stop_polling() {
      if (ReactorFactory::use_poll) {
	m_poll_interest = 0;
	m_reactor->modify_poll_interest(m_sd, 0);
	return;
      }
#if defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
      remove_poll_interest(Reactor::READ_READY|Reactor::WRITE_READY);
#elif defined(__linux__)
      struct epoll_event event;  // this is necessary for < Linux 2.6.9
      if (epoll_ctl(m_reactor->poll_fd, EPOLL_CTL_DEL, m_sd, &event) < 0) {
        HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_DEL, %d) failed : %s",
                     m_reactor->poll_fd, m_sd, strerror(errno));
        exit(1);
      }
      m_poll_interest = 0;
#endif
    }

    Mutex               m_mutex;
    size_t              m_reference_count;
    uint32_t            m_free_flag;
    int32_t             m_error;
    String              m_proxy;
    InetAddr            m_addr;
    InetAddr            m_local_addr;
    InetAddr            m_alias;
    int                 m_sd;
    DispatchHandlerPtr  m_dispatch_handler;
    ReactorPtr          m_reactor;
    int                 m_poll_interest;
    bool                m_decomissioned;
  };
  /** @}*/
}


#endif // HYPERTABLE_IOHANDLER_H
