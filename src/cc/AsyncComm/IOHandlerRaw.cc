/*
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Definitions for IOHandlerRaw.
 * This file contains method definitions for IOHandlerRaw, a class for
 * processing I/O events for data (TCP) sockets.
 */

#include <Common/Compat.h>

#include "IOHandlerRaw.h"
#include "PollEvent.h"
#include "ReactorRunner.h"

#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/InetAddr.h>
#include <Common/Time.h>

#include <cassert>
#include <iostream>

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#endif
#include <sys/uio.h>
}

using namespace Hypertable;
using namespace std;


bool
IOHandlerRaw::handle_event(struct pollfd *event,
                           ClockT::time_point arrival_time) {

  //DisplayEvent(event);

  try {

    if (event->revents & POLLERR) {
      HT_INFOF("Received POLLERR on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    if (event->revents & POLLHUP) {
      HT_DEBUGF("Received POLLHUP on descriptor %d (%s:%d)", m_sd,
		inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    int events {};

    if (event->revents & POLLOUT)
      events |= PollEvent::WRITE;

    if (event->revents & POLLIN)
      events |= PollEvent::READ;

    if (!m_handler->handle(m_sd, events))
      return true;

    update_poll_interest();

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    ReactorRunner::handler_map->decomission_handler(this);
    return true;
  }

  return m_error != Error::OK;
}

#if defined(__linux__)

bool
IOHandlerRaw::handle_event(struct epoll_event *event,
                           ClockT::time_point arrival_time) {

  //display_event(event);

  try {

    if (ReactorFactory::ms_epollet && event->events & POLLRDHUP) {
      HT_DEBUGF("Received POLLRDHUP on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    if (event->events & EPOLLERR) {
      HT_INFOF("Received EPOLLERR on descriptor %d (%s:%d)", m_sd,
               inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    if (event->events & EPOLLHUP) {
      HT_DEBUGF("Received EPOLLHUP on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    int events {};

    if (event->events & EPOLLOUT)
      events |= PollEvent::WRITE;

    if (event->events & EPOLLIN)
      events |= PollEvent::READ;

    if (!m_handler->handle(m_sd, events))
      return true;

    update_poll_interest();

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    ReactorRunner::handler_map->decomission_handler(this);
    return true;
  }

  return m_error != Error::OK;
}

#elif defined(__sun__)

bool IOHandlerRaw::handle_event(port_event_t *event,
                                ClockT::time_point arrival_time) {

  //display_event(event);

  try {

    if (event->portev_events & POLLERR) {
      HT_INFOF("Received POLLERR on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    if (event->portev_events & POLLHUP) {
      HT_DEBUGF("Received POLLHUP on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    if (event->portev_events & POLLREMOVE) {
      HT_DEBUGF("Received POLLREMOVE on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    int events {};

    if (event->portev_events & POLLOUT)
      events |= PollEvent::WRITE;

    if (event->portev_events & POLLIN)
      events |= PollEvent::READ;

    if (!m_handler->handle(m_sd, events))
      return true;

    update_poll_interest();
    
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    test_and_set_error(e.code());
    ReactorRunner::handler_map->decomission_handler(this);
    return true;
  }

  return m_error != Error::OK;
}

#elif defined(__APPLE__) || defined(__FreeBSD__)

bool IOHandlerRaw::handle_event(struct kevent *event,
                                ClockT::time_point arrival_time) {

  //DisplayEvent(event);

  try {
    assert(m_sd == (int)event->ident);

    if (event->flags & EV_EOF) {
      ReactorRunner::handler_map->decomission_handler(this);
      return true;
    }

    int events {};

    if (event->filter == EVFILT_WRITE)
      events |= PollEvent::WRITE;

    if (event->filter == EVFILT_READ) 
      events |= PollEvent::READ;

    if (!m_handler->handle(m_sd, events))
      return true;

    update_poll_interest();

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    test_and_set_error(e.code());
    ReactorRunner::handler_map->decomission_handler(this);
    return true;
  }

  return m_error != Error::OK;
}
#else
  ImplementMe;
#endif

void IOHandlerRaw::update_poll_interest() {
  int error;
  int new_interest = m_handler->poll_interest(m_sd);
  int changed = new_interest ^ m_poll_interest;
  int turn_off = changed ^ new_interest;
  int turn_on  = changed ^ m_poll_interest;
  int mask = ~(turn_off & turn_on);
  turn_off &= mask;
  turn_on &= mask;
  if (turn_off) {
    HT_DEBUGF("Turning poll interest OFF:  0x%x", turn_off);
    if ((error = remove_poll_interest(turn_off)) != Error::OK) {
      if (m_error == Error::OK)
        m_error = error;
      return;
    }
  }
  if (turn_on) {
    HT_DEBUGF("Turning poll interest ON:  0x%x", turn_on);
    if ((error = add_poll_interest(turn_on)) != Error::OK) {
      if (m_error == Error::OK)
        m_error = error;
      return;
    }
  }
}
