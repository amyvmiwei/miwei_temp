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
 * Definitions for IOHandlerAccept.
 * This file contains method definitions for IOHandlerAccept, a class for
 * processing I/O events for accept (listen) sockets.
 */

#include <Common/Compat.h>

#define HT_DISABLE_LOG_DEBUG 1

#include "HandlerMap.h"
#include "IOHandlerAccept.h"
#include "IOHandlerData.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"

#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>

#include <iostream>

extern "C" {
#include <errno.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
}


using namespace Hypertable;
using namespace std;


bool
IOHandlerAccept::handle_event(struct pollfd *event,
                              ClockT::time_point) {
  if (event->revents & POLLIN)
    return handle_incoming_connection();
  ReactorRunner::handler_map->decomission_handler(this);
  return true;
}

/**
 *
 */
#if defined(__APPLE__) || defined(__FreeBSD__)
bool IOHandlerAccept::handle_event(struct kevent *event,
                                   ClockT::time_point) {
  //DisplayEvent(event);
  if (event->filter == EVFILT_READ)
    return handle_incoming_connection();
  ReactorRunner::handler_map->decomission_handler(this);
  return true;
}
#elif defined(__linux__)
bool IOHandlerAccept::handle_event(struct epoll_event *event,
                                   ClockT::time_point) {
  //DisplayEvent(event);
  return handle_incoming_connection();
}
#elif defined(__sun__)
bool IOHandlerAccept::handle_event(port_event_t *event,
                                   ClockT::time_point) {
  if (event->portev_events == POLLIN)
    return handle_incoming_connection();
  ReactorRunner::handler_map->decomission_handler(this);
  return true;
}
#else
  ImplementMe;
#endif



bool IOHandlerAccept::handle_incoming_connection() {
  int sd;
  struct sockaddr_in addr;
  socklen_t addr_len = sizeof(sockaddr_in);
  int one = 1;
  IOHandlerData *handler;

  while (true) {

    if ((sd = accept(m_sd, (struct sockaddr *)&addr, &addr_len)) < 0) {
      if (errno == EAGAIN)
        break;
      HT_ERRORF("accept() failure: %s", strerror(errno));
      break;
    }

    HT_DEBUGF("Just accepted incoming connection, fd=%d (%s:%d)",
	      m_sd, inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

    // Set to non-blocking
    FileUtils::set_flags(sd, O_NONBLOCK);

#if defined(__linux__)
    if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
      HT_WARNF("setsockopt(TCP_NODELAY) failure: %s", strerror(errno));
#elif defined(__sun__)
    if (setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(one)) < 0)
      HT_ERRORF("setting TCP_NODELAY: %s", strerror(errno));
#elif defined(__APPLE__) || defined(__FreeBSD__)
    if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
      HT_WARNF("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

    if (setsockopt(m_sd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0)
      HT_ERRORF("setsockopt(SO_KEEPALIVE) failure: %s", strerror(errno));

    int bufsize = 4*32768;

    if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0)
      HT_WARNF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));

    if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0)
      HT_WARNF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));

    DispatchHandlerPtr dhp;
    m_handler_factory->get_instance(dhp);

    handler = new IOHandlerData(sd, addr, dhp, true);

    m_handler_map->insert_handler(handler, true);

    int32_t error;
    if ((error = handler->start_polling(PollEvent::READ |
                                        PollEvent::WRITE)) != Error::OK) {
      HT_ERRORF("Problem starting polling on incoming connection - %s",
                Error::get_text(error));
      ReactorRunner::handler_map->decrement_reference_count(handler);
      ReactorRunner::handler_map->decomission_handler(handler);
      return false;
    }

    if (ReactorFactory::proxy_master) {
      if ((error = ReactorRunner::handler_map->propagate_proxy_map(handler))
          != Error::OK) {
        HT_ERRORF("Problem sending proxy map to %s - %s",
                  m_addr.format().c_str(), Error::get_text(error));
        ReactorRunner::handler_map->decrement_reference_count(handler);
        return false;
      }
    }

    ReactorRunner::handler_map->decrement_reference_count(handler);

    EventPtr event = make_shared<Event>(Event::CONNECTION_ESTABLISHED, addr, Error::OK);
    deliver_event(event);
  }

  return false;
 }
