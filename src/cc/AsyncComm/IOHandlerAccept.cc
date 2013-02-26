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

#include "Common/Compat.h"

#include <iostream>

extern "C" {
#include <errno.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#define HT_DISABLE_LOG_DEBUG 1

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"

#include "HandlerMap.h"
#include "IOHandlerAccept.h"
#include "IOHandlerData.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"

using namespace Hypertable;
using namespace std;


bool
IOHandlerAccept::handle_event(struct pollfd *event, time_t arival_time) {
  if (event->revents & POLLIN)
    return handle_incoming_connection();
  ReactorRunner::handler_map->decomission_handler(this);
  return true;
}

/**
 *
 */
#if defined(__APPLE__) || defined(__FreeBSD__)
bool IOHandlerAccept::handle_event(struct kevent *event, time_t) {
  //DisplayEvent(event);
  if (event->filter == EVFILT_READ)
    return handle_incoming_connection();
  ReactorRunner::handler_map->decomission_handler(this);
  return true;
}
#elif defined(__linux__)
bool IOHandlerAccept::handle_event(struct epoll_event *event, time_t) {
  //DisplayEvent(event);
  return handle_incoming_connection();
}
#elif defined(__sun__)
bool IOHandlerAccept::handle_event(port_event_t *event, time_t) {
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

    int bufsize = 4*32768;

    if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0)
      HT_WARNF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));

    if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0)
      HT_WARNF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));

    DispatchHandlerPtr dhp;
    m_handler_factory->get_instance(dhp);

    handler = new IOHandlerData(sd, addr, dhp, true);

    int32_t error = m_handler_map->insert_handler(handler);
    if (error != Error::OK) {
      HT_ERRORF("Problem registering accepted connection in handler map - %s",
                Error::get_text(error));
      delete handler;
      return true;
    }
    handler->start_polling(Reactor::READ_READY|Reactor::WRITE_READY);

    deliver_event(new Event(Event::CONNECTION_ESTABLISHED, addr, Error::OK));
  }

  return false;
 }
