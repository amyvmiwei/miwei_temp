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


#ifndef HYPERTABLE_IOHANDLERACCEPT_H
#define HYPERTABLE_IOHANDLERACCEPT_H

#include "HandlerMap.h"
#include "IOHandler.h"
#include "ConnectionHandlerFactory.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** I/O handler for listen sockets.
   */
  class IOHandlerAccept : public IOHandler {

  public:

    IOHandlerAccept(int sd, DispatchHandlerPtr &dhp,
                    HandlerMapPtr &hmap, ConnectionHandlerFactoryPtr &chfp)
      : IOHandler(sd, dhp), m_handler_map(hmap), m_handler_factory(chfp) {
      memcpy(&m_addr, &m_local_addr, sizeof(InetAddr));
    }

    virtual ~IOHandlerAccept() { }

    // define default poll() interface for everyone since it is chosen at runtime
    virtual bool handle_event(struct pollfd *event, time_t arival_time=0);

#if defined(__APPLE__) || defined(__FreeBSD__)
    virtual bool handle_event(struct kevent *event, time_t arival_time=0);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event, time_t arival_time=0);
#elif defined(__sun__)
    virtual bool handle_event(port_event_t *event, time_t arival_time=0);
#else
    ImplementMe;
#endif

  private:

    bool handle_incoming_connection();

    HandlerMapPtr m_handler_map;
    ConnectionHandlerFactoryPtr m_handler_factory;
  };
  /** @}*/
}

#endif // HYPERTABLE_IOHANDLERACCEPT_H

