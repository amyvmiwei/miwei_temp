/* -*- C++ -*-
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

#ifndef HYPERTABLE_IOHANDLERDATAGRAM_H
#define HYPERTABLE_IOHANDLERDATAGRAM_H

#include <list>
#include <utility>

extern "C" {
#include <netdb.h>
#include <string.h>
#include <time.h>
}

#include "CommBuf.h"
#include "IOHandler.h"


namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** I/O handler for UDP sockets.
   */
  class IOHandlerDatagram : public IOHandler {

  public:

    IOHandlerDatagram(int sd, DispatchHandlerPtr &dhp) : IOHandler(sd, dhp) {
      m_message = new uint8_t [65536];
      memcpy(&m_addr, &m_local_addr, sizeof(InetAddr));
    }

    virtual ~IOHandlerDatagram() { delete [] m_message; }

    int send_message(const InetAddr &addr, CommBufPtr &cbp);

    int flush_send_queue();

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

    int handle_write_readiness();

  private:

    typedef std::pair<struct sockaddr_in, CommBufPtr> SendRec;

    Mutex           m_mutex;
    uint8_t        *m_message;
    std::list<SendRec>  m_send_queue;
  };
  /** @}*/
}

#endif // HYPERTABLE_IOHANDLERDATAGRAM_H
