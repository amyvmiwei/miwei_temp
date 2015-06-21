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

#ifndef Hyperspace_request_RequestHandlerRenewSession_h
#define Hyperspace_request_RequestHandlerRenewSession_h

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"

#include <set>

namespace Hyperspace {
  using namespace Hypertable;
  class Master;

  class RequestHandlerRenewSession : public ApplicationHandler {
  public:
    RequestHandlerRenewSession(Comm *comm, Master *master,
           uint64_t session_id, std::set<uint64_t> &delivered_events,
           bool destroy_session, EventPtr &event, struct sockaddr_in *send_addr)
      : m_comm(comm), m_master(master), m_session_id(session_id),
        m_delivered_events(delivered_events),m_destroy_session(destroy_session),
        m_event(event), m_send_addr(send_addr)  { }
    virtual ~RequestHandlerRenewSession() { }

    virtual void run();

  private:
    Comm        *m_comm;
    Master      *m_master;
    uint64_t     m_session_id;
    std::set<uint64_t> m_delivered_events;
    bool         m_destroy_session;
    EventPtr     m_event;
    struct sockaddr_in *m_send_addr;
  };
}

#endif // Hyperspace_request_RequestHandlerRenewSession_h
