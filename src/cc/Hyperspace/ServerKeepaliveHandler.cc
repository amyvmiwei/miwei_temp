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

#include <Common/Compat.h>

#include "request/RequestHandlerRenewSession.h"
#include "request/RequestHandlerExpireSessions.h"
#include "ServerKeepaliveHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "SessionData.h"

#include <Common/Error.h>
#include <Common/InetAddr.h>
#include <Common/StringExt.h>
#include <Common/System.h>

using namespace Hypertable;
using namespace Hyperspace;
using namespace Serialization;
using namespace std;

ServerKeepaliveHandler::ServerKeepaliveHandler(Comm *comm, Master *master,
                                               ApplicationQueuePtr &app_queue)
  : m_comm(comm), m_master(master),
    m_app_queue_ptr(app_queue), m_shutdown(false) {

  m_master->get_datagram_send_address(&m_send_addr);

}

void ServerKeepaliveHandler::start() {
  int error;

  if ((error = m_comm->set_timer(Master::TIMER_INTERVAL_MS, shared_from_this()))
      != Error::OK) {
    HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
    exit(EXIT_FAILURE);
  }

  m_master->tick();
}


void ServerKeepaliveHandler::handle(Hypertable::EventPtr &event) {
  int error;

  {
    lock_guard<mutex> lock(m_mutex);
    if (m_shutdown)
      return;
  }

  if (event->type == Hypertable::Event::MESSAGE) {
    const uint8_t *decode_ptr = event->payload;
    size_t decode_remain = event->payload_len;

    try {

      // sanity check command code
      if (event->header.command >= Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case Protocol::COMMAND_KEEPALIVE: {
          uint64_t session_id = decode_i64(&decode_ptr, &decode_remain);
          uint32_t delivered_event_count;
          std::set<uint64_t> delivered_events;
          delivered_event_count = decode_i32(&decode_ptr, &decode_remain);
          for (uint32_t i=0; i<delivered_event_count; i++)
            delivered_events.insert( decode_i64(&decode_ptr, &decode_remain) );
          bool shutdown = decode_bool(&decode_ptr, &decode_remain);

          m_app_queue_ptr->add( new RequestHandlerRenewSession(m_comm,
              m_master, session_id, delivered_events, shutdown, event,
              &m_send_addr ) );
        }
        break;
      default:
        HT_THROWF(Error::PROTOCOL_ERROR, "Unimplemented command (%llu)",
                  (Llu)event->header.command);
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
  }
  else if (event->type == Hypertable::Event::TIMER) {

    m_master->tick();

    try {
      m_app_queue_ptr->add( new RequestHandlerExpireSessions(m_master) );
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }

    if ((error = m_comm->set_timer(Master::TIMER_INTERVAL_MS, shared_from_this()))
        != Error::OK) {
      HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
      exit(EXIT_FAILURE);
    }

  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }
}


void ServerKeepaliveHandler::deliver_event_notifications(uint64_t session_id) {
  int error = 0;
  SessionDataPtr session_ptr;

  {
    lock_guard<mutex> lock(m_mutex);
    if (m_shutdown)
      return;
  }

  //HT_INFOF("Delivering event notifications for session %lld", session_id);

  if (!m_master->get_session(session_id, session_ptr)) {
    HT_ERRORF("Unable to find data for session %llu", (Llu)session_id);
    return;
  }

  /*
  HT_INFOF("Sending Keepalive request to %s",
           InetAddr::format(session_ptr->addr));
  **/

  CommBufPtr cbp(Protocol::create_server_keepalive_request(session_ptr));

  if ((error = m_comm->send_datagram(session_ptr->get_addr(), m_send_addr, cbp))
      != Error::OK) {
    HT_ERRORF("Comm::send_datagram returned %s", Error::get_text(error));
  }
}

void ServerKeepaliveHandler::shutdown() {
  lock_guard<mutex> lock(m_mutex);
  m_shutdown = true;
  m_app_queue_ptr->shutdown();
}
