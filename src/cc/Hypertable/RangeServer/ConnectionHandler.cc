/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#include "ConnectionHandler.h"

#include <Hypertable/RangeServer/RangeServer.h>
#include <Hypertable/RangeServer/Request/Handler/AcknowledgeLoad.h>
#include <Hypertable/RangeServer/Request/Handler/CommitLogSync.h>
#include <Hypertable/RangeServer/Request/Handler/Compact.h>
#include <Hypertable/RangeServer/Request/Handler/CreateScanner.h>
#include <Hypertable/RangeServer/Request/Handler/DestroyScanner.h>
#include <Hypertable/RangeServer/Request/Handler/DropRange.h>
#include <Hypertable/RangeServer/Request/Handler/DropTable.h>
#include <Hypertable/RangeServer/Request/Handler/Dump.h>
#include <Hypertable/RangeServer/Request/Handler/DumpPseudoTable.h>
#include <Hypertable/RangeServer/Request/Handler/FetchScanblock.h>
#include <Hypertable/RangeServer/Request/Handler/GetStatistics.h>
#include <Hypertable/RangeServer/Request/Handler/Heapcheck.h>
#include <Hypertable/RangeServer/Request/Handler/LoadRange.h>
#include <Hypertable/RangeServer/Request/Handler/MetadataSync.h>
#include <Hypertable/RangeServer/Request/Handler/PhantomCommitRanges.h>
#include <Hypertable/RangeServer/Request/Handler/PhantomLoad.h>
#include <Hypertable/RangeServer/Request/Handler/PhantomPrepareRanges.h>
#include <Hypertable/RangeServer/Request/Handler/PhantomUpdate.h>
#include <Hypertable/RangeServer/Request/Handler/RelinquishRange.h>
#include <Hypertable/RangeServer/Request/Handler/ReplayFragments.h>
#include <Hypertable/RangeServer/Request/Handler/SetState.h>
#include <Hypertable/RangeServer/Request/Handler/Shutdown.h>
#include <Hypertable/RangeServer/Request/Handler/Status.h>
#include <Hypertable/RangeServer/Request/Handler/TableMaintenanceDisable.h>
#include <Hypertable/RangeServer/Request/Handler/TableMaintenanceEnable.h>
#include <Hypertable/RangeServer/Request/Handler/Update.h>
#include <Hypertable/RangeServer/Request/Handler/UpdateSchema.h>
#include <Hypertable/RangeServer/Request/Handler/WaitForMaintenance.h>

#include <Hypertable/Lib/RangeServer/Protocol.h>

#include <AsyncComm/ApplicationQueue.h>
#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/StringExt.h>

#include <iostream>

extern "C" {
#include <poll.h>
}

using namespace Hypertable;
using namespace Hypertable::RangeServer;

void ConnectionHandler::handle(EventPtr &event) {

  if (m_shutdown &&
      event->header.command != Lib::RangeServer::Protocol::COMMAND_SHUTDOWN &&
      event->header.command != Lib::RangeServer::Protocol::COMMAND_STATUS) {
    ResponseCallback cb(m_comm, event);
    cb.error(Error::RANGESERVER_SHUTTING_DOWN, "");
    return;
  }

  if (event->type == Event::MESSAGE) {
    ApplicationHandler *handler = 0;

    //event->display();

    try {

      // sanity check command code
      if (event->header.command >= Lib::RangeServer::Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case Lib::RangeServer::Protocol::COMMAND_ACKNOWLEDGE_LOAD:
        handler = new Request::Handler::AcknowledgeLoad(m_comm, m_range_server,
                                                    event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_COMPACT:
        handler = new Request::Handler::Compact(m_comm, m_range_server,
                                            event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_LOAD_RANGE:
        handler = new Request::Handler::LoadRange(m_comm, m_range_server,
                                              event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_UPDATE:
        handler = new Request::Handler::Update(m_comm, m_range_server,
                                           event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_CREATE_SCANNER:
        handler = new Request::Handler::CreateScanner(m_comm,
            m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_DESTROY_SCANNER:
        handler = new Request::Handler::DestroyScanner(m_comm,
            m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_FETCH_SCANBLOCK:
        handler = new Request::Handler::FetchScanblock(m_comm,
            m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_DROP_TABLE:
        handler = new Request::Handler::DropTable(m_comm, m_range_server,
                                              event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_DROP_RANGE:
        handler = new Request::Handler::DropRange(m_comm, m_range_server,
                                              event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_RELINQUISH_RANGE:
        handler = new Request::Handler::RelinquishRange(m_comm, m_range_server,
                                                    event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_STATUS:
        handler = new Request::Handler::Status(m_comm, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_WAIT_FOR_MAINTENANCE:
        handler = new Request::Handler::WaitForMaintenance(m_comm, m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_SHUTDOWN:
        HT_INFO("Received shutdown command");
        m_shutdown = true;
        handler = new Request::Handler::Shutdown(m_comm, m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_DUMP:
        handler = new Request::Handler::Dump(m_comm, m_range_server,
                                         event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_DUMP_PSEUDO_TABLE:
        handler = new Request::Handler::DumpPseudoTable(m_comm, m_range_server,
                                                    event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_GET_STATISTICS:
        handler = new Request::Handler::GetStatistics(m_comm,
            m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_UPDATE_SCHEMA:
        handler = new Request::Handler::UpdateSchema(m_comm,
            m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_HEAPCHECK:
        handler = new Request::Handler::Heapcheck(m_comm, m_range_server,
                                              event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_COMMIT_LOG_SYNC:
        handler = new Request::Handler::CommitLogSync(m_comm, m_range_server, event);
        break;
      case Lib::RangeServer::Protocol::COMMAND_METADATA_SYNC:
        handler = new Request::Handler::MetadataSync(m_comm, m_range_server,
                                                 event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_REPLAY_FRAGMENTS:
        handler = new Request::Handler::ReplayFragments(m_comm, m_range_server,
                                                    event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_PHANTOM_LOAD:
        handler = new Request::Handler::PhantomLoad(m_comm, m_range_server,
                                                   event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_PHANTOM_UPDATE:
        handler = new Request::Handler::PhantomUpdate(m_comm, m_range_server,
                                                  event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_PHANTOM_PREPARE_RANGES:
        handler = new Request::Handler::PhantomPrepareRanges(m_comm, m_range_server,
                                                         event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_PHANTOM_COMMIT_RANGES:
        handler = new Request::Handler::PhantomCommitRanges(m_comm, m_range_server,
                                                        event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_SET_STATE:
        handler = new Request::Handler::SetState(m_comm, m_range_server,
                                             event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_TABLE_MAINTENANCE_ENABLE:
        handler = new Request::Handler::TableMaintenanceEnable(m_comm, m_range_server, event);
        break;

      case Lib::RangeServer::Protocol::COMMAND_TABLE_MAINTENANCE_DISABLE:
        handler = new Request::Handler::TableMaintenanceDisable(m_comm, m_range_server, event);
        break;

      default:
        HT_THROWF(Error::PROTOCOL_ERROR, "Unimplemented command (%llu)",
                  (Llu)event->header.command);
      }
      m_app_queue->add(handler);
    }
    catch (Exception &e) {
      ResponseCallback cb(m_comm, event);
      HT_ERROR_OUT << e << HT_END;
      std::string errmsg = format("%s - %s", e.what(), Error::get_text(e.code()));
      cb.error(Error::PROTOCOL_ERROR, errmsg);
    }
  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }

}
