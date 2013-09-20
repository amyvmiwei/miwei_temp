/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Definitions for ConnectionHandler.
 * This file contains definitions for ConnectionHandler, a class for handling
 * incoming Master requests.
 */

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/StringExt.h"
#include "Common/Serialization.h"
#include "Common/Time.h"

#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/MasterProtocol.h"

#include "ConnectionHandler.h"
#include "LoadBalancer.h"

#include "OperationAlterTable.h"
#include "OperationBalance.h"
#include "OperationCollectGarbage.h"
#include "OperationCompact.h"
#include "OperationCreateNamespace.h"
#include "OperationCreateTable.h"
#include "OperationDropNamespace.h"
#include "OperationDropTable.h"
#include "OperationGatherStatistics.h"
#include "OperationProcessor.h"
#include "OperationMoveRange.h"
#include "OperationRecover.h"
#include "OperationRegisterServer.h"
#include "OperationRelinquishAcknowledge.h"
#include "OperationRenameTable.h"
#include "OperationSetState.h"
#include "OperationStatus.h"
#include "OperationStop.h"
#include "OperationTimedBarrier.h"
#include "RangeServerConnection.h"
#include "ReferenceManager.h"

#include <fstream>
#include <iostream>

using namespace Hypertable;
using namespace Serialization;
using namespace Error;


/**
 *
 */
ConnectionHandler::ConnectionHandler(ContextPtr &context) : m_context(context), m_shutdown(false) {
  int error;
  if ((error = m_context->comm->set_timer(context->timer_interval, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));
}


/**
 *
 */
void ConnectionHandler::handle(EventPtr &event) {
  OperationPtr operation;
  boost::xtime expire_time;

  if (m_shutdown &&
      event->header.command != MasterProtocol::COMMAND_SHUTDOWN &&
      event->header.command != MasterProtocol::COMMAND_STATUS) {
    ResponseCallback cb(m_context->comm, event);
    cb.error(Error::SERVER_SHUTTING_DOWN, "");
    return;
  }

  if (event->type == Event::MESSAGE) {

    //event->display()

    try {
      // sanity check command code
      if (event->header.command >= MasterProtocol::COMMAND_MAX)
        HT_THROWF(PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case MasterProtocol::COMMAND_COMPACT:
        operation = new OperationCompact(m_context, event);
        break;
      case MasterProtocol::COMMAND_CREATE_TABLE:
        operation = new OperationCreateTable(m_context, event);
        break;
      case MasterProtocol::COMMAND_DROP_TABLE:
        operation = new OperationDropTable(m_context, event);
        break;
      case MasterProtocol::COMMAND_ALTER_TABLE:
        operation = new OperationAlterTable(m_context, event);
        break;
      case MasterProtocol::COMMAND_RENAME_TABLE:
        operation = new OperationRenameTable(m_context, event);
        break;
      case MasterProtocol::COMMAND_STATUS:
        operation = new OperationStatus(m_context, event);
        break;
      case MasterProtocol::COMMAND_REGISTER_SERVER:
        operation = new OperationRegisterServer(m_context, event);
        m_context->op->add_operation(operation);
        return;
      case MasterProtocol::COMMAND_MOVE_RANGE:
        operation = new OperationMoveRange(m_context, event);
        if (!m_context->reference_manager->add(operation)) {
          HT_INFOF("Skipping %s because already in progress",
                  operation->label().c_str());
          send_error_response(event, Error::MASTER_OPERATION_IN_PROGRESS, "");
          return;
        }
        HT_MAYBE_FAIL("connection-handler-move-range");
        m_context->op->add_operation(operation);
        return;
      case MasterProtocol::COMMAND_RELINQUISH_ACKNOWLEDGE:
        operation = new OperationRelinquishAcknowledge(m_context, event);
        break;
      case MasterProtocol::COMMAND_BALANCE:
        operation = new OperationBalance(m_context, event);
        break;
      case MasterProtocol::COMMAND_SET:
        operation = new OperationSetState(m_context, event);
        break;
      case MasterProtocol::COMMAND_STOP:
        operation = new OperationStop(m_context, event);
        break;
      case MasterProtocol::COMMAND_SHUTDOWN:
        HT_INFO("Received shutdown command");
        m_shutdown = true;
        if (m_context->recovery_barrier_op)
          m_context->recovery_barrier_op->shutdown();
        boost::xtime_get(&expire_time, boost::TIME_UTC_);
        expire_time.sec += 15;
        m_context->op->timed_wait_for_idle(expire_time);
        m_context->op->shutdown();
        return;
      case MasterProtocol::COMMAND_CREATE_NAMESPACE:
        operation = new OperationCreateNamespace(m_context, event);
        break;
      case MasterProtocol::COMMAND_DROP_NAMESPACE:
        operation = new OperationDropNamespace(m_context, event);
        break;

      case MasterProtocol::COMMAND_FETCH_RESULT:
        m_context->response_manager->add_delivery_info(event);
        return;
      case MasterProtocol::COMMAND_REPLAY_STATUS:
        m_context->replay_status(event);
        send_ok_response(event);
        return;
      case MasterProtocol::COMMAND_REPLAY_COMPLETE:
        m_context->replay_complete(event);
        send_ok_response(event);
        return;
      case MasterProtocol::COMMAND_PHANTOM_PREPARE_COMPLETE:
        m_context->prepare_complete(event);
        send_ok_response(event);
        return;
      case MasterProtocol::COMMAND_PHANTOM_COMMIT_COMPLETE:
        m_context->commit_complete(event);
        send_ok_response(event);
        return;
      default:
        HT_THROWF(PROTOCOL_ERROR, "Unimplemented command (%llu)",
                  (Llu)event->header.command);
      }
      if (operation) {
        HT_MAYBE_FAIL_X("connection-handler-before-id-response",
                event->header.command != MasterProtocol::COMMAND_STATUS &&
                event->header.command != MasterProtocol::COMMAND_RELINQUISH_ACKNOWLEDGE);
        if (send_id_response(event, operation) != Error::OK)
          return;
        m_context->op->add_operation(operation);
      }
      else {
        ResponseCallback cb(m_context->comm, event);
        cb.error(Error::PROTOCOL_ERROR,
                format("Unimplemented command (%llu)",
                    (Llu)event->header.command));
      }
    }
    catch (Exception &e) {
      if (e.code() == Error::MASTER_OPERATION_IN_PROGRESS)
        HT_WARNF("%s", e.what());
      else
        HT_ERROR_OUT << e << HT_END;
      if (operation) {
        operation->set_ephemeral();
        operation->complete_error(e.code(), e.what());
        m_context->response_manager->add_operation(operation);
      }
    }
  }
  else if (event->type == Hypertable::Event::TIMER && !m_shutdown) {
    OperationPtr operation;
    int error;
    time_t now = time(0);

    try {

      maybe_dump_op_statistics();

      if (m_context->next_monitoring_time <= now) {
        operation = new OperationGatherStatistics(m_context);
        m_context->op->add_operation(operation);
        m_context->next_monitoring_time = now + (m_context->monitoring_interval/1000) - 1;
      }

      if (m_context->next_gc_time <= now) {
        operation = new OperationCollectGarbage(m_context);
        m_context->op->add_operation(operation);
        m_context->next_gc_time = now + (m_context->gc_interval/1000) - 1;
      }

      if (m_context->balancer->balance_needed()) {
        operation = new OperationBalance(m_context);
        m_context->op->add_operation(operation);
      }
    }
    catch (Exception &e) {
      if (e.code() == Error::MASTER_OPERATION_IN_PROGRESS)
        HT_WARNF("%s", e.what());
      else
        HT_ERROR_OUT << e << HT_END;
    }

    if ((error = m_context->comm->set_timer(m_context->timer_interval, this)) != Error::OK)
      HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }

}

int32_t ConnectionHandler::send_id_response(EventPtr &event, OperationPtr &operation) {
  int error = Error::OK;
  CommHeader header;
  header.initialize_from_request_header(event->header);
  CommBufPtr cbp(new CommBuf(header, 12));
  cbp->append_i32(Error::OK);
  cbp->append_i64(operation->id());
  error = m_context->comm->send_response(event->addr, cbp);
  if (error != Error::OK)
    HT_ERRORF("Problem sending ID response back for %s operation (id=%lld) - %s",
              operation->label().c_str(), (Lld)operation->id(), Error::get_text(error));
  return error;
}

int32_t ConnectionHandler::send_ok_response(EventPtr &event) {
  CommHeader header;
  header.initialize_from_request_header(event->header);
  CommBufPtr cbp(new CommBuf(header, 4));
  cbp->append_i32(Error::OK);
  int ret = m_context->comm->send_response(event->addr, cbp);
  if (ret != Error::OK)
    HT_ERRORF("Problem sending error response back to %s - %s",
              event->addr.format().c_str(), Error::get_text(ret));
  return ret;
}

int32_t ConnectionHandler::send_error_response(EventPtr &event, int32_t error, const String &msg) {
  CommHeader header;
  header.initialize_from_request_header(event->header);
  CommBufPtr cbp(new CommBuf(header, 4 + Serialization::encoded_length_vstr(msg)));
  cbp->append_i32(error);
  cbp->append_vstr(msg);
  int ret = m_context->comm->send_response(event->addr, cbp);
  if (ret != Error::OK)
    HT_ERRORF("Problem sending error response back to %s - %s",
              event->addr.format().c_str(), Error::get_text(error));
  return error;
}

void ConnectionHandler::maybe_dump_op_statistics() {

  if (FileUtils::exists(System::install_dir + "/run/debug-op")) {
    String description;
    String output_fname = System::install_dir + "/run/op.output";
    ofstream out;
    out.open(output_fname.c_str());
    m_context->op->state_description(description);
    out << description;
    out.close();
    FileUtils::unlink(System::install_dir + "/run/debug-op");
  }
  
}
