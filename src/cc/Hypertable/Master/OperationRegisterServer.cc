/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#include "OperationRegisterServer.h"

#include <Hypertable/Master/LoadBalancer.h>
#include <Hypertable/Master/OperationProcessor.h>
#include <Hypertable/Master/RangeServerHyperspaceCallback.h>
#include <Hypertable/Master/Utility.h>

#include <Hypertable/Lib/RangeServer/Client.h>
#include <Hypertable/Lib/RangeServer/Protocol.h>
#include <Hypertable/Lib/Master/Response/Parameters/RegisterServer.h>

#include <Hyperspace/Session.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/Serialization.h>
#include <Common/StringExt.h>
#include <Common/Time.h>
#include <Common/md5.h>

#include <boost/algorithm/string.hpp>

#include <cmath>

using namespace Hypertable;
using namespace std;

OperationRegisterServer::OperationRegisterServer(ContextPtr &context,
                                                 EventPtr &event)
  : OperationEphemeral(context, event,
                       MetaLog::EntityType::OPERATION_REGISTER_SERVER) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);

  m_location = m_params.location();

  if (!m_location.empty()) {
    add_exclusivity(String("RegisterServer ") + m_location);
    add_dependency(String("RegisterServerBlocker ") + m_location);
  }

  m_local_addr = InetAddr(event->addr);
  m_public_addr = InetAddr(m_params.system_stats().net_info.primary_addr,
                           m_params.listen_port());
  m_received_ts = get_ts64();
}


void OperationRegisterServer::execute() {
  std::vector<SystemVariable::Spec> specs;
  uint64_t generation {};
  uint64_t handle {};
  RangeServerHyperspaceCallback *hyperspace_callback {};
  bool newly_created {};

  if (!m_location.empty()) {

    // This shouldn't be necessary, however we've seen Hyperspace declare a
    // server to be dead when it was in fact still alive (see issue 1346).
    // We'll leave this in place until Hyperspace gets overhauled.
    if (m_context->recovered_servers->contains(m_location)) {
      CommAddress addr(m_local_addr);
      Utility::shutdown_rangeserver(m_context, addr);
      HT_WARNF("Attempt to register server %s that has been removed",
               m_location.c_str());
      complete_ok();
      return;
    }

    m_context->rsc_manager->find_server_by_location(m_location, m_rsc);

  }
  else {
    uint64_t id = m_context->master_file->next_server_id();
    if (m_context->location_hash.empty())
      m_location = format("rs%llu", (Llu)id);
    else
      m_location = format("rs-%s-%llu", m_context->location_hash.c_str(),
                          (Llu)id);
    newly_created = true;

    String fname = m_context->toplevel_dir + "/servers/";
    // !!! wrap in try/catch
    m_context->hyperspace->mkdirs(fname);
    fname += m_location;
    uint32_t oflags = Hyperspace::OPEN_FLAG_READ | Hyperspace::OPEN_FLAG_CREATE;
    // !!! wrap in try/catch
    handle = m_context->hyperspace->open(fname, oflags);
    m_context->hyperspace->close(handle);
  }

  if (!m_rsc)
    m_rsc = make_shared<RangeServerConnection>(m_location, m_params.system_stats().net_info.host_name, m_public_addr);
  else
    m_context->rsc_manager->disconnect_server(m_rsc);

  if (!m_rsc->get_hyperspace_handle(&handle, &hyperspace_callback)) {
    String fname = m_context->toplevel_dir + "/servers/" + m_location;
    hyperspace_callback
      = new RangeServerHyperspaceCallback(m_context, m_rsc);
    Hyperspace::HandleCallbackPtr cb(hyperspace_callback);
    handle = m_context->hyperspace->open(fname, Hyperspace::OPEN_FLAG_READ|
                                         Hyperspace::OPEN_FLAG_CREATE, cb);
    HT_ASSERT(handle);
    m_rsc->set_hyperspace_handle(handle, hyperspace_callback);
  }

  if (m_context->rsc_manager->is_connected(m_location)) {
    complete_error(Error::CONNECT_ERROR_MASTER,
                   format("%s already connected", m_location.c_str()));
    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    header.command = Lib::RangeServer::Protocol::COMMAND_INITIALIZE;
    m_context->system_state->get(specs, &generation);
    CommBufPtr cbp(new CommBuf(header, encoded_response_length(generation,
                                                               specs)));
    encode_response(generation, specs, cbp->get_data_ptr_address());
    int error = m_context->comm->send_response(m_local_addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending response (location=%s) back to %s",
                m_location.c_str(), m_local_addr.format().c_str());
    return;
  }

  int32_t difference = (int32_t)abs((m_received_ts - m_params.now()) / 1000LL);
  if (difference > (3000000 + m_context->max_allowable_skew)) {
    String errstr = format("Detected clock skew while registering server "
            "%s (%s), as location %s register_ts=%llu, received_ts=%llu, "
            "difference=%d > allowable skew %d", 
            m_params.system_stats().net_info.host_name.c_str(), 
            m_public_addr.format().c_str(), m_location.c_str(), 
            (Llu)m_params.now(), (Llu)m_received_ts, difference, 
            m_context->max_allowable_skew);
    String subject = format("Clock skew detected while registering %s (%s)",
                            m_params.system_stats().net_info.host_name.c_str(),
                            m_location.c_str());
    m_context->notification_hook(subject, errstr);
    complete_error(Error::RANGESERVER_CLOCK_SKEW, errstr);
    HT_ERROR_OUT << errstr << HT_END;
    // clock skew detected by master
    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    header.command = Lib::RangeServer::Protocol::COMMAND_INITIALIZE;
    m_context->system_state->get(specs, &generation);
    CommBufPtr cbp(new CommBuf(header, encoded_response_length(generation,
                                                               specs)));

    encode_response(generation, specs, cbp->get_data_ptr_address());
    int error = m_context->comm->send_response(m_local_addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending response (location=%s) back to %s",
              m_location.c_str(), m_local_addr.format().c_str());
    m_context->op->unblock(m_location);
    m_context->op->unblock(Dependency::SERVERS);
    m_context->op->unblock(Dependency::RECOVERY_BLOCKER);
    HT_INFOF("%lld Leaving RegisterServer %s",
            (Lld)header.id, m_rsc->location().c_str());
    return;
  }
  else {
    m_context->monitoring->add_server(m_location, m_params.system_stats());
    String message = format("Registering server %s\nhost = %s\n"
                            "public addr = %s\nlocal addr = %s\n",
                            m_rsc->location().c_str(),
                            m_params.system_stats().net_info.host_name.c_str(),
                            m_public_addr.format().c_str(),
                            m_local_addr.format().c_str());

    if (newly_created) {
      String subject
        = format("Registering server %s - %s (%s)", m_location.c_str(),
                 m_params.system_stats().net_info.host_name.c_str(),
                 m_public_addr.format().c_str());
      m_context->notification_hook(subject, message);
    }
    boost::replace_all(message, "\n", " ");
    HT_INFOF("%lld %s", (Lld)header.id, message.c_str());

    /** Send back Response **/
    {
      CommHeader header;
      header.initialize_from_request_header(m_event->header);
      header.command = Lib::RangeServer::Protocol::COMMAND_INITIALIZE;
      m_context->system_state->get(specs, &generation);
      CommBufPtr cbp(new CommBuf(header, encoded_response_length(generation,
                                                                 specs)));
      encode_response(generation, specs, cbp->get_data_ptr_address());
      int error = m_context->comm->send_response(m_local_addr, cbp);
      if (error != Error::OK)
        HT_ERRORF("Problem sending response (location=%s) back to %s",
            m_location.c_str(), m_local_addr.format().c_str());
    }
  }

  // Wait for server to acquire lock on Hyperspace file
  if (!m_params.lock_held()) {
    if (!hyperspace_callback->wait_for_lock_acquisition(chrono::seconds(120))) {
      String notification_body = format("Timed out waiting for %s to acquire "
                                        "lock on Hyperspace file",
                                        m_location.c_str());
      HT_WARNF("%s, sending shutdown request...", notification_body.c_str());

      CommAddress addr(m_local_addr);
      Utility::shutdown_rangeserver(m_context, addr);
      m_context->rsc_manager->disconnect_server(m_rsc);
      String notification_subject =
        format("Server registration error for %s (%s)", m_location.c_str(),
               m_rsc->hostname().c_str());
      m_context->notification_hook(notification_subject, notification_body);
      OperationPtr operation = make_shared<OperationRecover>(m_context, m_rsc);
      try {
        m_context->op->add_operation(operation);
      }
      catch (Exception &e) {
        // Only exception thrown is Error::MASTER_OPERATION_IN_PROGRESS
      }
      complete_ok();
      return;
    }
  }

  // At this point, any pending Recover operations should be irrelevant
  OperationPtr operation(m_context->op->remove_operation(md5_hash("OperationRecover") ^ md5_hash(m_location.c_str())));
  if (operation)
    operation->complete_ok();

  string hostname(m_params.system_stats().net_info.host_name);
  m_context->comm->set_alias(m_local_addr, m_public_addr);
  m_context->comm->add_proxy(m_rsc->location(), hostname, m_public_addr);
  m_context->rsc_manager->connect_server(m_rsc, hostname, m_local_addr, m_public_addr);

  m_context->add_available_server(m_location);

  // Prevent subsequent registration until lock is released
  operation = make_shared<OperationRegisterServerBlocker>(m_context,
                                                          m_rsc->location());
  m_context->op->add_operation(operation);

  if (!m_rsc->get_balanced())
    m_context->balancer->signal_new_server();

  m_context->mml_writer->record_state(m_rsc);

  complete_ok();
  m_context->op->unblock(m_location);
  m_context->op->unblock(Dependency::SERVERS);
  m_context->op->unblock(Dependency::RECOVERY_BLOCKER);
  HT_INFOF("%lld Leaving RegisterServer %s", 
          (Lld)header.id, m_rsc->location().c_str());
}

void OperationRegisterServer::display_state(std::ostream &os) {
  os << " location=" << m_location << " host="
     << m_params.system_stats().net_info.host_name;
  os << " register_ts=" << m_params.now();
  os << " local_addr=" << m_rsc->local_addr().format();
  os << " public_addr=" << m_rsc->public_addr().format() << " ";
}

const String OperationRegisterServer::name() {
  return "OperationRegisterServer";
}

const String OperationRegisterServer::label() {
  return String("RegisterServer ") + m_location;
}

size_t OperationRegisterServer::encoded_response_length(uint64_t generation,
                             std::vector<SystemVariable::Spec> &specs) const {
  size_t length = 4;
  if (m_error == Error::OK) {
    Lib::Master::Response::Parameters::RegisterServer params(m_location, generation, specs);
    length += params.encoded_length();
  }
  else
    length += Serialization::encoded_length_str16(m_error_msg);
  return length;
}

void OperationRegisterServer::encode_response(uint64_t generation,
                             std::vector<SystemVariable::Spec> &specs,
                                              uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_error);
  if (m_error == Error::OK) {
    Lib::Master::Response::Parameters::RegisterServer params(m_location, generation, specs);
    params.encode(bufp);
  }
  else
    Serialization::encode_str16(bufp, m_error_msg);
}
