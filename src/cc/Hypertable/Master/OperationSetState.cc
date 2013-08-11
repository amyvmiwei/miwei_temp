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
 * Definitions for OperationSetState.
 * This file contains definitions for OperationSetState, an Operation class
 * for setting system state variables.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Key.h"

#include "DispatchHandlerOperationSetState.h"
#include "OperationSetState.h"
#include "Utility.h"

#include <boost/algorithm/string.hpp>

#define SET_VARIABLES_VERSION 1

using namespace Hypertable;

OperationSetState::OperationSetState(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_SET) {
  initialize_dependencies();
}

OperationSetState::OperationSetState(ContextPtr &context,
                                     const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationSetState::OperationSetState(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_SET) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
  m_context->system_state->admin_set(m_specs);
}

void OperationSetState::initialize_dependencies() {
  m_exclusivities.insert("SET VARIABLES");
}

void OperationSetState::execute() {
  int32_t state = get_state();
  DispatchHandlerOperationSetState dispatch_handler(m_context);

  HT_INFOF("Entering SetState-%lld() state=%s",
           (Lld)header.id, OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    // Check for and send notifications
    {
      std::vector<NotificationMessage> notifications;
      if (m_context->system_state->get_notifications(notifications)) {
        foreach_ht (NotificationMessage &msg, notifications)
          m_context->notification_hook(msg.subject, msg.body);
      }
    }

    m_context->system_state->get(m_specs, &m_generation);

    HT_INFOF("specs = %s", SystemVariable::specs_to_string(m_specs).c_str());

    set_state(OperationState::STARTED);
    {
      std::vector<Entity *> entities;
      entities.push_back(this);
      entities.push_back(m_context->system_state.get());
      m_context->mml_writer->record_state(entities);
    }

  case OperationState::STARTED:

    {
      StringSet locations;
      std::vector<RangeServerConnectionPtr> servers;
      m_context->rsc_manager->get_servers(servers);
      if (servers.empty()) {
        complete_ok();
        break;
      }
      foreach_ht (RangeServerConnectionPtr &rsc, servers)
        locations.insert(rsc->location());
      dispatch_handler.initialize();
      dispatch_handler.DispatchHandlerOperation::start(locations);
    }

    if (!dispatch_handler.wait_for_completion()) {
      std::set<DispatchHandlerOperation::Result> results;
      dispatch_handler.get_results(results);
      foreach_ht (const DispatchHandlerOperation::Result &result, results) {
        if (result.error != Error::OK)
          HT_WARNF("Problem setting state variables at %s - %s",
                   result.location.c_str(), Error::get_text(result.error));
      }
    }

    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving SetState-%lld", (Lld)header.id);
}


void OperationSetState::display_state(std::ostream &os) {
  bool first = true;
  foreach_ht (const SystemVariable::Spec &spec, m_specs) {
    if (!first)
      os << ",";
    os << SystemVariable::code_to_string(spec.code) << "=" << (spec.value ? "true" : "false");
    first = false;
  }
  os << " ";
}

size_t OperationSetState::encoded_state_length() const {
  return 16 + (5 * m_specs.size());
}

void OperationSetState::encode_state(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, SET_VARIABLES_VERSION);
  Serialization::encode_i64(bufp, m_generation);
  Serialization::encode_i32(bufp, m_specs.size());
  foreach_ht (const SystemVariable::Spec &spec, m_specs) {
    Serialization::encode_i32(bufp, spec.code);
    Serialization::encode_bool(bufp, spec.value);
  }
}

void OperationSetState::decode_state(const uint8_t **bufp, size_t *remainp) {
  SystemVariable::Spec spec;
  m_specs.clear();
  // skip version
  Serialization::decode_i32(bufp, remainp);
  m_generation = Serialization::decode_i64(bufp, remainp);
  int32_t count = Serialization::decode_i32(bufp, remainp);
  for (int32_t i=0; i<count; i++) {
    spec.code  = Serialization::decode_i32(bufp, remainp);
    spec.value = Serialization::decode_bool(bufp, remainp);
    m_specs.push_back(spec);
  }
}

void OperationSetState::decode_request(const uint8_t **bufp, size_t *remainp) {
  int32_t count = Serialization::decode_i32(bufp, remainp);
  SystemVariable::Spec spec;
  for (int32_t i=0; i<count; i++) {
    spec.code = Serialization::decode_i32(bufp, remainp);
    spec.value = Serialization::decode_bool(bufp, remainp);
    m_specs.push_back(spec);
  }
}

const String OperationSetState::name() {
  return "OperationSetState";
}

const String OperationSetState::label() {
  return String("SetState ") + SystemVariable::specs_to_string(m_specs);
}

