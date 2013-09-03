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
 * Definitions for OperationCompact.
 * This file contains definitions for OperationCompact, an Operation class
 * for carrying out a manual compaction operation.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Key.h"

#include "DispatchHandlerOperationCompact.h"
#include "OperationCompact.h"
#include "Utility.h"

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hyperspace;

OperationCompact::OperationCompact(ContextPtr &context,
                                   const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationCompact::OperationCompact(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_COMPACT) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationCompact::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
  m_exclusivities.insert(m_name);
  add_dependency(Dependency::INIT);
}

void OperationCompact::execute() {
  bool is_namespace;
  StringSet servers;
  DispatchHandlerOperationPtr op_handler;
  TableIdentifier table;
  int32_t state = get_state();

  HT_INFOF("Entering Compact-%lld (table=%s, row=%s) state=%s",
           (Lld)header.id, m_name.c_str(), m_row.c_str(),
           OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    if (!m_name.empty()) {
      if (m_context->namemap->name_to_id(m_name, m_id, &is_namespace)) {
        if (is_namespace) {
          complete_error(Error::TABLE_NOT_FOUND, format("%s is a namespace", m_name.c_str()));
          return;
        }
        set_state(OperationState::SCAN_METADATA);
      }
      else {
        complete_error(Error::TABLE_NOT_FOUND, m_name);
        return;
      }
    }
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::METADATA);
    }
    HT_MAYBE_FAIL("compact-INITIAL");

  case OperationState::SCAN_METADATA:
    servers.clear();
    if (!m_name.empty())
      Utility::get_table_server_set(m_context, m_id, m_row, servers);
    else
      m_context->get_available_servers(servers);
    {
      ScopedLock lock(m_mutex);
      m_servers.clear();
      m_dependencies.clear();
      for (StringSet::iterator iter=servers.begin(); iter!=servers.end(); ++iter) {
        if (m_completed.count(*iter) == 0) {
          m_dependencies.insert(*iter);
          m_servers.insert(*iter);
        }
      }
      m_state = OperationState::ISSUE_REQUESTS;
    }
    m_context->mml_writer->record_state(this);
    return;

  case OperationState::ISSUE_REQUESTS:
    table.id = m_id.c_str();
    table.generation = 0;
    op_handler = new DispatchHandlerOperationCompact(m_context, table, m_row, m_range_types);
    op_handler->start(m_servers);
    if (!op_handler->wait_for_completion()) {
      std::set<DispatchHandlerOperation::Result> results;
      op_handler->get_results(results);
      foreach_ht (const DispatchHandlerOperation::Result &result, results) {
        if (result.error == Error::OK ||
            result.error == Error::TABLE_NOT_FOUND ||
            result.error == Error::RANGESERVER_TABLE_DROPPED) {
          ScopedLock lock(m_mutex);
          m_completed.insert(result.location);
        }
        else
          HT_WARNF("Compact error at %s - %s (%s)", result.location.c_str(),
                   Error::get_text(result.error), result.msg.c_str());
      }

      {
        ScopedLock lock(m_mutex);
        m_servers.clear();
        m_dependencies.clear();
        m_dependencies.insert(Dependency::METADATA);
        m_state = OperationState::SCAN_METADATA;
      }
      m_context->mml_writer->record_state(this);
      return;
    }
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving Compact-%lld (table=%s, row=%s) state=%s",
           (Lld)header.id, m_name.c_str(), m_row.c_str(),
           OperationState::get_text(get_state()));
}


void OperationCompact::display_state(std::ostream &os) {
  os << " name=" << m_name << " id=" << m_id << " " 
     << " row=" << m_row << " range_types=" 
     << RangeServerProtocol::compact_flags_to_string(m_range_types);
}

#define OPERATION_COMPACT_VERSION 2

uint16_t OperationCompact::encoding_version() const {
  return OPERATION_COMPACT_VERSION;
}

size_t OperationCompact::encoded_state_length() const {
  size_t length = Serialization::encoded_length_vstr(m_name) +
    Serialization::encoded_length_vstr(m_row) + 4 +
    Serialization::encoded_length_vstr(m_id);
  length += 4;
  foreach_ht (const String &location, m_completed)
    length += Serialization::encoded_length_vstr(location);
  length += 4;
  foreach_ht (const String &location, m_servers)
    length += Serialization::encoded_length_vstr(location);
  return length;
}

void OperationCompact::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_vstr(bufp, m_row);
  Serialization::encode_i32(bufp, m_range_types);
  Serialization::encode_vstr(bufp, m_id);
  Serialization::encode_i32(bufp, m_completed.size());
  foreach_ht (const String &location, m_completed)
    Serialization::encode_vstr(bufp, location);
  Serialization::encode_i32(bufp, m_servers.size());
  foreach_ht (const String &location, m_servers)
    Serialization::encode_vstr(bufp, location);
}

void OperationCompact::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
  size_t length = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<length; i++)
    m_completed.insert( Serialization::decode_vstr(bufp, remainp) );
  if (m_decode_version >= 2) {
    length = Serialization::decode_i32(bufp, remainp);
    for (size_t i=0; i<length; i++)
      m_servers.insert( Serialization::decode_vstr(bufp, remainp) );
  }
}

void OperationCompact::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_row  = Serialization::decode_vstr(bufp, remainp);
  m_range_types = Serialization::decode_i32(bufp, remainp);
}

const String OperationCompact::name() {
  return "OperationCompact";
}

const String OperationCompact::label() {
  return format("Compact table='%s' row='%s'", m_name.c_str(), m_row.c_str());
}

