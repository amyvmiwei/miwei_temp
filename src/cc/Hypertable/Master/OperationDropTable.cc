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

/** @file
 * Definitions for OperationDropTable.
 * This file contains definitions for OperationDropTable, an Operation class
 * for dropping (removing) a table from the system.
 */

#include <Common/Compat.h>
#include "OperationDropTable.h"

#include <Hypertable/Master/DispatchHandlerOperationDropTable.h>
#include <Hypertable/Master/ReferenceManager.h>
#include <Hypertable/Master/Utility.h>

#include <Hypertable/Lib/Key.h>

#include <Hyperspace/Session.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <iterator>
#include <vector>

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationDropTable::OperationDropTable(ContextPtr &context, const String &name,
                                       bool if_exists, TableParts parts)
  : Operation(context, MetaLog::EntityType::OPERATION_DROP_TABLE),
    m_params(name, if_exists), m_parts(parts) {
}

OperationDropTable::OperationDropTable(ContextPtr &context,
                                       const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationDropTable::OperationDropTable(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_DROP_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_exclusivities.insert(m_params.name());
  m_dependencies.insert(Dependency::INIT);
}

void OperationDropTable::execute() {
  String index_id;
  String qualifier_index_id;
  String index_name = Filesystem::dirname(m_params.name());
  if (index_name == "/")
    index_name += String("^") + Filesystem::basename(m_params.name());
  else
    index_name += String("/^") + Filesystem::basename(m_params.name());
  String qualifier_index_name = Filesystem::dirname(m_params.name());
  if (qualifier_index_name == "/")
    qualifier_index_name += String("^^") + Filesystem::basename(m_params.name());
  else
    qualifier_index_name += String("/^^") + Filesystem::basename(m_params.name());
  bool is_namespace;
  DispatchHandlerOperationPtr op_handler;
  TableIdentifier table;
  int32_t state = get_state();

  HT_INFOF("Entering DropTable-%lld(%s, if_exists=%s, parts=%s) state=%s",
           (Lld)header.id, m_params.name().c_str(),
           m_params.if_exists() ? "true" : "false",
           m_parts.to_string().c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if(m_context->namemap->name_to_id(m_params.name(), m_id, &is_namespace)) {
      if (is_namespace && !m_params.if_exists()) {
        complete_error(Error::TABLE_NOT_FOUND, format("%s is a namespace", m_params.name().c_str()));
        break;
      }
    }
    else {
      if (m_params.if_exists())
        complete_ok();
      else
        complete_error(Error::TABLE_NOT_FOUND, m_params.name());
      break;
    }
    set_state(OperationState::DROP_VALUE_INDEX);
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("drop-table-INITIAL");
    // drop through ...

  case OperationState::DROP_VALUE_INDEX:

    set_state(OperationState::DROP_QUALIFIER_INDEX);

    if (m_parts.value_index() &&
        m_context->namemap->name_to_id(index_name, index_id)) {
      HT_INFOF("  Dropping index table %s (id %s)", 
               index_name.c_str(), index_id.c_str());
      stage_subop(make_shared<OperationDropTable>(m_context, index_name, false,
                                                  TableParts(TableParts::PRIMARY)));
      HT_MAYBE_FAIL("drop-table-DROP_VALUE_INDEX-1");
      record_state();
      HT_MAYBE_FAIL("drop-table-DROP_VALUE_INDEX-2");
      break;
    }
    // drop through ...

  case OperationState::DROP_QUALIFIER_INDEX:

    if (!validate_subops())
      break;

    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(m_id + " move range");
      m_state = OperationState::SCAN_METADATA;
    }

    if (m_parts.qualifier_index() &&
        m_context->namemap->name_to_id(qualifier_index_name, 
                                       qualifier_index_id)) {
      HT_INFOF("  Dropping qualifier index table %s (id %s)", 
               qualifier_index_name.c_str(), qualifier_index_id.c_str());
      stage_subop(make_shared<OperationDropTable>(m_context, qualifier_index_name,
                                                  false, TableParts(TableParts::PRIMARY)));
      HT_MAYBE_FAIL("drop-table-DROP_QUALIFIER_INDEX-1");
      record_state();
      HT_MAYBE_FAIL("drop-table-DROP_QUALIFIER_INDEX-2");
    }
    break;

  case OperationState::SCAN_METADATA:

    if (!validate_subops())
      break;

    if (!m_parts.primary()) {
      complete_ok();
      break;
    }

    {
      StringSet servers;
      Utility::get_table_server_set(m_context, m_id, "", servers);
      {
        ScopedLock lock(m_mutex);
        for (StringSet::iterator iter=servers.begin(); iter!=servers.end(); ++iter) {
          if (m_completed.count(*iter) == 0) {
            m_dependencies.insert(*iter);
            m_servers.insert(*iter);
          }
        }
        m_state = OperationState::ISSUE_REQUESTS;
      }
    }
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("drop-table-SCAN_METADATA");
    break;

  case OperationState::ISSUE_REQUESTS:
    {
      if (!m_context->test_mode) {
        table.id = m_id.c_str();
        table.generation = 0;
        op_handler = make_shared<DispatchHandlerOperationDropTable>(m_context, table);
        op_handler->start(m_servers);
        if (!op_handler->wait_for_completion()) {
          std::set<DispatchHandlerOperation::Result> results;
          op_handler->get_results(results);
          foreach_ht (const DispatchHandlerOperation::Result &result, results) {
            if (result.error == Error::OK ||
                result.error == Error::TABLE_NOT_FOUND) {
              ScopedLock lock(m_mutex);
              m_completed.insert(result.location);
            }
            else
              HT_WARNF("Drop table error at %s - %s (%s)", result.location.c_str(),
                       Error::get_text(result.error), result.msg.c_str());
          }
          {
            ScopedLock lock(m_mutex);
            m_servers.clear();
            m_dependencies.insert(Dependency::METADATA);
            m_dependencies.insert(m_id + " move range");
            m_state = OperationState::SCAN_METADATA;
          }
          m_context->mml_writer->record_state(shared_from_this());
          break;
        }
        m_context->monitoring->invalidate_id_mapping(m_id);
      }
      HT_MAYBE_FAIL("drop-table-ISSUE_REQUESTS");
      set_state(OperationState::UPDATE_HYPERSPACE);
      record_state();
      break;
    }

  case OperationState::UPDATE_HYPERSPACE:

    try {
      m_context->namemap->drop_mapping(m_params.name());
      string filename = m_context->toplevel_dir + "/tables/" + m_id;
      m_context->hyperspace->unlink(filename.c_str());
    }
    catch (Exception &e) {
      if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND &&
          e.code() != Error::HYPERSPACE_BAD_PATHNAME)
        HT_THROW2F(e.code(), e, "Error executing DropTable %s", 
                m_params.name().c_str());
    }
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving DropTable-%lld(%s) state=%s",
           (Lld)header.id, m_params.name().c_str(), OperationState::get_text(m_state));
}


void OperationDropTable::display_state(std::ostream &os) {
  os << " name=" << m_params.name() << " id=" << m_id << " ";
}

uint8_t OperationDropTable::encoding_version_state() const {
  return 1;
}

size_t OperationDropTable::encoded_length_state() const {
  size_t length = m_params.encoded_length() +
    Serialization::encoded_length_vstr(m_id);
  length += 4;
  foreach_ht (const String &location, m_completed)
    length += Serialization::encoded_length_vstr(location);
  length += 4;
  foreach_ht (const String &location, m_servers)
    length += Serialization::encoded_length_vstr(location);
  length += m_parts.encoded_length();
  return length;
}

void OperationDropTable::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  Serialization::encode_vstr(bufp, m_id);
  Serialization::encode_i32(bufp, m_completed.size());
  foreach_ht (const String &location, m_completed)
    Serialization::encode_vstr(bufp, location);
  Serialization::encode_i32(bufp, m_servers.size());
  foreach_ht (const String &location, m_servers)
    Serialization::encode_vstr(bufp, location);
  m_parts.encode(bufp);
}

void OperationDropTable::decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
  size_t length = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<length; i++)
    m_completed.insert( Serialization::decode_vstr(bufp, remainp) );
  length = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<length; i++)
    m_servers.insert( Serialization::decode_vstr(bufp, remainp) );
  m_parts.decode(bufp, remainp);
}

void OperationDropTable::decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  {
    bool if_exists = Serialization::decode_bool(bufp, remainp);
    string name = Serialization::decode_vstr(bufp, remainp);
    m_params = Lib::Master::Request::Parameters::DropTable(name, if_exists);
  }
  m_id = Serialization::decode_vstr(bufp, remainp);
  size_t length = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<length; i++)
    m_completed.insert( Serialization::decode_vstr(bufp, remainp) );
  if (version >= 2) {
    length = Serialization::decode_i32(bufp, remainp);
    for (size_t i=0; i<length; i++)
      m_servers.insert( Serialization::decode_vstr(bufp, remainp) );
    if (version >= 3) {
      int8_t parts = (int8_t)Serialization::decode_byte(bufp, remainp);
      m_parts = TableParts(parts);
    }
  }
}

const String OperationDropTable::name() {
  return "OperationDropTable";
}

const String OperationDropTable::label() {
  return String("DropTable ") + m_params.name();
}

