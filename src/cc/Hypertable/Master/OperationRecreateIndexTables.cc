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

/// @file
/// Definitions for OperationRecreateIndexTables.
/// This file contains definitions for OperationRecreateIndexTables, an
/// Operation class for reconstructing a table's index tables.

#include <Common/Compat.h>
#include "OperationRecreateIndexTables.h"

#include <Hypertable/Master/OperationCreateTable.h>
#include <Hypertable/Master/OperationDropTable.h>
#include <Hypertable/Master/OperationToggleTableMaintenance.h>
#include <Hypertable/Master/ReferenceManager.h>
#include <Hypertable/Master/Utility.h>

#include <Hypertable/Lib/Key.h>

#include <Hyperspace/Session.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>

#include <boost/algorithm/string.hpp>

#include <poll.h>

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationRecreateIndexTables::OperationRecreateIndexTables(ContextPtr &context,
                                                       std::string table_name,
                                                       TableParts table_parts) :
  Operation(context, MetaLog::EntityType::OPERATION_RECREATE_INDEX_TABLES),
  m_name(table_name), m_parts(table_parts) {
  Utility::canonicalize_pathname(m_name);
  m_exclusivities.insert(m_name);
}


OperationRecreateIndexTables::OperationRecreateIndexTables(ContextPtr &context,
                                                           const MetaLog::EntityHeader &header)
  : Operation(context, header) {
}

OperationRecreateIndexTables::OperationRecreateIndexTables(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_RECREATE_INDEX_TABLES) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  Utility::canonicalize_pathname(m_name);
  m_exclusivities.insert(m_name);
}

void OperationRecreateIndexTables::execute() {
  std::vector<Entity *> entities;
  Operation *op;
  int32_t state = get_state();

  HT_INFOF("Entering RecreateIndexTables-%lld (table=%s, parts=%s) state=%s",
           (Lld)header.id, m_name.c_str(),
           m_parts.to_string().c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    {
      string schema_str;
      if (!fetch_schema(schema_str))
        break;
      SchemaPtr schema = Schema::new_instance(schema_str);
      if (!schema->is_valid()) {
        complete_error(Error::SCHEMA_PARSE_ERROR, schema->get_error_string());
        break;
      }
      uint8_t parts = 0;
      for (auto cf : schema->get_column_families()) {
        if (m_parts.value_index() && cf->has_index)
          parts |= TableParts::VALUE_INDEX;
        if (m_parts.qualifier_index() && cf->has_qualifier_index)
          parts |= TableParts::QUALIFIER_INDEX;
      }
      if (parts == 0) {
        complete_error(Error::NOTHING_TO_DO, m_name);
        break;
      }
      m_parts = TableParts(parts);
    }
    set_state(OperationState::SUSPEND_TABLE_MAINTENANCE);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("recreate-index-tables-INITIAL");

    // drop through ...

  case OperationState::SUSPEND_TABLE_MAINTENANCE:
    HT_ASSERT(entities.empty());
    op = new OperationToggleTableMaintenance(m_context, m_name,
                                             TableMaintenance::OFF);
    stage_subop(op);
    set_state(OperationState::DROP_INDICES);
    entities.push_back(this);
    entities.push_back(op);
    m_context->mml_writer->record_state(entities);
    break;

  case OperationState::DROP_INDICES:
    HT_ASSERT(entities.empty());
    if (!fetch_and_validate_subop(entities))
      break;
    op = new OperationDropTable(m_context, m_name, true, m_parts);
    stage_subop(op);
    entities.push_back(op);
    entities.push_back(this);
    set_state(OperationState::CREATE_INDICES);
    record_state(entities);
    break;

  case OperationState::CREATE_INDICES:
    HT_ASSERT(entities.empty());
    if (!fetch_and_validate_subop(entities))
      break;
    if (m_parts) {
      string schema;
      if (!fetch_schema(schema))
        break;
      op = new OperationCreateTable(m_context, m_name, schema, m_parts);
      stage_subop(op);
      entities.push_back(op);
    }
    entities.push_back(this);
    set_state(OperationState::RESUME_TABLE_MAINTENANCE);
    record_state(entities);
    break;

  case OperationState::RESUME_TABLE_MAINTENANCE:
    HT_ASSERT(entities.empty());
    if (!fetch_and_validate_subop(entities))
      break;
    op = new OperationToggleTableMaintenance(m_context, m_name,
                                             TableMaintenance::ON);
    stage_subop(op);
    entities.push_back(op);
    entities.push_back(this);
    set_state(OperationState::FINALIZE);
    HT_MAYBE_FAIL("recreate-index-tables-RESUME_TABLE_MAINTENANCE-a");
    record_state(entities);
    HT_MAYBE_FAIL("recreate-index-tables-RESUME_TABLE_MAINTENANCE-b");
    break;

  case OperationState::FINALIZE:
    HT_ASSERT(entities.empty());
    if (!fetch_and_validate_subop(entities))
      break;
    complete_ok(entities);
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving RecreateIndexTables-%lld (table=%s, parts=%s) state=%s",
           (Lld)header.id, m_name.c_str(),
           m_parts.to_string().c_str(),
           OperationState::get_text(get_state()));
}

void OperationRecreateIndexTables::display_state(std::ostream &os) {
  os << " table_name=" << m_name
     << " parts=" << m_parts.to_string()
     << " sub_op="  << m_subop_hash_code;
}

#define OPERATION_RECREATE_INDEX_TABLES_VERSION 1

uint16_t OperationRecreateIndexTables::encoding_version() const {
  return OPERATION_RECREATE_INDEX_TABLES_VERSION;
}

size_t OperationRecreateIndexTables::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name) +
    m_parts.encoded_length() + 8;
}

void OperationRecreateIndexTables::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  m_parts.encode(bufp);
  Serialization::encode_i64(bufp, m_subop_hash_code);
}

void OperationRecreateIndexTables::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_subop_hash_code = Serialization::decode_i64(bufp, remainp);
}

void OperationRecreateIndexTables::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_parts.decode(bufp, remainp);
}

const String OperationRecreateIndexTables::name() {
  return "OperationRecreateIndexTables";
}

const String OperationRecreateIndexTables::label() {
  return format("Recreate Index Tables (table=%s, parts=%s)",
                m_name.c_str(), m_parts.to_string().c_str());
}

bool OperationRecreateIndexTables::fetch_schema(std::string &schema) {
  string table_id;
  bool is_namespace;
  if (m_context->namemap->name_to_id(m_name, table_id, &is_namespace)) {
    if (is_namespace) {
      complete_error(Error::TABLE_NOT_FOUND, format("%s is a namespace", m_name.c_str()));
      return false;
    }
  }
  else {
    complete_error(Error::TABLE_NOT_FOUND, m_name);
    return false;
  }
  DynamicBuffer value_buf;
  string filename = m_context->toplevel_dir + "/tables/" + table_id;
  m_context->hyperspace->attr_get(filename, "schema", value_buf);
  schema = string((char *)value_buf.base, strlen((char *)value_buf.base));
  return true;
}


bool OperationRecreateIndexTables::fetch_and_validate_subop(vector<Entity *> &entities) {
  if (m_subop_hash_code) {
    OperationPtr op = m_context->reference_manager->get(m_subop_hash_code);
    op->remove_approval_add(0x01);
    m_subop_hash_code = 0;
    if (op->get_error()) {
      complete_error(op->get_error(), op->get_error_msg(), op.get());
      return false;
    }
    entities.push_back(op.get());
    string dependency_string =
      format("RECREATE INDEX TABLES subop %lld", (Lld)op->hash_code());
    ScopedLock lock(m_mutex);
    m_dependencies.erase(dependency_string);
  }
  return true;
}

void OperationRecreateIndexTables::stage_subop(Operation *operation) {
  string dependency_string =
    format("RECREATE INDEX TABLES subop %lld", (Lld)operation->hash_code());
  operation->add_obstruction(dependency_string);
  operation->set_remove_approval_mask(0x01);
  m_context->reference_manager->add(operation);
  {
    ScopedLock lock(m_mutex);
    add_dependency(dependency_string);
    m_sub_ops.push_back(operation);
    m_subop_hash_code = operation->hash_code();
  }
}
