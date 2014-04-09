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
/// Definitions for OperationCreateTable.
/// This file contains definitions for OperationCreateTable, an Operation class
/// for creating a table.

#include <Common/Compat.h>
#include "OperationCreateTable.h"

#include <Hypertable/Master/BalancePlanAuthority.h>
#include <Hypertable/Master/ReferenceManager.h>
#include <Hypertable/Master/Utility.h>

#include <Hypertable/Lib/Key.h>

#include <Hyperspace/Session.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>

#include <boost/algorithm/string.hpp>

#include <vector>

using namespace Hypertable;
using namespace Hyperspace;

OperationCreateTable::OperationCreateTable(ContextPtr &context,
                                           const String &name,
                                           const String &schema,
                                           TableParts parts)
  : Operation(context, MetaLog::EntityType::OPERATION_CREATE_TABLE),
    m_name(name), m_schema(schema), m_parts(parts) {
  Utility::canonicalize_pathname(m_name);
}

OperationCreateTable::OperationCreateTable(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_CREATE_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  Utility::canonicalize_pathname(m_name);
  m_exclusivities.insert(m_name);
  if (!boost::algorithm::starts_with(m_name, "/sys/"))
    m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
}

void OperationCreateTable::requires_indices(bool &needs_index, 
        bool &needs_qualifier_index) {
  SchemaPtr s;

  s = Schema::new_instance(m_schema.c_str(), m_schema.size());
  foreach_ht (Schema::ColumnFamily *cf, s->get_column_families()) {
    if (cf && !cf->deleted) {
     if (cf->has_index)
       needs_index = true;
     if (cf->has_qualifier_index)
       needs_qualifier_index = true;
    }
    if (needs_index && needs_qualifier_index)
      return;
  }
}

void OperationCreateTable::execute() {
  vector<Entity *> entities;
  RangeSpec range, index_range, qualifier_index_range;
  std::string range_name;
  int32_t state = get_state();
  bool has_index = false;
  bool has_qualifier_index = false;
  bool initialized = false;
  bool is_namespace; 

  HT_INFOF("Entering CreateTable-%lld(%s, location=%s, parts=%s) state=%s",
           (Lld)header.id, m_name.c_str(), m_location.c_str(), 
           m_parts.to_string().c_str(), OperationState::get_text(state));

  // If skipping primary table creation, jumpt to create index
  if (state == OperationState::INITIAL && !m_parts.primary())
    state = OperationState::CREATE_INDEX;

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if (m_context->namemap->exists_mapping(m_name, &is_namespace))
      complete_error(Error::NAME_ALREADY_IN_USE, "");

    set_state(OperationState::ASSIGN_ID);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-INITIAL");
    break;

  case OperationState::ASSIGN_ID:
    try {
      Utility::create_table_in_hyperspace(m_context, m_name, m_schema, 
              &m_table);
    }
    catch (Exception &e) {
      if (e.code() == Error::INDUCED_FAILURE)
        throw;
      if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
        HT_ERROR_OUT << e << HT_END;
      complete_error(e);
      return;
    }

    HT_MAYBE_FAIL("create-table-ASSIGN_ID");
    set_state(OperationState::CREATE_INDEX);
    // fall through

  case OperationState::CREATE_INDEX:
    requires_indices(has_index, has_qualifier_index);
    initialized = true;

    if (has_index && m_parts.value_index()) {
      Operation *op = 0;
      try {
        String index_name;
        String index_schema;

        HT_INFOF("  creating index for table %s", m_name.c_str()); 
        Utility::prepare_index(m_context, m_name, m_schema, 
                        false, index_name, index_schema);
        op = new OperationCreateTable(m_context, index_name, index_schema,
                                      TableParts(TableParts::PRIMARY));
        stage_subop(op, m_name + "-create-index");
        entities.push_back(op);
      }
      catch (Exception &e) {
        if (e.code() == Error::INDUCED_FAILURE)
          throw;
        if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
          HT_ERROR_OUT << e << HT_END;
        complete_error(e);
        return;
      }

      HT_MAYBE_FAIL("create-table-CREATE_INDEX-1");
      set_state(OperationState::CREATE_QUALIFIER_INDEX);
      entities.push_back(this);
      m_context->mml_writer->record_state(entities);
      HT_MAYBE_FAIL("create-table-CREATE_INDEX-2");
      break;
    }

    set_state(OperationState::CREATE_QUALIFIER_INDEX);

    // fall through

  case OperationState::CREATE_QUALIFIER_INDEX:

    if (!initialized)
      requires_indices(has_index, has_qualifier_index);

    entities.clear();

    if (!fetch_and_validate_subop(entities))
      break;

    if (has_qualifier_index && m_parts.qualifier_index()) {
      try {
        String index_name;
        String index_schema;
        Operation *op = 0;

        HT_INFOF("  creating qualifier index for table %s", m_name.c_str()); 
        Utility::prepare_index(m_context, m_name, m_schema, 
                        true, index_name, index_schema);
        op = new OperationCreateTable(m_context, index_name, index_schema,
                                      TableParts(TableParts::PRIMARY));
        stage_subop(op, m_name + "-create-qualifier-index");
        entities.push_back(op);
      }
      catch (Exception &e) {
        if (e.code() == Error::INDUCED_FAILURE)
          throw;
        if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
          HT_ERROR_OUT << e << HT_END;
        complete_error(e, entities);
        break;
      }

      HT_MAYBE_FAIL("create-table-CREATE_QUALIFIER_INDEX-1");
      set_state(OperationState::WRITE_METADATA);
      entities.push_back(this);
      record_state(entities);
      HT_MAYBE_FAIL("create-table-CREATE_QUALIFIER_INDEX-2");
      break;
    }
    set_state(OperationState::WRITE_METADATA);
    entities.push_back(this);
    record_state(entities);
    
    // fall through

  case OperationState::WRITE_METADATA:

    entities.clear();

    if (!fetch_and_validate_subop(entities))
      break;

    // If skipping primary table creation, finish here
    if (!m_parts.primary()) {
      complete_ok(entities);
      break;
    }

    Utility::create_table_write_metadata(m_context, &m_table);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-a");

    range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::SERVERS);
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_obstructions.insert(String("OperationMove ") + range_name);
      m_state = OperationState::ASSIGN_LOCATION;
    }
    entities.push_back(this);
    record_state(entities);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-b");
    return;

  case OperationState::ASSIGN_LOCATION:

    if (range_name.empty())
      range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);

    range.start_row = 0;
    range.end_row = Key::END_ROW_MARKER;
    m_context->get_balance_plan_authority()->get_balance_destination(m_table, range, m_location);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_obstructions.insert(String("OperationMove ") + range_name);
      m_state = OperationState::LOAD_RANGE;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-ASSIGN_LOCATION");
    return;

  case OperationState::LOAD_RANGE:
    try {
      range.start_row = 0;
      range.end_row = Key::END_ROW_MARKER;
      Utility::create_table_load_range(m_context, m_location, m_table,
                                       range, false);
    }
    catch (Exception &e) {
      HT_INFOF("%s - %s", Error::get_text(e.code()), e.what());
      poll(0, 0, 5000);
      set_state(OperationState::ASSIGN_LOCATION);
      return;
    }
    HT_MAYBE_FAIL("create-table-LOAD_RANGE-a");
    set_state(OperationState::ACKNOWLEDGE);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-LOAD_RANGE-b");

  case OperationState::ACKNOWLEDGE:

    try {
      range.start_row = 0;
      range.end_row = Key::END_ROW_MARKER;
      Utility::create_table_acknowledge_range(m_context, m_location,
                                              m_table, range);
    }
    catch (Exception &e) {
      if (range_name.empty())
        range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);
      // Destination might be down - go back to the initial state
      HT_INFOF("Problem acknowledging load range %s: %s - %s (dest %s)",
               range_name.c_str(), Error::get_text(e.code()),
               e.what(), m_location.c_str());
      poll(0, 0, 5000);
      // Fetch new destination, if changed, and then try again
      range.start_row = 0;
      range.end_row = Key::END_ROW_MARKER;
      m_context->get_balance_plan_authority()->get_balance_destination(m_table, range, m_location);
      return;
    }
    HT_MAYBE_FAIL("create-table-ACKNOWLEDGE");
    set_state(OperationState::FINALIZE);

  case OperationState::FINALIZE:
    range.start_row = 0;
    range.end_row = Key::END_ROW_MARKER;
    m_context->get_balance_plan_authority()->balance_move_complete(m_table, range);
    {
      String tablefile = m_context->toplevel_dir + "/tables/" + m_table.id;
      m_context->hyperspace->attr_set(tablefile, "x", "", 0);
    }
    HT_MAYBE_FAIL("create-table-FINALIZE");
    complete_ok(m_context->get_balance_plan_authority());
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving CreateTable-%lld(%s, id=%s, generation=%u) state=%s",
           (Lld)header.id, m_name.c_str(), m_table.id ? m_table.id : "",
           (unsigned)m_table.generation,
           OperationState::get_text(get_state()));

}


void OperationCreateTable::display_state(std::ostream &os) {
  os << " name=" << m_name << " ";
  if (m_table.id)
    os << m_table << " ";
  os << " location=" << m_location << " ";
}

#define OPERATION_CREATE_TABLE_VERSION 2

uint16_t OperationCreateTable::encoding_version() const {
  return OPERATION_CREATE_TABLE_VERSION;
}

size_t OperationCreateTable::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name) +
    Serialization::encoded_length_vstr(m_schema) +
    m_table.encoded_length() +
    Serialization::encoded_length_vstr(m_location) +
    m_parts.encoded_length() + 8;
}

void OperationCreateTable::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_vstr(bufp, m_schema);
  m_table.encode(bufp);
  Serialization::encode_vstr(bufp, m_location);
  m_parts.encode(bufp);
  Serialization::encode_i64(bufp, m_subop_hash_code);
}

void OperationCreateTable::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  if (m_decode_version >= 2) {
    m_parts.decode(bufp, remainp);
    m_subop_hash_code = Serialization::decode_i64(bufp, remainp);
  }
}

void OperationCreateTable::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_schema = Serialization::decode_vstr(bufp, remainp);
}

const String OperationCreateTable::name() {
  return "OperationCreateTable";
}

const String OperationCreateTable::label() {
  return String("CreateTable ") + m_name;
}

bool OperationCreateTable::fetch_and_validate_subop(vector<Entity *> &entities) {
  if (m_subop_hash_code) {
    OperationPtr op = m_context->reference_manager->get(m_subop_hash_code);
    op->remove_approval_add(0x01);
    m_subop_hash_code = 0;
    if (op->get_error()) {
      complete_error(op->get_error(), op->get_error_msg(), op.get());
      return false;
    }
    entities.push_back(op.get());
  }
  return true;
}

void OperationCreateTable::stage_subop(Operation *operation,
                                       const std::string dependency_string) {
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
