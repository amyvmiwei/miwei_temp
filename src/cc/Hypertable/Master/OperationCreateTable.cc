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
#include <Hypertable/Lib/LegacyDecoder.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <Hyperspace/Session.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>
#include <Common/Time.h>

#include <boost/algorithm/string.hpp>

#include <chrono>
#include <thread>
#include <vector>

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationCreateTable::OperationCreateTable(ContextPtr &context,
                                           const String &name,
                                           const String &schema,
                                           TableParts parts)
  : Operation(context, MetaLog::EntityType::OPERATION_CREATE_TABLE),
    m_params(name, schema), m_parts(parts) {
}

OperationCreateTable::OperationCreateTable(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_CREATE_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_exclusivities.insert(m_params.name());
  if (!boost::algorithm::starts_with(m_params.name(), "/sys/"))
    m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
}

void OperationCreateTable::execute() {
  RangeSpec range, index_range, qualifier_index_range;
  std::string range_name;
  int32_t state = get_state();
  bool is_namespace; 

  HT_INFOF("Entering CreateTable-%lld(%s, location=%s, parts=%s) state=%s",
           (Lld)header.id, m_params.name().c_str(), m_location.c_str(), 
           m_parts.to_string().c_str(), OperationState::get_text(state));

  // If skipping primary table creation, jumpt to create index
  if (state == OperationState::INITIAL && !m_parts.primary())
    state = OperationState::CREATE_INDEX;

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if (m_context->namemap->exists_mapping(m_params.name(), &is_namespace))
      complete_error(Error::NAME_ALREADY_IN_USE, "");

    // Update table/schema generation number
    {
      SchemaPtr schema( Schema::new_instance(m_params.schema()) );
      int64_t generation = get_ts64();
      schema->update_generation(generation);
      m_schema = schema->render_xml(true);
      m_table.generation = schema->get_generation();
    }

    set_state(OperationState::ASSIGN_ID);
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("create-table-INITIAL");
    break;

  case OperationState::ASSIGN_ID:
    try {
      Utility::create_table_in_hyperspace(m_context, m_params.name(), m_schema, 
              &m_table);
    }
    catch (Exception &e) {
      if (e.code() == Error::INDUCED_FAILURE)
        throw;
      if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
        HT_ERROR_OUT << e << HT_END;
      complete_error(e);
      break;
    }

    // Update m_parts to reflect indices actually found in the schema
    {
      uint8_t parts {};
      TableParts schema_parts = Utility::get_index_parts(m_schema);
      if (m_parts.primary())
        parts |= TableParts::PRIMARY;
      if (m_parts.value_index() && schema_parts.value_index())
        parts |= TableParts::VALUE_INDEX;
      if (m_parts.qualifier_index() && schema_parts.qualifier_index())
        parts |= TableParts::QUALIFIER_INDEX;
      m_parts = TableParts(parts);
    }

    HT_MAYBE_FAIL("create-table-ASSIGN_ID");
    set_state(OperationState::CREATE_INDEX);
    // fall through

  case OperationState::CREATE_INDEX:

    if (m_parts.value_index()) {
      try {
        String index_name;
        String index_schema;

        HT_INFOF("  creating index for table %s", m_params.name().c_str()); 
        Utility::prepare_index(m_context, m_params.name(), m_params.schema(),
                               false, index_name, index_schema);
        stage_subop(make_shared<OperationCreateTable>(m_context, index_name, index_schema,
                                                      TableParts(TableParts::PRIMARY)));
      }
      catch (Exception &e) {
        if (e.code() == Error::INDUCED_FAILURE)
          throw;
        if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
          HT_ERROR_OUT << e << HT_END;
        complete_error(e);
        break;
      }

      HT_MAYBE_FAIL("create-table-CREATE_INDEX-1");
      set_state(OperationState::CREATE_QUALIFIER_INDEX);
      record_state();
      HT_MAYBE_FAIL("create-table-CREATE_INDEX-2");
      break;
    }

    set_state(OperationState::CREATE_QUALIFIER_INDEX);

    // fall through

  case OperationState::CREATE_QUALIFIER_INDEX:

    if (!validate_subops())
      break;

    if (m_parts.qualifier_index()) {
      try {
        String index_name;
        String index_schema;

        HT_INFOF("  creating qualifier index for table %s", m_params.name().c_str()); 
        Utility::prepare_index(m_context, m_params.name(), m_params.schema(),
                        true, index_name, index_schema);
        stage_subop(make_shared<OperationCreateTable>(m_context, index_name, index_schema,
                                                      TableParts(TableParts::PRIMARY)));
      }
      catch (Exception &e) {
        if (e.code() == Error::INDUCED_FAILURE)
          throw;
        if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
          HT_ERROR_OUT << e << HT_END;
        complete_error(e);
        break;
      }

      HT_MAYBE_FAIL("create-table-CREATE_QUALIFIER_INDEX-1");
      set_state(OperationState::WRITE_METADATA);
      record_state();
      HT_MAYBE_FAIL("create-table-CREATE_QUALIFIER_INDEX-2");
      break;
    }
    set_state(OperationState::WRITE_METADATA);
    record_state();
    
    // fall through

  case OperationState::WRITE_METADATA:

    if (!validate_subops())
      break;

    // If skipping primary table creation, finish here
    if (!m_parts.primary()) {
      complete_ok();
      break;
    }

    Utility::create_table_write_metadata(m_context, &m_table);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-a");

    range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);
    {
      lock_guard<mutex> lock(m_mutex);
      m_dependencies.insert(Dependency::SERVERS);
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_obstructions.insert(String("OperationMove ") + range_name);
      m_state = OperationState::ASSIGN_LOCATION;
    }
    record_state();
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-b");
    break;

  case OperationState::ASSIGN_LOCATION:

    if (range_name.empty())
      range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);

    range.start_row = 0;
    range.end_row = Key::END_ROW_MARKER;
    m_context->get_balance_plan_authority()->get_balance_destination(m_table, range, m_location);
    {
      lock_guard<mutex> lock(m_mutex);
      m_dependencies.erase(Dependency::SERVERS);
      m_state = OperationState::LOAD_RANGE;
    }
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("create-table-ASSIGN_LOCATION");
    break;

  case OperationState::LOAD_RANGE:
    try {
      range.start_row = 0;
      range.end_row = Key::END_ROW_MARKER;
      Utility::create_table_load_range(m_context, m_location, m_table,
                                       range, false);
    }
    catch (Exception &e) {
      HT_INFOF("%s - %s", Error::get_text(e.code()), e.what());
      this_thread::sleep_for(chrono::milliseconds(5000));
      set_state(OperationState::ASSIGN_LOCATION);
      break;
    }
    HT_MAYBE_FAIL("create-table-LOAD_RANGE-a");
    set_state(OperationState::ACKNOWLEDGE);
    m_context->mml_writer->record_state(shared_from_this());
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
      this_thread::sleep_for(chrono::milliseconds(5000));
      // Fetch new destination, if changed, and then try again
      range.start_row = 0;
      range.end_row = Key::END_ROW_MARKER;
      m_context->get_balance_plan_authority()->get_balance_destination(m_table, range, m_location);
      break;
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
      HT_MAYBE_FAIL("create-table-FINALIZE");
      MetaLog::EntityPtr bpa_entity;
      m_context->get_balance_plan_authority(bpa_entity);
      complete_ok(bpa_entity);
    }
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving CreateTable-%lld(%s, id=%s, generation=%u) state=%s",
           (Lld)header.id, m_params.name().c_str(), m_table.id ? m_table.id : "",
           (unsigned)m_table.generation,
           OperationState::get_text(get_state()));

}


void OperationCreateTable::display_state(std::ostream &os) {
  os << " name=" << m_params.name() << " ";
  if (m_table.id)
    os << m_table << " ";
  os << " location=" << m_location << " ";
}

uint8_t OperationCreateTable::encoding_version_state() const {
  return 1;
}

size_t OperationCreateTable::encoded_length_state() const {
  return m_params.encoded_length() + m_table.encoded_length() +
    Serialization::encoded_length_vstr(m_schema) +
    Serialization::encoded_length_vstr(m_location) +
    m_parts.encoded_length();
}

void OperationCreateTable::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  m_table.encode(bufp);
  Serialization::encode_vstr(bufp, m_schema);
  Serialization::encode_vstr(bufp, m_location);
  m_parts.encode(bufp);
}

void OperationCreateTable::decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_schema = Serialization::decode_vstr(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_parts.decode(bufp, remainp);
}

void OperationCreateTable::decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  {
    string name = Serialization::decode_vstr(bufp, remainp);
    m_schema = Serialization::decode_vstr(bufp, remainp);
    m_params = Lib::Master::Request::Parameters::CreateTable(name, m_schema);
  }
  legacy_decode(bufp, remainp, &m_table);
  m_location = Serialization::decode_vstr(bufp, remainp);
  if (version >= 2) {
    int8_t parts = (int8_t)Serialization::decode_byte(bufp, remainp);
    m_parts = TableParts(parts);
  }
}

const String OperationCreateTable::name() {
  return "OperationCreateTable";
}

const String OperationCreateTable::label() {
  return String("CreateTable ") + m_params.name();
}
