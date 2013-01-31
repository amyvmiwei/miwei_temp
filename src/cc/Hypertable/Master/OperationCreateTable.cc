/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Key.h"

#include "BalancePlanAuthority.h"
#include "OperationCreateTable.h"
#include "Utility.h"

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hyperspace;

OperationCreateTable::OperationCreateTable(ContextPtr &context, const String &name, const String &schema)
  : Operation(context, MetaLog::EntityType::OPERATION_CREATE_TABLE), m_name(name), m_schema(schema) {
  initialize_dependencies();
}

OperationCreateTable::OperationCreateTable(ContextPtr &context,
                                           const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
  if (m_table.id && *m_table.id != 0)
    m_range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);
}

OperationCreateTable::OperationCreateTable(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_CREATE_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationCreateTable::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
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
  bool is_namespace; 
  RangeSpec range, index_range, qualifier_index_range;
  int32_t state = get_state();
  bool has_index = false;
  bool has_qualifier_index = false;
  bool initialized = false;

  HT_INFOF("Entering CreateTable-%lld(%s, location=%s) state=%s",
           (Lld)header.id, m_name.c_str(), m_location.c_str(), 
           OperationState::get_text(state));

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

    if (has_index) {
      try {
        String index_name;
        String index_schema;
        Operation *op = 0;

        HT_INFOF("  creating index for table %s", m_name.c_str()); 
        Utility::prepare_index(m_context, m_name, m_schema, 
                        false, index_name, index_schema);
        op = new OperationCreateTable(m_context, 
                index_name, index_schema);
        op->add_obstruction(m_name + "-create-index");

        ScopedLock lock(m_mutex);
        add_dependency(m_name + "-create-index");
        m_sub_ops.push_back(op);
      }
      catch (Exception &e) {
        if (e.code() == Error::INDUCED_FAILURE)
          throw;
        if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
          HT_ERROR_OUT << e << HT_END;
        complete_error(e);
        return;
      }

      HT_MAYBE_FAIL("create-table-CREATE_INDEX");
      set_state(OperationState::CREATE_QUALIFIER_INDEX);
      return;
    }

    set_state(OperationState::CREATE_QUALIFIER_INDEX);

    // fall through

  case OperationState::CREATE_QUALIFIER_INDEX:
    if (!initialized)
      requires_indices(has_index, has_qualifier_index);

    if (has_qualifier_index) {
      try {
        String index_name;
        String index_schema;
        Operation *op = 0;

        HT_INFOF("  creating qualifier index for table %s", m_name.c_str()); 
        Utility::prepare_index(m_context, m_name, m_schema, 
                        true, index_name, index_schema);
        op = new OperationCreateTable(m_context, 
                index_name, index_schema);
        op->add_obstruction(m_name + "-create-qualifier-index");

        ScopedLock lock(m_mutex);
        add_dependency(m_name + "-create-qualifier-index");
        m_sub_ops.push_back(op);
      }
      catch (Exception &e) {
        if (e.code() == Error::INDUCED_FAILURE)
          throw;
        if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
          HT_ERROR_OUT << e << HT_END;
        complete_error(e);
        return;
      }

      HT_MAYBE_FAIL("create-table-CREATE_QUALIFIER_INDEX");
      set_state(OperationState::WRITE_METADATA);
      return;
    }
    set_state(OperationState::WRITE_METADATA);
    
    // fall through

  case OperationState::WRITE_METADATA:
    Utility::create_table_write_metadata(m_context, &m_table);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-a");

    m_range_name = format("%s[..%s]", 
            m_table.id, Key::END_ROW_MARKER);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::SERVERS);
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_obstructions.insert(String("OperationMove ") + m_range_name);
      m_state = OperationState::ASSIGN_LOCATION;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-b");
    return;

  case OperationState::ASSIGN_LOCATION:
    range.start_row = 0;
    range.end_row = Key::END_ROW_MARKER;
    m_context->get_balance_plan_authority()->get_balance_destination(m_table, range, m_location);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_obstructions.insert(String("OperationMove ") + m_range_name);
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
      if (e.code() == Error::RANGESERVER_TABLE_DROPPED) {
        complete_error(e);
        return;
      }
      HT_INFO_OUT << e << HT_END;
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
      // Destination might be down - go back to the initial state
      HT_INFOF("Problem acknowledging load range %s: %s - %s (dest %s)",
               m_range_name.c_str(), Error::get_text(e.code()),
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

      uint64_t handle = 0;
      HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, 
              &handle);
      handle = m_context->hyperspace->open(tablefile, 
              OPEN_FLAG_READ|OPEN_FLAG_WRITE);
      m_context->hyperspace->attr_set(handle, "x", "", 0);
    }
    HT_MAYBE_FAIL("create-table-FINALIZE");
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving CreateTable-%lld(%s, id=%s, generation=%u)",
           (Lld)header.id, m_name.c_str(), 
           m_table.id, (unsigned)m_table.generation);
}


void OperationCreateTable::display_state(std::ostream &os) {
  os << " name=" << m_name << " ";
  if (m_table.id)
    os << m_table << " ";
  os << " location=" << m_location << " ";
}

size_t OperationCreateTable::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name) +
    Serialization::encoded_length_vstr(m_schema) +
    m_table.encoded_length() +
    Serialization::encoded_length_vstr(m_location);
}

void OperationCreateTable::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_vstr(bufp, m_schema);
  m_table.encode(bufp);
  Serialization::encode_vstr(bufp, m_location);
}

void OperationCreateTable::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
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

