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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/Serialization.h"

#include <boost/algorithm/string.hpp>

#include "OperationRenameTable.h"
#include "Utility.h"

using namespace Hypertable;
using namespace std;

OperationRenameTable::OperationRenameTable(ContextPtr &context,
                                           const String &old_name,
                                           const String &new_name)
  : Operation(context, MetaLog::EntityType::OPERATION_RENAME_TABLE),
    m_old_name(old_name), m_new_name(new_name) {
  initialize_dependencies();
}

OperationRenameTable::OperationRenameTable(ContextPtr &context,
                                 const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationRenameTable::OperationRenameTable(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_RENAME_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationRenameTable::initialize_dependencies() {
  boost::trim_if(m_old_name, boost::is_any_of("/ "));
  m_old_name = String("/") + m_old_name;
  boost::trim_if(m_new_name, boost::is_any_of("/ "));
  m_new_name = String("/") + m_new_name;
  m_exclusivities.insert(m_old_name);
  m_exclusivities.insert(m_new_name);
  m_dependencies.insert(Dependency::INIT);
}


void OperationRenameTable::execute() {
  int32_t state = get_state();
  String id;

  HT_INFOF("Entering RenameTable-%lld(%s, %s) state=%s",
           (Lld)header.id, m_old_name.c_str(), m_new_name.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    // Verify old name refers to an existing table
    if (!Utility::table_exists(m_context, m_old_name, m_id)) {
      complete_error(Error::TABLE_NOT_FOUND, m_old_name);
      break;
    }

    // Verify new name does not refer to an existing table
    if (Utility::table_exists(m_context, m_new_name, id)) {
      complete_error(Error::NAME_ALREADY_IN_USE, m_new_name);
      break;
    }

    // Verify new name does not refer to an existing namespace
    bool is_namespace;
    if (m_context->namemap->exists_mapping(m_new_name, &is_namespace) && is_namespace) {
      complete_error(Error::NAME_ALREADY_IN_USE, m_new_name);
      break;
    }

    try {
      DynamicBuffer value_buf;
      string filename = m_context->toplevel_dir + "/tables/" + m_id;
      m_context->hyperspace->attr_get(filename, "schema", value_buf);
      m_parts = Utility::get_index_parts((const char *)value_buf.base);
    }
    catch (Exception &e) {
      complete_error(e);
      break;
    }
    
    set_state(OperationState::RENAME_VALUE_INDEX);
    record_state();

  case OperationState::RENAME_VALUE_INDEX:

    if (m_parts.value_index()) {
      string old_index = Filesystem::dirname(m_old_name) 
        + "/^" + Filesystem::basename(m_old_name);
      string new_index = Filesystem::dirname(m_new_name) 
        + "/^" + Filesystem::basename(m_new_name);

      boost::trim_if(old_index, boost::is_any_of("/ "));
      boost::trim_if(new_index, boost::is_any_of("/ "));
      Operation *op = new OperationRenameTable(m_context, old_index, new_index);
      stage_subop(op);
      set_state(OperationState::RENAME_QUALIFIER_INDEX);
      record_state();
      break;
    }

    set_state(OperationState::RENAME_QUALIFIER_INDEX);

  case OperationState::RENAME_QUALIFIER_INDEX:

    if (!validate_subops())
      break;
    
    if (m_parts.qualifier_index()) {
      string old_index = Filesystem::dirname(m_old_name) 
        + "/^^" + Filesystem::basename(m_old_name);
      string new_index = Filesystem::dirname(m_new_name) 
        + "/^^" + Filesystem::basename(m_new_name);

      boost::trim_if(old_index, boost::is_any_of("/ "));
      boost::trim_if(new_index, boost::is_any_of("/ "));
      Operation *op = new OperationRenameTable(m_context, old_index, new_index);
      stage_subop(op);
      set_state(OperationState::STARTED);
      record_state();
      break;
    }

    set_state(OperationState::STARTED);

  case OperationState::STARTED:
    if (!validate_subops())
      break;
    m_context->namemap->rename(m_old_name, m_new_name);
    m_context->monitoring->change_id_mapping(m_id, m_new_name);

    HT_MAYBE_FAIL("rename-table-STARTED");
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving RenameTable-%lld(%s, %s) state=%s",
           (Lld)header.id, m_old_name.c_str(), m_new_name.c_str(),
           OperationState::get_text(get_state()));
}


void OperationRenameTable::display_state(std::ostream &os) {
  os << " old_name=" << m_old_name << " new_name=" << m_new_name 
     << " id=" << m_id << " ";
}

#define OPERATION_RENAME_TABLE_VERSION 2

uint16_t OperationRenameTable::encoding_version() const {
  return OPERATION_RENAME_TABLE_VERSION;
}

size_t OperationRenameTable::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_old_name) + 
    Serialization::encoded_length_vstr(m_new_name) +
    Serialization::encoded_length_vstr(m_id) +
    m_parts.encoded_length();
}

void OperationRenameTable::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_old_name);
  Serialization::encode_vstr(bufp, m_new_name);
  Serialization::encode_vstr(bufp, m_id);
  m_parts.encode(bufp);
}

void OperationRenameTable::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
  if (m_decode_version >= 2)
    m_parts.decode(bufp, remainp);
}

void OperationRenameTable::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_old_name = Serialization::decode_vstr(bufp, remainp);
  m_new_name = Serialization::decode_vstr(bufp, remainp);
}

const String OperationRenameTable::name() {
  return "OperationRenameTable";
}

const String OperationRenameTable::label() {
  return String("RenameTable ") + m_old_name + " -> " + m_new_name;
}

