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

#include "OperationRenameTable.h"
#include "Utility.h"

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/Serialization.h>

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace std;

OperationRenameTable::OperationRenameTable(ContextPtr &context,
                                           const String &from,
                                           const String &to)
  : Operation(context, MetaLog::EntityType::OPERATION_RENAME_TABLE),
    m_params(from, to) {
  m_exclusivities.insert(m_params.from());
  m_exclusivities.insert(m_params.to());
  m_dependencies.insert(Dependency::INIT);
}

OperationRenameTable::OperationRenameTable(ContextPtr &context,
                                 const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationRenameTable::OperationRenameTable(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_RENAME_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_exclusivities.insert(m_params.from());
  m_exclusivities.insert(m_params.to());
  m_dependencies.insert(Dependency::INIT);
}

void OperationRenameTable::execute() {
  int32_t state = get_state();
  String id;

  HT_INFOF("Entering RenameTable-%lld(%s, %s) state=%s",
           (Lld)header.id, m_params.from().c_str(), m_params.to().c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    // Verify old name refers to an existing table
    if (!Utility::table_exists(m_context, m_params.from(), m_id)) {
      complete_error(Error::TABLE_NOT_FOUND, m_params.from());
      break;
    }

    // Verify new name does not refer to an existing table
    if (Utility::table_exists(m_context, m_params.to(), id)) {
      complete_error(Error::NAME_ALREADY_IN_USE, m_params.to());
      break;
    }

    // Verify new name does not refer to an existing namespace
    bool is_namespace;
    if (m_context->namemap->exists_mapping(m_params.to(), &is_namespace) && is_namespace) {
      complete_error(Error::NAME_ALREADY_IN_USE, m_params.to());
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
      string old_index = Filesystem::dirname(m_params.from()) 
        + "/^" + Filesystem::basename(m_params.from());
      string new_index = Filesystem::dirname(m_params.to()) 
        + "/^" + Filesystem::basename(m_params.to());

      boost::trim_if(old_index, boost::is_any_of("/ "));
      boost::trim_if(new_index, boost::is_any_of("/ "));
      stage_subop(make_shared<OperationRenameTable>(m_context, old_index, new_index));
      set_state(OperationState::RENAME_QUALIFIER_INDEX);
      record_state();
      break;
    }

    set_state(OperationState::RENAME_QUALIFIER_INDEX);

  case OperationState::RENAME_QUALIFIER_INDEX:

    if (!validate_subops())
      break;
    
    if (m_parts.qualifier_index()) {
      string old_index = Filesystem::dirname(m_params.from()) 
        + "/^^" + Filesystem::basename(m_params.from());
      string new_index = Filesystem::dirname(m_params.to()) 
        + "/^^" + Filesystem::basename(m_params.to());

      boost::trim_if(old_index, boost::is_any_of("/ "));
      boost::trim_if(new_index, boost::is_any_of("/ "));
      stage_subop(make_shared<OperationRenameTable>(m_context, old_index, new_index));
      set_state(OperationState::STARTED);
      record_state();
      break;
    }

    set_state(OperationState::STARTED);

  case OperationState::STARTED:
    if (!validate_subops())
      break;
    m_context->namemap->rename(m_params.from(), m_params.to());
    m_context->monitoring->change_id_mapping(m_id, m_params.to());

    HT_MAYBE_FAIL("rename-table-STARTED");
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving RenameTable-%lld(%s, %s) state=%s",
           (Lld)header.id, m_params.from().c_str(), m_params.to().c_str(),
           OperationState::get_text(get_state()));
}


void OperationRenameTable::display_state(std::ostream &os) {
  os << " from=" << m_params.from() << " to=" << m_params.to() 
     << " id=" << m_id << " ";
}

uint8_t OperationRenameTable::encoding_version_state() const {
  return 1;
}

size_t OperationRenameTable::encoded_length_state() const {
  return m_params.encoded_length() +
    Serialization::encoded_length_vstr(m_id) +
    m_parts.encoded_length();
}

void OperationRenameTable::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  Serialization::encode_vstr(bufp, m_id);
  m_parts.encode(bufp);
}

void OperationRenameTable::decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
  m_parts.decode(bufp, remainp);
}

void OperationRenameTable::decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  {
    string from = Serialization::decode_vstr(bufp, remainp);
    string to = Serialization::decode_vstr(bufp, remainp);
    m_params = Lib::Master::Request::Parameters::RenameTable(from, to);
  }
  m_id = Serialization::decode_vstr(bufp, remainp);
  if (version >= 2) {
    int8_t parts = (int8_t)Serialization::decode_byte(bufp, remainp);
    m_parts = TableParts(parts);
  }
}

const String OperationRenameTable::name() {
  return "OperationRenameTable";
}

const String OperationRenameTable::label() {
  return String("RenameTable ") + m_params.from() + " -> " + m_params.to();
}

