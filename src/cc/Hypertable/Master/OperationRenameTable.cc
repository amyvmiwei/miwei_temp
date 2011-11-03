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
#include "Common/Serialization.h"

#include <boost/algorithm/string.hpp>

#include "OperationRenameTable.h"
#include "Utility.h"

using namespace Hypertable;


OperationRenameTable::OperationRenameTable(ContextPtr &context, const String &old_name, const String &new_name)
  : Operation(context, MetaLog::EntityType::OPERATION_RENAME_TABLE), m_old_name(old_name), m_new_name(new_name) {
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
  String old_index;
  String new_index;

  HT_INFOF("Entering RenameTable-%lld(%s, %s) state=%s",
           (Lld)header.id, m_old_name.c_str(), m_new_name.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    if (!Utility::table_exists(m_context, m_old_name, m_id)) {
      complete_error(Error::TABLE_NOT_FOUND, m_old_name);
      return;
    }

    // check if an index table exists; if yes then create a sub-operation
    old_index = Filesystem::dirname(m_old_name) 
                   + "/^" + Filesystem::basename(m_old_name);
    boost::trim_if(old_index, boost::is_any_of("/ "));
    if (Utility::table_exists(m_context, old_index, id)) {
      new_index = Filesystem::dirname(m_new_name) 
                     + "/^" + Filesystem::basename(m_new_name);
      boost::trim_if(new_index, boost::is_any_of("/ "));

      Operation *op = new OperationRenameTable(m_context, old_index, new_index);
      op->add_obstruction(old_index + "-rename-index");

      ScopedLock lock(m_mutex);
      add_dependency(old_index + "-rename-index");
      m_sub_ops.push_back(op);
    }

    // and now the same for a qualified index
    old_index = Filesystem::dirname(m_old_name) 
                   + "/^^" + Filesystem::basename(m_old_name);
    boost::trim_if(old_index, boost::is_any_of("/ "));
    if (Utility::table_exists(m_context, old_index, id)) {
      new_index = Filesystem::dirname(m_new_name) 
                     + "/^^" + Filesystem::basename(m_new_name);
      boost::trim_if(new_index, boost::is_any_of("/ "));

      Operation *op = new OperationRenameTable(m_context, old_index, new_index);
      op->add_obstruction(old_index + "-rename-qualified-index");

      ScopedLock lock(m_mutex);
      add_dependency(old_index + "-rename-qualified-index");
      m_sub_ops.push_back(op);
    }

    set_state(OperationState::STARTED);
    m_context->mml_writer->record_state(this);
    break;

  case OperationState::STARTED:
    m_context->namemap->rename(m_old_name, m_new_name);
    m_context->monitoring->change_id_mapping(m_id, m_new_name);

    HT_MAYBE_FAIL("rename-table-STARTED");
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving RenameTable-%lld(%s, %s)",
           (Lld)header.id, m_old_name.c_str(), m_new_name.c_str());
}


void OperationRenameTable::display_state(std::ostream &os) {
  os << " old_name=" << m_old_name << " new_name=" << m_new_name 
     << " id=" << m_id << " ";
}

size_t OperationRenameTable::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_old_name) + 
    Serialization::encoded_length_vstr(m_new_name) +
    Serialization::encoded_length_vstr(m_id);
}

void OperationRenameTable::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_old_name);
  Serialization::encode_vstr(bufp, m_new_name);
  Serialization::encode_vstr(bufp, m_id);
}

void OperationRenameTable::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
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

