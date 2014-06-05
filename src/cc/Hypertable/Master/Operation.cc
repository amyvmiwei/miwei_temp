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
 * Definitions for Operation.
 * This file contains definitions for Operation, an abstract base class that
 * that represents a master operation, and from which specific/concrete
 * operation classes are derived.
 */

#include <Common/Compat.h>
#include "Operation.h"

#include <Hypertable/Master/ReferenceManager.h>

#include <Common/Serialization.h>

#include <algorithm>
#include <ctime>
#include <iterator>
#include <sstream>
#include <unordered_map>

using namespace Hypertable;
using namespace std;

const char *Dependency::INIT = "INIT";
const char *Dependency::SERVERS = "SERVERS";
const char *Dependency::ROOT = "ROOT";
const char *Dependency::METADATA = "METADATA";
const char *Dependency::SYSTEM = "SYSTEM";
const char *Dependency::USER = "USER";
const char *Dependency::RECOVER_SERVER = "RECOVER_SERVER";
const char *Dependency::RECOVERY_BLOCKER= "RECOVERY_BLOCKER";
const char *Dependency::RECOVERY = "RECOVERY";

namespace Hypertable {
  namespace OperationState {
    const char *get_text(int32_t state);
  }
}

Operation::Operation(ContextPtr &context, int32_t type)
  : MetaLog::Entity(type), m_context(context) {
  int32_t timeout = m_context->props->get_i32("Hypertable.Request.Timeout");
  m_expiration_time.sec = time(0) + timeout/1000;
  m_expiration_time.nsec = (timeout%1000) * 1000000LL;
  m_hash_code = (int64_t)header.id;
}

Operation::Operation(ContextPtr &context, EventPtr &event, int32_t type)
  : MetaLog::Entity(type), m_context(context), m_event(event) {
  m_expiration_time.sec = time(0) + m_event->header.timeout_ms/1000;
  m_expiration_time.nsec = (m_event->header.timeout_ms%1000) * 1000000LL;
  m_hash_code = (int64_t)header.id;
}

Operation::Operation(ContextPtr &context, const MetaLog::EntityHeader &header_)
  : MetaLog::Entity(header_), m_context(context) {
  m_hash_code = (int64_t)header.id;
}

void Operation::display(std::ostream &os) {

  os << " state=" << OperationState::get_text(m_state);
  os << " remove_approvals=" << m_remove_approvals;
  os << " remove_approval_mask=" << m_remove_approval_mask;
  if (m_state == OperationState::COMPLETE) {
    os << " [" << Error::get_text(m_error) << "] ";
    if (m_error != Error::OK)
      os << m_error_msg << " ";
  }
  else {
    bool first = true;
    display_state(os);

    os << " exclusivities=";
    for (DependencySet::iterator iter = m_exclusivities.begin(); iter != m_exclusivities.end(); ++iter) {
      if (first) {
        os << *iter;
        first = false;
      }
      else
        os << "," << *iter;
    }

    first = true;
    os << " dependencies=";
    for (DependencySet::iterator iter = m_dependencies.begin(); iter != m_dependencies.end(); ++iter) {
      if (first) {
        os << *iter;
        first = false;
      }
      else
        os << "," << *iter;
    }

    first = true;
    os << " obstructions=";
    for (DependencySet::iterator iter = m_obstructions.begin(); iter != m_obstructions.end(); ++iter) {
      if (first) {
        os << *iter;
        first = false;
      }
      else
        os << "," << *iter;
    }
    os << " ";
  }
}

size_t Operation::encoded_length() const {
  size_t length = 22;

  if (m_state == OperationState::COMPLETE) {
    length += 8 + encoded_result_length();
  }
  else {
    length += encoded_state_length();
    length += Serialization::encoded_length_vi32(m_exclusivities.size()) +
      Serialization::encoded_length_vi32(m_dependencies.size()) +
      Serialization::encoded_length_vi32(m_obstructions.size());
    for (DependencySet::iterator iter = m_exclusivities.begin(); iter != m_exclusivities.end(); ++iter)
      length += Serialization::encoded_length_vstr(*iter);
    for (DependencySet::iterator iter = m_dependencies.begin(); iter != m_dependencies.end(); ++iter)
      length += Serialization::encoded_length_vstr(*iter);
    for (DependencySet::iterator iter = m_obstructions.begin(); iter != m_obstructions.end(); ++iter)
      length += Serialization::encoded_length_vstr(*iter);
    // permanent obstructions
    length += Serialization::encoded_length_vi32(m_obstructions_permanent.size());
    for (auto & str : m_obstructions_permanent)
      length += Serialization::encoded_length_vstr(str);
    // sub operations
    length += Serialization::encoded_length_vi32(m_sub_ops.size());
    for (int64_t id : m_sub_ops)
      length += Serialization::encoded_length_vi64(id);
  }
  return length;
}

void Operation::encode(uint8_t **bufp) const {

  Serialization::encode_i16(bufp, encoding_version());
  Serialization::encode_i32(bufp, m_state);
  Serialization::encode_i64(bufp, m_expiration_time.sec);
  Serialization::encode_i32(bufp, m_expiration_time.nsec);
  Serialization::encode_i16(bufp, m_remove_approvals);
  Serialization::encode_i16(bufp, m_remove_approval_mask);
  if (m_state == OperationState::COMPLETE) {
    Serialization::encode_i64(bufp, m_hash_code);
    encode_result(bufp);
  }
  else {
    encode_state(bufp);
    Serialization::encode_vi32(bufp, m_exclusivities.size());
    for (DependencySet::iterator iter = m_exclusivities.begin(); iter != m_exclusivities.end(); ++iter)
      Serialization::encode_vstr(bufp, *iter);
    Serialization::encode_vi32(bufp, m_dependencies.size());
    for (DependencySet::iterator iter = m_dependencies.begin(); iter != m_dependencies.end(); ++iter)
      Serialization::encode_vstr(bufp, *iter);
    Serialization::encode_vi32(bufp, m_obstructions.size());
    for (DependencySet::iterator iter = m_obstructions.begin(); iter != m_obstructions.end(); ++iter)
      Serialization::encode_vstr(bufp, *iter);
    Serialization::encode_vi32(bufp, m_obstructions_permanent.size());
    for (auto & str : m_obstructions_permanent)
      Serialization::encode_vstr(bufp, str);
    Serialization::encode_vi32(bufp, m_sub_ops.size());
    for (int64_t id : m_sub_ops)
      Serialization::encode_vi64(bufp, id);
  }
}

void Operation::decode(const uint8_t **bufp, size_t *remainp,
                       uint16_t definition_version) {
  String str;
  size_t length;

  if (definition_version >= 2)
    m_decode_version = Serialization::decode_i16(bufp, remainp);
  else
    m_decode_version = 0;
  m_state = Serialization::decode_i32(bufp, remainp);
  m_expiration_time.sec = Serialization::decode_i64(bufp, remainp);
  m_expiration_time.nsec = Serialization::decode_i32(bufp, remainp);
  if (m_original_type == 0 || (m_original_type & 0xF0000L) > 0x20000L) {
    m_remove_approvals = Serialization::decode_i16(bufp, remainp);
    m_remove_approval_mask = Serialization::decode_i16(bufp, remainp);
  }
  if (m_state == OperationState::COMPLETE) {
    m_hash_code = Serialization::decode_i64(bufp, remainp);
    decode_result(bufp, remainp);
  }
  else {
    decode_state(bufp, remainp);

    m_exclusivities.clear();
    length = Serialization::decode_vi32(bufp, remainp);
    for (size_t i=0; i<length; i++) {
      str = Serialization::decode_vstr(bufp, remainp);
      m_exclusivities.insert(str);
    }

    m_dependencies.clear();
    length = Serialization::decode_vi32(bufp, remainp);
    for (size_t i=0; i<length; i++) {
      str = Serialization::decode_vstr(bufp, remainp);
      m_dependencies.insert(str);
    }

    m_obstructions.clear();
    length = Serialization::decode_vi32(bufp, remainp);
    for (size_t i=0; i<length; i++) {
      str = Serialization::decode_vstr(bufp, remainp);
      m_obstructions.insert(str);
    }

    if (definition_version >= 3) {
      // permanent obstructions
      m_obstructions_permanent.clear();
      length = Serialization::decode_vi32(bufp, remainp);
      for (size_t i=0; i<length; i++) {
        str = Serialization::decode_vstr(bufp, remainp);
        m_obstructions_permanent.insert(str);
      }
      // sub operations
      m_sub_ops.clear();
      length = Serialization::decode_vi32(bufp, remainp);
      for (size_t i=0; i<length; i++)
        m_sub_ops.push_back( Serialization::decode_vi64(bufp, remainp) );
    }
  }
}

size_t Operation::encoded_result_length() const {
  if (m_error == Error::OK)
    return 4;
  return 4 + Serialization::encoded_length_vstr(m_error_msg);
}

void Operation::encode_result(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_error);
  if (m_error != Error::OK)
    Serialization::encode_vstr(bufp, m_error_msg);
}

void Operation::decode_result(const uint8_t **bufp, size_t *remainp) {
  m_error = Serialization::decode_i32(bufp, remainp);
  if (m_error != Error::OK)
    m_error_msg = Serialization::decode_vstr(bufp, remainp);
}

bool Operation::removal_approved() {
  ScopedLock lock(m_mutex);
  return m_remove_approval_mask && m_remove_approvals == m_remove_approval_mask;
}

void Operation::record_state(std::vector<MetaLog::Entity *> &additional) {
  std::vector<MetaLog::Entity *> entities;
  entities.reserve(1 + additional.size() + m_sub_ops.size());
  // Add this
  if (removal_approved())
    mark_for_removal();
  entities.push_back(this);
  // Add additional entities
  for (auto entity : additional) {
    Operation *op = dynamic_cast<Operation *>(entity);
    if (op && op->removal_approved())
      op->mark_for_removal();
    entities.push_back(entity);
  }
  // Add sub operations
  std::vector<int64_t> new_sub_ops;
  for (int64_t id : m_sub_ops) {
    OperationPtr op = m_context->reference_manager->get(id);
    if (op->removal_approved())
      op->mark_for_removal();
    else
      new_sub_ops.push_back(op->id());
    entities.push_back(op.get());
  }
  m_context->mml_writer->record_state(entities);
  for (auto entity : entities) {
    Operation *op = dynamic_cast<Operation *>(entity);
    if (op && op->marked_for_removal())
      m_context->reference_manager->remove(op);
  }
  m_sub_ops.swap(new_sub_ops);
}

void Operation::complete_error(int error, const String &msg,
                               std::vector<MetaLog::Entity *> &additional) {
  {
    ScopedLock lock(m_mutex);
    m_state = OperationState::COMPLETE;
    m_error = error;
    m_error_msg = msg;
    m_dependencies.clear();
    m_obstructions.clear();
    m_exclusivities.clear();
    for (int64_t id : m_sub_ops) {
      OperationPtr op = m_context->reference_manager->get(id);
      op->remove_approval_add(op->get_remove_approval_mask());
    }
  }

  std::stringstream sout;
  sout << "Operation failed (" << *this << ") " << Error::get_text(error) << " - " << msg;
  HT_INFOF("%s", sout.str().c_str());

  if (m_ephemeral) {
    HT_ASSERT(additional.empty());
    return;
  }

  record_state(additional);
}

void Operation::complete_error(int error, const String &msg, MetaLog::Entity *additional) {
  std::vector<MetaLog::Entity *> entities;
  if (additional)
    entities.push_back(additional);
  complete_error(error, msg, entities);
}


void Operation::complete_ok(std::vector<MetaLog::Entity *> &additional) {
  {
    ScopedLock lock(m_mutex);
    m_state = OperationState::COMPLETE;
    m_error = Error::OK;
    m_dependencies.clear();
    m_obstructions.clear();
    m_exclusivities.clear();
    if (m_ephemeral) {
      HT_ASSERT(additional.empty());
      return;
    }
  }
  record_state(additional);
}

void Operation::complete_ok(MetaLog::Entity *additional) {
  std::vector<MetaLog::Entity *> entities;
  if (additional)
    entities.push_back(additional);
  complete_ok(entities);
}

void Operation::exclusivities(DependencySet &exclusivities) {
  ScopedLock lock(m_mutex);
  exclusivities = m_exclusivities;
}

void Operation::dependencies(DependencySet &dependencies) {
  ScopedLock lock(m_mutex);
  dependencies = m_dependencies;
}

void Operation::obstructions(DependencySet &obstructions) {
  ScopedLock lock(m_mutex);
  obstructions = m_obstructions;
  obstructions.insert(m_obstructions_permanent.begin(), m_obstructions_permanent.end());
}

void Operation::fetch_sub_operations(std::vector<Operation *> &sub_ops) {
  ScopedLock lock(m_mutex);
  for (int64_t id : m_sub_ops) {
    OperationPtr op = m_context->reference_manager->get(id);
    sub_ops.push_back(op.get());
  }
}


void Operation::pre_run() {
  ScopedLock lock(m_mutex);
  m_unblock_on_exit=false;
}


void Operation::post_run() {
  ScopedLock lock(m_mutex);
  if (m_unblock_on_exit)
    m_blocked = false;
}


bool Operation::block() {
  ScopedLock lock(m_mutex);
  if (!m_blocked) {
    m_blocked = true;
    return true;
  }
  return false;
}

bool Operation::unblock() {
  ScopedLock lock(m_mutex);
  bool blocked_on_entry = m_blocked;
  m_unblock_on_exit = true;
  m_blocked = false;
  return blocked_on_entry;
}

bool Operation::validate_subops() {
  vector<MetaLog::Entity *> entities;

  for (int64_t id : m_sub_ops) {
    OperationPtr op = m_context->reference_manager->get(id);
    if (op->get_error()) {
      complete_error(op->get_error(), op->get_error_msg());
      return false;
    }
    op->remove_approval_add(op->get_remove_approval_mask());
    string dependency_string =
      format("%s subop %s %lld", name().c_str(), op->name().c_str(),
             (Lld)op->hash_code());
    ScopedLock lock(m_mutex);
    m_dependencies.erase(dependency_string);
    entities.push_back(op.get());
  }

  m_sub_ops.clear();
  record_state(entities);

  return true;
}

void Operation::stage_subop(Operation *operation) {
  string dependency_string =
    format("%s subop %s %lld", name().c_str(), operation->name().c_str(),
           (Lld)operation->hash_code());
  operation->add_obstruction_permanent(dependency_string);
  operation->set_remove_approval_mask(0x01);
  m_context->reference_manager->add(operation);
  {
    ScopedLock lock(m_mutex);
    add_dependency(dependency_string);
    m_sub_ops.push_back(operation->id());
  }
}



namespace {
  struct StateInfo {
    int32_t code;
    const char *text;
  };

  StateInfo state_info[] = {
    { Hypertable::OperationState::INITIAL, "INITIAL" },
    { Hypertable::OperationState::COMPLETE, "COMPLETE" },
    { Hypertable::OperationState::UNUSED, "UNUSED" },
    { Hypertable::OperationState::STARTED, "STARTED" },
    { Hypertable::OperationState::ASSIGN_ID, "ASSIGN_ID" },
    { Hypertable::OperationState::ASSIGN_LOCATION,"ASSIGN_LOCATION" },
    { Hypertable::OperationState::ASSIGN_METADATA_RANGES, "ASSIGN_METADATA_RANGES" },
    { Hypertable::OperationState::LOAD_RANGE, "LOAD_RANGE" },
    { Hypertable::OperationState::LOAD_ROOT_METADATA_RANGE, "LOAD_ROOT_METADATA_RANGE" },
    { Hypertable::OperationState::LOAD_SECOND_METADATA_RANGE, "LOAD_SECOND_METADATA_RANGE" },
    { Hypertable::OperationState::WRITE_METADATA, "WRITE_METADATA" },
    { Hypertable::OperationState::CREATE_RS_METRICS, "CREATE_RS_METRICS" },
    { Hypertable::OperationState::VALIDATE_SCHEMA, "VALIDATE_SCHEMA" },
    { Hypertable::OperationState::SCAN_METADATA, "SCAN_METADATA" },
    { Hypertable::OperationState::ISSUE_REQUESTS, "ISSUE_REQUESTS" },
    { Hypertable::OperationState::UPDATE_HYPERSPACE, "UPDATE_HYPERSPACE" },
    { Hypertable::OperationState::ACKNOWLEDGE, "ACKNOWLEDGE" },
    { Hypertable::OperationState::FINALIZE, "FINALIZE" },
    { Hypertable::OperationState::CREATE_INDEX, "CREATE_INDEX" },
    { Hypertable::OperationState::CREATE_QUALIFIER_INDEX, "CREATE_QUALIFIER_INDEX" },
    { Hypertable::OperationState::PREPARE, "PREPARE" },
    { Hypertable::OperationState::COMMIT, "COMMIT" },
    { Hypertable::OperationState::PHANTOM_LOAD, "PHANTOM_LOAD" },
    { Hypertable::OperationState::REPLAY_FRAGMENTS, "REPLAY_FRAGMENTS" },
    { Hypertable::OperationState::CREATE_INDICES, "CREATE_INDICES" },
    { Hypertable::OperationState::DROP_INDICES, "DROP_INDICES" },
    { Hypertable::OperationState::SUSPEND_TABLE_MAINTENANCE, "SUSPEND_TABLE_MAINTENANCE" },
    { Hypertable::OperationState::RESUME_TABLE_MAINTENANCE, "RESUME_TABLE_MAINTENANCE" },
    { Hypertable::OperationState::DROP_VALUE_INDEX, "DROP_VALUE_INDEX" },
    { Hypertable::OperationState::DROP_QUALIFIER_INDEX, "DROP_QUALIFIER_INDEX" },
    { 0, 0 }
  };

  typedef std::unordered_map<int32_t, const char *> TextMap;

  TextMap &build_text_map() {
    TextMap *map = new TextMap();
    for (int i=0; state_info[i].text != 0; i++)
      (*map)[state_info[i].code] = state_info[i].text;
    return *map;
  }

  TextMap &text_map = build_text_map();

} // local namespace

namespace Hypertable {
  namespace OperationState {
    const char *get_text(int32_t state) {
      const char *text = text_map[state];
      if (text == 0)
        return "STATE NOT REGISTERED";
      return text;
    }
  }
}
