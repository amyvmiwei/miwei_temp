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

#include "Common/Compat.h"
#include "Common/HashMap.h"
#include "Common/Serialization.h"

#include <ctime>
#include <sstream>

#include "Operation.h"
#include "ReferenceManager.h"

using namespace Hypertable;

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
  : MetaLog::Entity(type), m_context(context), m_state(OperationState::INITIAL),
    m_error(0), m_remove_approvals(0), m_original_type(0), m_decode_version(0),
    m_unblock_on_exit(false), m_blocked(false), m_ephemeral(false) {
  int32_t timeout = m_context->props->get_i32("Hypertable.Request.Timeout");
  m_expiration_time.sec = time(0) + timeout/1000;
  m_expiration_time.nsec = (timeout%1000) * 1000000LL;
  m_hash_code = (int64_t)header.id;
}

Operation::Operation(ContextPtr &context, EventPtr &event, int32_t type)
  : MetaLog::Entity(type), m_context(context), m_event(event),
    m_state(OperationState::INITIAL), m_error(0), m_remove_approvals(0),
    m_original_type(0),  m_decode_version(0), m_unblock_on_exit(false),
    m_blocked(false), m_ephemeral(false) {
  m_expiration_time.sec = time(0) + m_event->header.timeout_ms/1000;
  m_expiration_time.nsec = (m_event->header.timeout_ms%1000) * 1000000LL;
  m_hash_code = (int64_t)header.id;
}

Operation::Operation(ContextPtr &context, const MetaLog::EntityHeader &header_)
  : MetaLog::Entity(header_), m_context(context),
    m_state(OperationState::INITIAL), m_error(0), m_remove_approvals(0),
    m_original_type(0), m_decode_version(0), m_unblock_on_exit(false),
    m_blocked(false), m_ephemeral(false) {
  m_hash_code = (int64_t)header.id;
}

void Operation::display(std::ostream &os) {

  os << " state=" << OperationState::get_text(m_state);
  os << " remove_approvals=" << m_remove_approvals;
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
  }
  return length;
}

void Operation::encode(uint8_t **bufp) const {

  Serialization::encode_i16(bufp, encoding_version());
  Serialization::encode_i32(bufp, m_state);
  Serialization::encode_i64(bufp, m_expiration_time.sec);
  Serialization::encode_i32(bufp, m_expiration_time.nsec);
  Serialization::encode_i32(bufp, m_remove_approvals);
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
  if (m_original_type == 0 || (m_original_type & 0xF0000L) > 0x20000L)
    m_remove_approvals = Serialization::decode_i32(bufp, remainp);
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

bool Operation::remove_if_ready() {
  ScopedLock lock(m_remove_approval_mutex);
  HT_ASSERT(remove_explicitly());
  if (m_remove_approvals == remove_approval_mask()) {
    m_context->mml_writer->record_removal(this);
    return true;
  }
  return false;
}

void Operation::complete_error(int error, const String &msg) {
  {
    ScopedLock lock(m_mutex);
    m_state = OperationState::COMPLETE;
    m_error = error;
    m_error_msg = msg;
    m_dependencies.clear();
    m_obstructions.clear();
    m_exclusivities.clear();
  }

  std::stringstream sout;

  sout << "Operation failed (" << *this << ") " << Error::get_text(error) << " - " << msg;
  HT_INFOF("%s", sout.str().c_str());

  if (m_ephemeral)
    return;

  m_context->mml_writer->record_state(this);
}

void Operation::complete_error(Exception &e) {
  complete_error(e.code(), e.what());
}

void Operation::complete_ok(MetaLog::Entity *additional) {
  {
    ScopedLock lock(m_mutex);
    m_state = OperationState::COMPLETE;
    m_error = Error::OK;
    m_dependencies.clear();
    m_obstructions.clear();
    m_exclusivities.clear();
  }
  {
    ScopedLock lock(m_remove_approval_mutex);
    if (remove_explicitly() && m_remove_approvals == remove_approval_mask()) {
      m_context->reference_manager->remove(this);
      mark_for_removal();
    }
  }
  if (m_ephemeral) {
    HT_ASSERT(additional == 0);
    return;
  }
  std::vector<MetaLog::Entity *> entities;
  entities.push_back(this);
  if (additional)
    entities.push_back(additional);
  m_context->mml_writer->record_state(entities);
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
}

void Operation::swap_sub_operations(std::vector<Operation *> &sub_ops) {
  ScopedLock lock(m_mutex);
  sub_ops.swap(m_sub_ops);
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
    { 0, 0 }
  };

  typedef hash_map<int32_t, const char *> TextMap;

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
