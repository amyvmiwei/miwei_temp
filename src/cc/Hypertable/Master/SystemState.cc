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

#include "MetaLogEntityTypes.h"
#include "SystemState.h"

#include <Common/Config.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace std;

SystemState::SystemState() : MetaLog::Entity(MetaLog::EntityType::SYSTEM_STATE),
                             m_generation(1) {
  SystemVariable::Spec spec;
  for (int i=0; i<SystemVariable::COUNT; i++) {
    spec.code = i;
    spec.value = SystemVariable::default_value(i);
    m_admin_specified.push_back(spec);
    m_auto_specified.push_back(spec);
  }
  m_admin_last_notification.resize(SystemVariable::COUNT, 0);
  m_auto_last_notification.resize(SystemVariable::COUNT, 0);
  m_notification_interval
    = Config::properties->get_i32("Hypertable.Master.NotificationInterval");
}

SystemState::SystemState(const MetaLog::EntityHeader &header_)
  : MetaLog::Entity(header_) {
  m_notification_interval
    = Config::properties->get_i32("Hypertable.Master.NotificationInterval");
}

bool SystemState::admin_set(const std::vector<SystemVariable::Spec> &specs) {
  lock_guard<mutex> lock(m_mutex);
  bool changed = false;
  int32_t now = (int32_t)time(0);
  for (auto &spec : specs) {
    HT_ASSERT(spec.code < (int)m_admin_specified.size());
    String msg = format("System state %s=%s set administratively.",
                        SystemVariable::code_to_string(spec.code),
                        spec.value ? "true" : "false");
    if (spec.value != m_admin_specified[spec.code].value) {
      m_notifications.push_back(NotificationMessage("System State Change", msg));
      m_admin_last_notification[spec.code] = now;
      m_admin_specified[spec.code].value = spec.value;
      changed = true;
    }
    else if (SystemVariable::default_value(spec.code) != spec.value &&
             now - m_admin_last_notification[spec.code] >
             m_notification_interval) {
      m_notifications.push_back(NotificationMessage("System State Alert", msg));
      m_admin_last_notification[spec.code] = now;
    }
  }
  if (changed)
    m_generation++;
  return changed;
}

bool SystemState::admin_set(int code, bool value) {
  lock_guard<mutex> lock(m_mutex);
  int32_t now = (int32_t)time(0);
  HT_ASSERT(code < (int)m_admin_specified.size());
  String msg = format("System state %s=%s set administratively.",
                      SystemVariable::code_to_string(code),
                      value ? "true" : "false");
  if (value != m_admin_specified[code].value) {
    m_notifications.push_back(NotificationMessage("System State Change", msg));
    m_admin_last_notification[code] = now;
    m_admin_specified[code].value = value;
    m_generation++;
    return true;
  }

  if (SystemVariable::default_value(code) != value &&
      now - m_admin_last_notification[code] > m_notification_interval) {
    m_notifications.push_back(NotificationMessage("System State Alert", msg));
    m_admin_last_notification[code] = now;
  }

  return false;
}

bool SystemState::auto_set(int code, bool value, const String &reason) {
  lock_guard<mutex> lock(m_mutex);
  String body = format("System state %s=%s %s.",
                       SystemVariable::code_to_string(code),
                       value ? "true" : "false", reason.c_str());
  int32_t now = (int32_t)time(0);
  HT_ASSERT(code < (int)m_auto_specified.size());
  if (value != m_auto_specified[code].value) {
    m_notifications.push_back(NotificationMessage("System State Change", body));
    m_auto_last_notification[code] = now;
    m_auto_specified[code].value = value;
    m_generation++;
    return true;
  }

  if (SystemVariable::default_value(code) != value &&
      now - m_auto_last_notification[code] > m_notification_interval) {
    m_notifications.push_back(NotificationMessage("System State Alert", body));
    m_auto_last_notification[code] = now;
  }

  return false;
}

void SystemState::get(std::vector<SystemVariable::Spec> &specs,
                      uint64_t *generation) {
  lock_guard<mutex> lock(m_mutex);
  SystemVariable::Spec spec;
  bool default_value;
  HT_ASSERT(m_admin_specified.size() == m_auto_specified.size());
  specs.clear();
  for (int i=0; i<(int)m_admin_specified.size(); i++) {
    spec.code = i;
    default_value = SystemVariable::default_value(i);
    if (m_admin_specified[i].value == default_value &&
        m_auto_specified[i].value == default_value)
      spec.value = default_value;
    else
      spec.value = !default_value;
    specs.push_back(spec);
  }
  *generation = m_generation;
}

bool SystemState::get_notifications(std::vector<NotificationMessage> &notifications) {
  lock_guard<mutex> lock(m_mutex);
  notifications = m_notifications;
  m_notifications.clear();
  return !notifications.empty();
}


void SystemState::get_non_default(std::vector<SystemVariable::Spec> &specs,
                                  uint64_t *generation) {
  lock_guard<mutex> lock(m_mutex);
  SystemVariable::Spec spec;
  bool default_value;
  HT_ASSERT(m_admin_specified.size() == m_auto_specified.size());
  specs.clear();
  for (int i=0; i<(int)m_admin_specified.size(); i++) {
    spec.code = i;
    default_value = SystemVariable::default_value(i);
    if (m_admin_specified[i].value != default_value ||
        m_auto_specified[i].value != default_value) {
      spec.value = !default_value;
      specs.push_back(spec);
    }
  }
  if (generation)
    *generation = m_generation;
}


void SystemState::display(std::ostream &os) {
  lock_guard<mutex> lock(m_mutex);
  os << " generation=" << m_generation << " admin{";
  HT_ASSERT(m_admin_specified.size() == SystemVariable::COUNT);
  os << SystemVariable::specs_to_string(m_admin_specified);
  os << "} auto{";
  os << SystemVariable::specs_to_string(m_auto_specified);
  os << "} ";
}

void SystemState::decode(const uint8_t **bufp, size_t *remainp,
                         uint16_t definition_version) {
  if (definition_version < 4) {
    decode_old(bufp, remainp);
    return;
  }
  Entity::decode(bufp, remainp);
}


uint8_t SystemState::encoding_version() const {
  return 1;
}

size_t SystemState::encoded_length_internal() const {
  size_t length = 8;
  length += 4;
  for (auto &spec : m_admin_specified)
    length += spec.encoded_length() + 4;
  length += 4;
  for (auto &spec : m_auto_specified)
    length += spec.encoded_length() + 4;
  return length;
}

void SystemState::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i64(bufp, m_generation);
  Serialization::encode_i32(bufp, m_admin_specified.size());
  for (size_t i=0; i<m_admin_specified.size(); i++) {
    m_admin_specified[i].encode(bufp);
    Serialization::encode_i32(bufp, m_admin_last_notification[i]);
  }
  Serialization::encode_i32(bufp, m_auto_specified.size());
  for (size_t i=0; i<m_auto_specified.size(); i++) {
    m_auto_specified[i].encode(bufp);
    Serialization::encode_i32(bufp, m_auto_last_notification[i]);
  }
}

void SystemState::decode_internal(uint8_t version, const uint8_t **bufp,
                                  size_t *remainp) {
  SystemVariable::Spec spec;
  m_generation = Serialization::decode_i64(bufp, remainp);
  // Admin set
  int count = Serialization::decode_i32(bufp, remainp);
  for (int i=0; i<count; i++) {
    spec.decode(bufp, remainp);
    m_admin_specified.push_back(spec);
    int32_t timestamp = Serialization::decode_i32(bufp, remainp);
    m_admin_last_notification.push_back(timestamp);
  }
  // Add entries for newly added variables
  for (int i=count; i<SystemVariable::COUNT; i++) {
    spec.code = i;
    spec.value = false;
    m_admin_specified.push_back(spec);
    m_admin_last_notification.push_back(0);
  }
  // Auto set
  count = Serialization::decode_i32(bufp, remainp);
  for (int i=0; i<count; i++) {
    spec.decode(bufp, remainp);
    m_auto_specified.push_back(spec);
    int32_t timestamp = Serialization::decode_i32(bufp, remainp);
    m_auto_last_notification.push_back(timestamp);
  }
  // Add entries for newly added variables
  for (int i=count; i<SystemVariable::COUNT; i++) {
    spec.code = i;
    spec.value = false;
    m_auto_specified.push_back(spec);
    m_auto_last_notification.push_back(0);
  }
}

void SystemState::decode_old(const uint8_t **bufp, size_t *remainp) {
  int32_t timestamp;
  SystemVariable::Spec spec;
  Serialization::decode_i32(bufp, remainp);  // skip version
  m_generation = Serialization::decode_i64(bufp, remainp);
  // Admin set
  int count = Serialization::decode_i32(bufp, remainp);
  for (int i=0; i<count; i++) {
    spec.code = Serialization::decode_i32(bufp, remainp);
    spec.value = Serialization::decode_bool(bufp, remainp);
    m_admin_specified.push_back(spec);
    timestamp = Serialization::decode_i32(bufp, remainp);
    m_admin_last_notification.push_back(timestamp);
  }
  // Add entries for newly added variables
  for (int i=count; i<SystemVariable::COUNT; i++) {
    spec.code = i;
    spec.value = false;
    m_admin_specified.push_back(spec);
    m_admin_last_notification.push_back(0);
  }
  // Auto set
  count = Serialization::decode_i32(bufp, remainp);
  for (int i=0; i<count; i++) {
    spec.code = Serialization::decode_i32(bufp, remainp);
    spec.value = Serialization::decode_bool(bufp, remainp);
    m_auto_specified.push_back(spec);
    timestamp = Serialization::decode_i32(bufp, remainp);
    m_auto_last_notification.push_back(timestamp);
  }
  // Add entries for newly added variables
  for (int i=count; i<SystemVariable::COUNT; i++) {
    spec.code = i;
    spec.value = false;
    m_auto_specified.push_back(spec);
    m_auto_last_notification.push_back(0);
  }
}
