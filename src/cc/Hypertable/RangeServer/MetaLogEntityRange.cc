/*
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

/** @file
 * Definitions for MetaLogEntityRange.
 * This file contains the type definitions for MetaLogEntityRange, a %MetaLog
 * entity class used to persist a range's state to the RSML.
 */

#include "Common/Compat.h"
#include "Common/Mutex.h"
#include "Common/Serialization.h"
#include "MetaLogEntityRange.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

bool MetaLogEntityRange::encountered_upgrade = false;

MetaLogEntityRange::MetaLogEntityRange(const EntityHeader &header_) 
  : Entity(header_) {
}

MetaLogEntityRange::MetaLogEntityRange(const TableIdentifier &table,
                                       const RangeSpec &spec,
                                       const RangeState &state,
                                       bool needs_compaction)
  : Entity(EntityType::RANGE2), m_table(table), m_spec(spec), m_state(state),
    m_needs_compaction(needs_compaction), m_load_acknowledged(false) {
}

void MetaLogEntityRange::get_table_identifier(TableIdentifier &table) {
  ScopedLock lock(m_mutex);
  table = m_table;
}

const char *MetaLogEntityRange::get_table_id() {
  return m_table.id;
}

void MetaLogEntityRange::set_table_generation(uint32_t generation) {
  ScopedLock lock(m_mutex);
  m_table.generation = generation;
}

void MetaLogEntityRange::get_range_spec(RangeSpecManaged &spec) {
  ScopedLock lock(m_mutex);
  spec = m_spec;
}

void MetaLogEntityRange::get_range_state(RangeStateManaged &state) {
  ScopedLock lock(m_mutex);
  state = m_state;
}

int MetaLogEntityRange::get_state() {
  ScopedLock lock(m_mutex);
  return (int)m_state.state;
}

void MetaLogEntityRange::set_state_bits(uint8_t bits) {
  ScopedLock lock(m_mutex);
  m_state.state |= bits;
  m_cond.notify_all();
}

void MetaLogEntityRange::clear_state_bits(uint8_t bits) {
  ScopedLock lock(m_mutex);
  m_state.state &= ~bits;
  m_cond.notify_all();
}

void MetaLogEntityRange::set_state(uint8_t state, const String &source) {
  ScopedLock lock(m_mutex);
  m_state.state = state;
  m_state.set_source(source);
  m_cond.notify_all();
}

void MetaLogEntityRange::clear_state() {
  ScopedLock lock(m_mutex);
  m_state.clear();
  m_cond.notify_all();
}

void MetaLogEntityRange::wait_for_state(uint8_t state) {
  ScopedLock lock(m_mutex);
  while (m_state.state != state)
    m_cond.wait(lock);
}

uint64_t MetaLogEntityRange::get_soft_limit() {
  ScopedLock lock(m_mutex);
  return m_state.soft_limit;
}

void MetaLogEntityRange::set_soft_limit(uint64_t soft_limit) {
  ScopedLock lock(m_mutex);
  m_state.soft_limit = soft_limit;
}

String MetaLogEntityRange::get_split_row() {
  ScopedLock lock(m_mutex);
  return m_state.split_point;
}

void MetaLogEntityRange::set_split_row(const String &row) {
  ScopedLock lock(m_mutex);
  m_state.set_split_point(row);
}

String MetaLogEntityRange::get_transfer_log() {
  ScopedLock lock(m_mutex);
  return m_state.transfer_log ? m_state.transfer_log : "";
}

void MetaLogEntityRange::set_transfer_log(const String &path) {
  ScopedLock lock(m_mutex);
  m_state.set_transfer_log(path);
}

String MetaLogEntityRange::get_start_row() {
  ScopedLock lock(m_mutex);
  return m_spec.start_row;
}

void MetaLogEntityRange::set_start_row(const String &row) {
  ScopedLock lock(m_mutex);
  m_spec.set_start_row(row);
}

String MetaLogEntityRange::get_end_row() {
  ScopedLock lock(m_mutex);
  return m_spec.end_row;
}

void MetaLogEntityRange::set_end_row(const String &row) {
  ScopedLock lock(m_mutex);
  m_spec.set_end_row(row);
}

void MetaLogEntityRange::get_boundary_rows(String &start, String &end) {
  ScopedLock lock(m_mutex);
  start = m_spec.start_row;
  end = m_spec.end_row;
}

String MetaLogEntityRange::get_old_boundary_row() {
  ScopedLock lock(m_mutex);
  return m_state.old_boundary_row;
}

void MetaLogEntityRange::set_old_boundary_row(const String &row) {
  ScopedLock lock(m_mutex);
  m_state.set_old_boundary_row(row);
}

bool MetaLogEntityRange::get_needs_compaction() {
  ScopedLock lock(m_mutex);
  return m_needs_compaction;
}

void MetaLogEntityRange::set_needs_compaction(bool val) {
  ScopedLock lock(m_mutex);
  m_needs_compaction = val;
}

bool MetaLogEntityRange::get_load_acknowledged() {
  ScopedLock lock(m_mutex);
  return m_load_acknowledged;
}

void MetaLogEntityRange::set_load_acknowledged(bool val) {
  ScopedLock lock(m_mutex);
  m_load_acknowledged = val;
}

String MetaLogEntityRange::get_original_transfer_log() {
  ScopedLock lock(m_mutex);
  return m_original_transfer_log;
}

void MetaLogEntityRange::set_original_transfer_log(const String &path) {
  ScopedLock lock(m_mutex);
  m_original_transfer_log = path;
}

void MetaLogEntityRange::save_original_transfer_log() {
  ScopedLock lock(m_mutex);
  if (m_state.transfer_log && *m_state.transfer_log != 0)
    m_original_transfer_log = m_state.transfer_log;
}


void MetaLogEntityRange::rollback_transfer_log() {
  ScopedLock lock(m_mutex);
  m_state.set_transfer_log(m_original_transfer_log);
}

String MetaLogEntityRange::get_source() {
  ScopedLock lock(m_mutex);
  return m_state.source ? m_state.source : "";
}

size_t MetaLogEntityRange::encoded_length() const {
  return m_table.encoded_length() + m_spec.encoded_length() +
    m_state.encoded_length() + 2 + 
    Serialization::encoded_length_vstr(m_original_transfer_log);
}

void MetaLogEntityRange::encode(uint8_t **bufp) const {
  m_table.encode(bufp);
  m_spec.encode(bufp);
  m_state.encode(bufp);
  Serialization::encode_bool(bufp, m_needs_compaction);
  Serialization::encode_bool(bufp, m_load_acknowledged);
  Serialization::encode_vstr(bufp, m_original_transfer_log);
}

void MetaLogEntityRange::decode(const uint8_t **bufp, size_t *remainp,
                                uint16_t definition_version) {
  (void)definition_version;
  m_table.decode(bufp, remainp);
  m_spec.decode(bufp, remainp);
  m_state.decode(bufp, remainp);
  m_needs_compaction = Serialization::decode_bool(bufp, remainp);
  m_load_acknowledged = Serialization::decode_bool(bufp, remainp);
  if (header.type == EntityType::RANGE2)
    m_original_transfer_log = Serialization::decode_vstr(bufp, remainp);
  else {
    header.type = EntityType::RANGE2;
    encountered_upgrade = true;
  }
}

const String MetaLogEntityRange::name() {
  return "Range";
}

void MetaLogEntityRange::display(std::ostream &os) {
  ScopedLock lock(m_mutex);
  os << " " << m_table << " " << m_spec << " " << m_state << " ";
  os << "needs_compaction=" << (m_needs_compaction ? "true" : "false") << " ";
  os << "load_acknowledged=" << (m_load_acknowledged ? "true" : "false") << " ";
  os << "original_transfer_log=" << m_original_transfer_log << " ";
}

