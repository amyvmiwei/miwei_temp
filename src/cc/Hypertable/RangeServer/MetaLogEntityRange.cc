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

/** @file
 * Definitions for MetaLogEntityRange.
 * This file contains the type definitions for MetaLogEntityRange, a %MetaLog
 * entity class used to persist a range's state to the RSML.
 */

#include <Common/Compat.h>

#include "MetaLogEntityRange.h"

#include <Hypertable/Lib/LegacyDecoder.h>

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::MetaLog;
using namespace std;

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
  lock_guard<mutex> lock(m_mutex);
  table = m_table;
}

const char *MetaLogEntityRange::get_table_id() {
  return m_table.id;
}

void MetaLogEntityRange::set_table_generation(uint32_t generation) {
  lock_guard<mutex> lock(m_mutex);
  m_table.generation = generation;
}

void MetaLogEntityRange::get_range_spec(RangeSpecManaged &spec) {
  lock_guard<mutex> lock(m_mutex);
  spec = m_spec;
}

void MetaLogEntityRange::get_range_state(RangeStateManaged &state) {
  lock_guard<mutex> lock(m_mutex);
  state = m_state;
}

int MetaLogEntityRange::get_state() {
  lock_guard<mutex> lock(m_mutex);
  return (int)m_state.state;
}

void MetaLogEntityRange::set_state_bits(uint8_t bits) {
  lock_guard<mutex> lock(m_mutex);
  m_state.state |= bits;
}

void MetaLogEntityRange::clear_state_bits(uint8_t bits) {
  lock_guard<mutex> lock(m_mutex);
  m_state.state &= ~bits;
}

void MetaLogEntityRange::set_state(uint8_t state, const String &source) {
  lock_guard<mutex> lock(m_mutex);
  m_state.state = state;
  m_state.set_source(source);
}

void MetaLogEntityRange::clear_state() {
  lock_guard<mutex> lock(m_mutex);
  m_state.clear();
}

uint64_t MetaLogEntityRange::get_soft_limit() {
  lock_guard<mutex> lock(m_mutex);
  return m_state.soft_limit;
}

void MetaLogEntityRange::set_soft_limit(uint64_t soft_limit) {
  lock_guard<mutex> lock(m_mutex);
  m_state.soft_limit = soft_limit;
}

String MetaLogEntityRange::get_split_row() {
  lock_guard<mutex> lock(m_mutex);
  return m_state.split_point;
}

void MetaLogEntityRange::set_split_row(const String &row) {
  lock_guard<mutex> lock(m_mutex);
  m_state.set_split_point(row);
}

String MetaLogEntityRange::get_transfer_log() {
  lock_guard<mutex> lock(m_mutex);
  return m_state.transfer_log ? m_state.transfer_log : "";
}

void MetaLogEntityRange::set_transfer_log(const String &path) {
  lock_guard<mutex> lock(m_mutex);
  m_state.set_transfer_log(path);
}

String MetaLogEntityRange::get_start_row() {
  lock_guard<mutex> lock(m_mutex);
  return m_spec.start_row;
}

void MetaLogEntityRange::set_start_row(const String &row) {
  lock_guard<mutex> lock(m_mutex);
  m_spec.set_start_row(row);
}

String MetaLogEntityRange::get_end_row() {
  lock_guard<mutex> lock(m_mutex);
  return m_spec.end_row;
}

void MetaLogEntityRange::set_end_row(const String &row) {
  lock_guard<mutex> lock(m_mutex);
  m_spec.set_end_row(row);
}

void MetaLogEntityRange::get_boundary_rows(String &start, String &end) {
  lock_guard<mutex> lock(m_mutex);
  start = m_spec.start_row;
  end = m_spec.end_row;
}

String MetaLogEntityRange::get_old_boundary_row() {
  lock_guard<mutex> lock(m_mutex);
  return m_state.old_boundary_row;
}

void MetaLogEntityRange::set_old_boundary_row(const String &row) {
  lock_guard<mutex> lock(m_mutex);
  m_state.set_old_boundary_row(row);
}

bool MetaLogEntityRange::get_needs_compaction() {
  lock_guard<mutex> lock(m_mutex);
  return m_needs_compaction;
}

void MetaLogEntityRange::set_needs_compaction(bool val) {
  lock_guard<mutex> lock(m_mutex);
  m_needs_compaction = val;
}

bool MetaLogEntityRange::get_load_acknowledged() {
  lock_guard<mutex> lock(m_mutex);
  return m_load_acknowledged;
}

void MetaLogEntityRange::set_load_acknowledged(bool val) {
  lock_guard<mutex> lock(m_mutex);
  m_load_acknowledged = val;
}

String MetaLogEntityRange::get_source() {
  lock_guard<mutex> lock(m_mutex);
  return m_state.source ? m_state.source : "";
}

void MetaLogEntityRange::decode(const uint8_t **bufp, size_t *remainp,
                                uint16_t definition_version) {
  if (definition_version < 3)
    decode_old(bufp, remainp);
  else
    Entity::decode(bufp, remainp);
}

const String MetaLogEntityRange::name() {
  return "Range";
}

void MetaLogEntityRange::display(std::ostream &os) {
  lock_guard<mutex> lock(m_mutex);
  os << " " << m_table << " " << m_spec << " " << m_state << " ";
  os << "needs_compaction=" << (m_needs_compaction ? "true" : "false") << " ";
  os << "load_acknowledged=" << (m_load_acknowledged ? "true" : "false") << " ";
}

uint8_t MetaLogEntityRange::encoding_version() const {
  return 1;
}

size_t MetaLogEntityRange::encoded_length_internal() const {
  return m_table.encoded_length() + m_spec.encoded_length() +
    m_state.encoded_length() + 2;
}

void MetaLogEntityRange::encode_internal(uint8_t **bufp) const {
  m_table.encode(bufp);
  m_spec.encode(bufp);
  m_state.encode(bufp);
  Serialization::encode_bool(bufp, m_needs_compaction);
  Serialization::encode_bool(bufp, m_load_acknowledged);
}

void MetaLogEntityRange::decode_internal(uint8_t version, const uint8_t **bufp,
                                         size_t *remainp) {
  m_table.decode(bufp, remainp);
  m_spec.decode(bufp, remainp);
  m_state.decode(bufp, remainp);
  m_needs_compaction = Serialization::decode_bool(bufp, remainp);
  m_load_acknowledged = Serialization::decode_bool(bufp, remainp);
}

void MetaLogEntityRange::decode_old(const uint8_t **bufp, size_t *remainp) {
  legacy_decode(bufp, remainp, &m_table);
  legacy_decode(bufp, remainp, &m_spec);
  legacy_decode(bufp, remainp, &m_state);
  m_needs_compaction = Serialization::decode_bool(bufp, remainp);
  m_load_acknowledged = Serialization::decode_bool(bufp, remainp);
  if (header.type == EntityType::RANGE2)
    Serialization::decode_vstr(bufp, remainp);
  else {
    header.type = EntityType::RANGE2;
    encountered_upgrade = true;
  }
}
