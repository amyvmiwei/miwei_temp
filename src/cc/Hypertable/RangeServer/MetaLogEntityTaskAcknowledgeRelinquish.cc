/* -*- c++ -*-
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

#include <Common/Compat.h>

#include "Global.h"
#include "MetaLogEntityTaskAcknowledgeRelinquish.h"

#include <Hypertable/Lib/LegacyDecoder.h>

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::MetaLog;

EntityTaskAcknowledgeRelinquish::EntityTaskAcknowledgeRelinquish(const EntityHeader &header_) : EntityTask(header_) { }

EntityTaskAcknowledgeRelinquish::EntityTaskAcknowledgeRelinquish(const String &loc, int64_t id, const TableIdentifier &t, const RangeSpec &rs)
  : EntityTask(EntityType::TASK_ACKNOWLEDGE_RELINQUISH), location(loc), range_id(id), table(t), range_spec(rs) {
}

bool EntityTaskAcknowledgeRelinquish::execute() {
  String label = format("%lld %s %s[%s..%s]", (Lld)range_id, location.c_str(),
                        table.id, range_spec.start_row, range_spec.end_row);

  try {
    HT_INFOF("relinquish_acknowledge(%s)", label.c_str());
    Global::master_client->relinquish_acknowledge(location, range_id, table, range_spec);
    Global::immovable_range_set_remove(table, range_spec);
  }
  catch (Exception &e) {
    HT_WARN_OUT << "Master::relinquish_acknowledge(" << label << ") error - " << e << HT_END;
    return false;
  }

  HT_INFOF("Successfully relinquished %s", label.c_str());

  return true;
}

void EntityTaskAcknowledgeRelinquish::work_queue_add_hook() {
  Global::immovable_range_set_add(table, range_spec);
}

void EntityTaskAcknowledgeRelinquish::decode(const uint8_t **bufp,
                                             size_t *remainp,
                                             uint16_t definition_version) {
  if (definition_version <= 2) {
    decode_old(bufp, remainp);
    return;
  }
  Entity::decode(bufp, remainp);
}

const String EntityTaskAcknowledgeRelinquish::name() {
  return format("TaskAcknowledgeRelinquish %s[%s..%s]", table.id, range_spec.start_row, range_spec.end_row);
}

void EntityTaskAcknowledgeRelinquish::display(std::ostream &os) {
  os << " " << location << " " << table << " " << range_spec << " ";
}

uint8_t EntityTaskAcknowledgeRelinquish::encoding_version() const {
  return 1;
}

size_t EntityTaskAcknowledgeRelinquish::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(location) + 8 +
    table.encoded_length() + range_spec.encoded_length();
}

void EntityTaskAcknowledgeRelinquish::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, location);
  Serialization::encode_i64(bufp, range_id);
  table.encode(bufp);
  range_spec.encode(bufp);
}

void EntityTaskAcknowledgeRelinquish::decode_internal(uint8_t version, const uint8_t **bufp,
                                                      size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  range_id = Serialization::decode_i64(bufp, remainp);
  table.decode(bufp, remainp);
  range_spec.decode(bufp, remainp);
}

void EntityTaskAcknowledgeRelinquish::decode_old(const uint8_t **bufp, size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  range_id = 0;
  legacy_decode(bufp, remainp, &table);
  legacy_decode(bufp, remainp, &range_spec);
}
