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
#include "Common/Serialization.h"
#include "MetaLogEntityRange.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

bool EntityRange::encountered_upgrade = false;

EntityRange::EntityRange(const EntityHeader &header_) : Entity(header_) { }

EntityRange::EntityRange(const TableIdentifier &identifier,
                         const RangeSpec &range, const RangeState &state_,
                         bool needs_compaction_)
  : Entity(EntityType::RANGE2), table(identifier), spec(range), state(state_),
    needs_compaction(needs_compaction_), load_acknowledged(false) {
}


size_t EntityRange::encoded_length() const {
  return table.encoded_length() + spec.encoded_length() +
    state.encoded_length() + 2 + 
    Serialization::encoded_length_vstr(original_transfer_log);
}


void EntityRange::encode(uint8_t **bufp) const {
  table.encode(bufp);
  spec.encode(bufp);
  state.encode(bufp);
  Serialization::encode_bool(bufp, needs_compaction);
  Serialization::encode_bool(bufp, load_acknowledged);
  Serialization::encode_vstr(bufp, original_transfer_log);
}

void EntityRange::decode(const uint8_t **bufp, size_t *remainp) {
  table.decode(bufp, remainp);
  spec.decode(bufp, remainp);
  state.decode(bufp, remainp);
  needs_compaction = Serialization::decode_bool(bufp, remainp);
  load_acknowledged = Serialization::decode_bool(bufp, remainp);
  if (header.type == EntityType::RANGE2)
    original_transfer_log = Serialization::decode_vstr(bufp, remainp);
  else {
    header.type = EntityType::RANGE2;
    encountered_upgrade = true;
  }
}

const String EntityRange::name() {
  return "Range";
}

void EntityRange::display(std::ostream &os) {
  os << " " << table << " " << spec << " " << state << " ";
  os << "needs_compaction=" << (needs_compaction ? "true" : "false") << " ";
  os << "load_acknowledged=" << (load_acknowledged ? "true" : "false") << " ";
  os << "original_transfer_log=" << original_transfer_log << " ";
}

