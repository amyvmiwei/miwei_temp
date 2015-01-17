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

#include "LegacyDecoder.h"

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServerRecovery;

#define TABLE_IDENTIFIER_VERSION 2

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, TableIdentifier *tid) {
  int16_t version;
  HT_TRY("decoding table identitier version",
         version = Serialization::decode_i16(bufp, remainp));
  if (version != TABLE_IDENTIFIER_VERSION) {
    *bufp -= 2;
    *remainp += 2;
    HT_TRY("decoding table identitier",
           tid->id = Serialization::decode_vstr(bufp, remainp);
           tid->generation = Serialization::decode_i32(bufp, remainp));
  }
  else {
    HT_TRY("decoding table identitier",
           tid->id = Serialization::decode_vstr(bufp, remainp);
           tid->generation = Serialization::decode_i64(bufp, remainp));
  }
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, TableIdentifierManaged *tid) {
  legacy_decode(bufp, remainp, (TableIdentifier *)tid);
  *tid = *tid;
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, RangeSpec *spec) {
  HT_TRY("decoding range spec",
    spec->start_row = Serialization::decode_vstr(bufp, remainp);
    spec->end_row = Serialization::decode_vstr(bufp, remainp));
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, RangeSpecManaged *spec) {
  legacy_decode(bufp, remainp, (RangeSpec *)spec);
  *spec = *spec;
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, QualifiedRangeSpec *spec) {
  legacy_decode(bufp, remainp, &spec->table);
  legacy_decode(bufp, remainp, &spec->range);
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, RangeMoveSpec *spec) {
  TableIdentifier table;
  legacy_decode(bufp, remainp, &table);
  spec->table = table;
  RangeSpec range;
  legacy_decode(bufp, remainp, &range);
  spec->range = range;
  spec->source_location = Serialization::decode_vstr(bufp, remainp);
  spec->dest_location = Serialization::decode_vstr(bufp, remainp);
  spec->error = Serialization::decode_i32(bufp, remainp);
  spec->complete = Serialization::decode_bool(bufp, remainp);
}

#define RANGESTATE_VERSION 100

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, RangeState *state) {
  uint8_t version;
  try {
    version = Serialization::decode_byte(bufp, remainp);
    if (version == RANGESTATE_VERSION)
      state->state = Serialization::decode_byte(bufp, remainp);
    else
      state->state = version;
    state->timestamp = Serialization::decode_i64(bufp, remainp);
    state->soft_limit = Serialization::decode_i64(bufp, remainp);
    state->transfer_log = Serialization::decode_vstr(bufp, remainp);
    state->split_point = Serialization::decode_vstr(bufp, remainp);
    state->old_boundary_row = Serialization::decode_vstr(bufp, remainp);
    if (version == RANGESTATE_VERSION)
      state->source = Serialization::decode_vstr(bufp, remainp);
  }
  HT_RETHROW("decoding range state")
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, RangeStateManaged *state) {
  legacy_decode(bufp, remainp, (RangeState *)state);
  *state = *state;
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, FragmentReplayPlan *plan) {
  plan->location = Serialization::decode_vstr(bufp, remainp);
  plan->fragment = Serialization::decode_i32(bufp, remainp);
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, ReplayPlan *plan) {
  size_t count = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<count; ++i) {
    FragmentReplayPlan entry;
    legacy_decode(bufp, remainp, &entry);
    plan->container.insert(entry);
  }
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, ServerReceiverPlan *plan) {
  plan->location = Serialization::decode_vstr(bufp, remainp);
  legacy_decode(bufp, remainp, &plan->spec);
  legacy_decode(bufp, remainp, &plan->state);
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, ReceiverPlan *plan) {
  size_t count = Serialization::decode_i32(bufp, remainp);
  for (size_t i = 0; i<count; ++i) {
    ServerReceiverPlan tmp;
    legacy_decode(bufp, remainp, &tmp);
    ServerReceiverPlan entry(plan->arena, tmp.location, tmp.spec.table,
                             tmp.spec.range, tmp.state);
    plan->container.insert(entry);
  }
}

void Hypertable::legacy_decode(const uint8_t **bufp, size_t *remainp, Plan *plan) {
  plan->type = Serialization::decode_i32(bufp, remainp);
  legacy_decode(bufp, remainp, &plan->replay_plan);
  legacy_decode(bufp, remainp, &plan->receiver_plan);
}
