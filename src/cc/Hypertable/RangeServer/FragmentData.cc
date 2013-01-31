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

#include "FragmentData.h"

using namespace std;
using namespace Hypertable;

void FragmentData::add(EventPtr &event) {
  m_data.push_back(event);
  return;
}

void FragmentData::merge(RangePtr &range, const char *split_point,
                         DynamicBuffer &dbuf, int64_t *latest_revision) {
  String location;
  QualifiedRangeSpec range_spec;
  DeserializedFragments fragments;
  Key key;
  SerializedKey serkey;
  ByteString value;
  int plan_generation;
  size_t total_size = 0;

  *latest_revision = TIMESTAMP_MIN;

  // de-serialize all objects
  foreach_ht(EventPtr &event, m_data) {
    const uint8_t *decode_ptr = event->payload;
    size_t decode_remain = event->payload_len;
    location = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    plan_generation = Serialization::decode_i32(&decode_ptr, &decode_remain);
    range_spec.decode(&decode_ptr, &decode_remain);
    // skip "fragment"
    (void)Serialization::decode_i32(&decode_ptr, &decode_remain);

    total_size += decode_remain;
    uint8_t *base = (uint8_t *)decode_ptr;
    size_t size = decode_remain;
    fragments.push_back(Fragment(base, size));
  }

  // resize the buffer
  dbuf.ensure(total_size);

  foreach_ht(Fragment &fragment, fragments) {
    const uint8_t *mod, *mod_end;
    size_t kv_pairs = 0;

    mod_end = fragment.first + fragment.second;
    mod = fragment.first;

    while (mod < mod_end) {
      serkey.ptr = mod;
      value.ptr = mod + serkey.length();
      HT_ASSERT(serkey.ptr <= mod_end && value.ptr <= mod_end);
      HT_ASSERT(key.load(serkey));

      if (key.revision > *latest_revision)
        *latest_revision = key.revision;

      range->add(key, value);

      // skip to next kv pair
      value.next();
      dbuf.add_unchecked((const void *)mod, value.ptr-mod);
      kv_pairs++;
      mod = value.ptr;
    }
    if (range->is_metadata())
      HT_INFOF("Just added %d key/value pairs from fragment", (int)kv_pairs);
  }
}
