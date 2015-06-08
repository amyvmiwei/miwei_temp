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

/// @file
/// Definitions for FragmentData.
/// This file contains the type definitions for FragmentData, a class for
/// accumulating phantom update data for a phantom range.

#include <Common/Compat.h>
#include "FragmentData.h"

#include <Hypertable/RangeServer/Global.h>

#include <Hypertable/Lib/ClusterId.h>
#include <Hypertable/Lib/RangeServer/Request/Parameters/PhantomUpdate.h>

using namespace std;
using namespace Hypertable;

FragmentData::~FragmentData() {
  Global::memory_tracker->subtract(m_memory_consumption);
}

void FragmentData::add(EventPtr &event) {
  m_data.push_back(event);
  int64_t memory_added = sizeof(Event) + event->payload_len;
  Global::memory_tracker->add(memory_added);
  m_memory_consumption += memory_added;
  return;
}

void FragmentData::clear() {
  m_data.clear();
  Global::memory_tracker->subtract(m_memory_consumption);
  m_memory_consumption = 0;
}


void FragmentData::merge(TableIdentifier &table, RangePtr &range,
                         CommitLogPtr &log) {

  Key key;
  SerializedKey serkey;
  ByteString value;
  const uint8_t *mod, *mod_end;
  DynamicBuffer dbuf;
  int64_t latest_revision;
  int64_t total_bytes = 0;
  size_t kv_pairs = 0;

  // de-serialize all objects
  for (auto &event : m_data) {
    const uint8_t *ptr = event->payload;
    size_t remain = event->payload_len;
    Lib::RangeServer::Request::Parameters::PhantomUpdate params;
    params.decode(&ptr, &remain);

    dbuf.clear();
    dbuf.ensure(table.encoded_length() + remain);
    table.encode(&dbuf.ptr);

    latest_revision = TIMESTAMP_MIN;

    mod = (const uint8_t *)ptr;
    mod_end = mod + remain;

    total_bytes += remain;

    while (mod < mod_end) {
      serkey.ptr = mod;
      value.ptr = mod + serkey.length();
      HT_ASSERT(serkey.ptr <= mod_end && value.ptr <= mod_end);
      HT_ASSERT(key.load(serkey));

      if (key.revision > latest_revision)
        latest_revision = key.revision;

      range->add(key, value);

      // skip to next kv pair
      value.next();
      dbuf.add_unchecked((const void *)mod, value.ptr-mod);
      kv_pairs++;
      mod = value.ptr;
    }
    
    HT_ASSERT(dbuf.ptr-dbuf.base <= (long)dbuf.size);

    if (remain)
      log->write(ClusterId::get(), dbuf, latest_revision, Filesystem::Flags::NONE);
  }

  HT_INFOF("Just added %d key/value pairs (%lld bytes)",
           (int)kv_pairs, (Lld)total_bytes);
}
