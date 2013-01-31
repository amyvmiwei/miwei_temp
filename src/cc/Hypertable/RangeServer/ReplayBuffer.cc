/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc
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
#include "ReplayBuffer.h"
#include "ReplayDispatchHandler.h"

using namespace std;
using namespace Hypertable;
using namespace Hypertable::Property;

ReplayBuffer::ReplayBuffer(PropertiesPtr &props, Comm *comm,
     RangeRecoveryReceiverPlan &plan, const String &location,
     int plan_generation)
  : m_comm(comm), m_plan(plan), m_location(location),
    m_plan_generation(plan_generation), m_memory_used(0) {
  m_flush_limit_aggregate =
      (size_t)props->get_i64("Hypertable.RangeServer.Failover.FlushLimit.Aggregate");
  m_flush_limit_per_range =
      (size_t)props->get_i32("Hypertable.RangeServer.Failover.FlushLimit.PerRange");
  m_timeout_ms = props->get_i32("Hypertable.Failover.Timeout");

  StringSet locations;
  m_plan.get_locations(locations);
  foreach_ht(const String &location, locations) {
    vector<QualifiedRangeSpec> specs;
    m_plan.get_range_specs(location, specs);
    foreach_ht(QualifiedRangeSpec &spec, specs) {
      RangeReplayBufferPtr replay_buffer
          = new RangeReplayBuffer(location, spec);
      m_buffer_map[spec] = replay_buffer;
    }
  }
}

void ReplayBuffer::add(const TableIdentifier &table, SerializedKey &key,
        ByteString &value) {
  const char *row = key.row();
  QualifiedRangeSpec range;
  // skip over any cells that are not in the recovery plan
  if (m_plan.get_range_spec(table, row, range)) {
    ReplayBufferMap::iterator it = m_buffer_map.find(range);
    if (it == m_buffer_map.end())
      return;
    m_memory_used += it->second->add(key, value);
    if (m_memory_used > m_flush_limit_aggregate ||
       it->second->memory_used() > m_flush_limit_per_range) {
#if 0
       HT_DEBUG_OUT << "flushing replay buffer for fragment " << m_fragment
           << ", total mem=" << m_memory_used << " range mem used="
           << it->second->memory_used() << ", total limit="
           << m_flush_limit_aggregate << ", per range limit="
           << m_flush_limit_per_range << " key=" << row << HT_END;
#endif
       flush();
    }
  }
  else {
    HT_DEBUG_OUT << "Skipping key " << row << " for table " << table.id
        << " because it is not in recovery plan" << HT_END;
  }
}

void ReplayBuffer::flush() {
  ReplayDispatchHandler handler(m_comm, m_location, m_plan_generation, m_timeout_ms);

  foreach_ht(ReplayBufferMap::value_type &vv, m_buffer_map) {

    if (vv.second->memory_used() > 0) {
      RangeReplayBuffer &buffer = *(vv.second.get());
      CommAddress &addr         = buffer.get_comm_address();
      QualifiedRangeSpec &range = buffer.get_range();
      StaticBuffer updates;
      buffer.get_updates(updates);
      handler.add(addr, range, m_fragment, updates);
      buffer.clear();
    }
  }

  handler.wait_for_completion();

  m_memory_used=0;
}
