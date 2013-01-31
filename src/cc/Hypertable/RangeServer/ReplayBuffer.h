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

#ifndef HYPERTABLE_REPLAYBUFFER_H
#define HYPERTABLE_REPLAYBUFFER_H

#include <map>
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "AsyncComm/Comm.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeRecoveryReceiverPlan.h"
#include "RangeReplayBuffer.h"

namespace Hypertable {

  class ReplayBuffer : public ReferenceCount {
  public:
    ReplayBuffer(PropertiesPtr &props, Comm *comm,
                 RangeRecoveryReceiverPlan &plan, const String &location,
                 int plan_generation);
    
    void add(const TableIdentifier &table, SerializedKey &key,
             ByteString &value);

    size_t memory_used() const { return m_memory_used; }

    void set_current_fragment(uint32_t fragment_id) {
      m_fragment = fragment_id;
    }

    void flush();

  private:

    Comm *m_comm;
    RangeRecoveryReceiverPlan &m_plan;
    typedef map<QualifiedRangeSpec, RangeReplayBufferPtr> ReplayBufferMap;
    ReplayBufferMap m_buffer_map;
    String m_location;
    int m_plan_generation;
    size_t m_memory_used;
    size_t m_flush_limit_aggregate;
    size_t m_flush_limit_per_range;
    int32_t m_timeout_ms;
    uint32_t m_fragment;
  };

  typedef intrusive_ptr<ReplayBuffer> ReplayBufferPtr;

} // namespace Hypertable

#endif // HYPERTABLE_REPLAYBUFFER_H
