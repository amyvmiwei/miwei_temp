/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc
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

#ifndef Hypertable_RangeServer_RangeReplayBuffer_h
#define Hypertable_RangeServer_RangeReplayBuffer_h

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/SerializedKey.h>

#include <AsyncComm/CommAddress.h>

#include <Common/DynamicBuffer.h>
#include <Common/Properties.h>
#include <Common/ReferenceCount.h>
#include <Common/StaticBuffer.h>
#include <Common/StringExt.h>

namespace Hypertable {

  class RangeReplayBuffer : public ReferenceCount {
  public:
    RangeReplayBuffer(const String &location, const QualifiedRangeSpec &range) :
      m_location(location), m_range(range) {
      m_addr.set_proxy(location);
    }
    const String& get_location() const {return m_location; }
    size_t memory_used() const { return m_accum.fill(); }
    size_t add(SerializedKey &key, ByteString &value);
    void get_updates(StaticBuffer &updates) { updates = m_accum; }
    void clear();
    CommAddress& get_comm_address() { return m_addr; }
    QualifiedRangeSpec& get_range() { return m_range; }

  private:
    String m_location;
    CommAddress m_addr;
    QualifiedRangeSpec m_range;
    DynamicBuffer m_accum;
  };

  typedef intrusive_ptr<RangeReplayBuffer> RangeReplayBufferPtr;

}

#endif // Hypertable_RangeServer_RangeReplayBuffer_h
