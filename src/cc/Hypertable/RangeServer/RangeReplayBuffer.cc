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
#include "RangeReplayBuffer.h"

using namespace std;
using namespace Hypertable;
using namespace Hypertable::Property;

size_t RangeReplayBuffer::add(SerializedKey &key, ByteString &value) {
  const uint8_t *ptr = key.ptr;
  size_t len = Serialization::decode_vi32(&ptr);
  size_t incr_mem = m_accum.fill();
  m_accum.add(key.ptr, (ptr-key.ptr)+len);
  m_accum.add(value.ptr, value.length());
  incr_mem = m_accum.fill() - incr_mem;
  return incr_mem;
}

void RangeReplayBuffer::clear() {
  m_accum.clear();
}


