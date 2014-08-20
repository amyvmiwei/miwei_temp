/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Definitions for SshOutputCollector.
/// This file contains type definitions for SshOutputCollector, a class for
/// collecting buffers of output returned from the exection of a remote SSH
/// command.

#include <Common/Compat.h>

#include "SshOutputCollector.h"

using namespace Hypertable;
using namespace std;


void SshOutputCollector::iterator::increment() {
  m_index = m_next_index;
  m_offset = m_next_offset;
  m_line.clear();
  while (m_next_index < m_buffers.size()) {
    const char *base = m_buffers[m_next_index].base + m_next_offset;
    const char *end = m_buffers[m_next_index].ptr;
    const char *ptr = base;
    while (ptr < end && *ptr != '\n')
      ptr++;
    if (ptr > base)
      m_line.append(base, ptr-base);
    if (ptr < end) {
      ptr++;  // skip newline
      if (ptr == end) {
        m_next_index++;
        m_next_offset = 0;
      }
      else
        m_next_offset = ptr - m_buffers[m_next_index].base;
      return;
    }
    m_next_index++;
    m_next_offset = 0;
  }
}
