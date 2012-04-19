/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */
#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/KeySpec.h"

#include "SerializedCellsWriter.h"
#include "SerializedCellsFlag.h"

using namespace Hypertable;


bool SerializedCellsWriter::add(const char *row, const char *column_family,
                                const char *column_qualifier, int64_t timestamp,
                                const void *value, int32_t value_length,
								uint8_t cell_flag) {
  int32_t row_length = strlen(row);
  int32_t column_family_length = column_family ? strlen(column_family) : 0;
  int32_t column_qualifier_length = column_qualifier ? strlen(column_qualifier) : 0;
  uint8_t flag = 0;

  bool need_row = false;
  if (row_length != m_previous_row_length
      || (m_previous_row && memcmp(row, m_previous_row, row_length)))
    need_row = true;

  if (!value && value_length)
    value_length = 0;

  int32_t length = 9 + column_family_length + 3
      + column_qualifier_length + value_length + 1;
  if (m_buf.empty())  // Add version to beginning of buffer
    length += 4;
  if (need_row)
    length += row_length;

  if (timestamp == AUTO_ASSIGN)
    flag |= SerializedCellsFlag::AUTO_TIMESTAMP;
  else if (timestamp != TIMESTAMP_NULL) {
    flag |= SerializedCellsFlag::HAVE_TIMESTAMP;
    length += 8;
  }

  // need to leave room for the termination byte
  if (length > (int32_t)m_buf.remaining()) {
    if (m_grow)
      m_buf.ensure(length);
    else {
      if (!m_buf.empty())
        return false;
      m_buf.grow(length);
    }
  }

  // version
  if (m_buf.empty())
    Serialization::encode_i32(&m_buf.ptr, SerializedCellsVersion::VERSION);

  // flag byte
  *m_buf.ptr++ = flag;

  // timestamp
  if ((flag & SerializedCellsFlag::HAVE_TIMESTAMP) != 0)
    Serialization::encode_i64(&m_buf.ptr, timestamp);

  // revision
  if ((flag & SerializedCellsFlag::HAVE_REVISION) &&
      (flag & SerializedCellsFlag::REV_IS_TS) == 0)
    Serialization::encode_i64(&m_buf.ptr, 0);

  // row; only write it if it's not identical to the previous row
  if (need_row) {
    memcpy(m_buf.ptr, row, row_length);
    m_previous_row = m_buf.ptr;
    m_buf.ptr += row_length;
    m_previous_row_length = row_length;
  }
  *m_buf.ptr++ = 0;

  // column_family
  if (column_family)
    memcpy(m_buf.ptr, column_family, column_family_length);
  m_buf.ptr += column_family_length;
  *m_buf.ptr++ = 0;

  // column_qualifier
  if (column_qualifier)
    memcpy(m_buf.ptr, column_qualifier, column_qualifier_length);
  m_buf.ptr += column_qualifier_length;
  *m_buf.ptr++ = 0;

  Serialization::encode_i32(&m_buf.ptr, value_length);
  if (value)
    memcpy(m_buf.ptr, value, value_length);
  m_buf.ptr += value_length;
  Serialization::encode_i8(&m_buf.ptr, cell_flag);

  return true;
}


void SerializedCellsWriter::clear() { 
  m_buf.clear();
  m_previous_row = 0;
  m_previous_row_length = 0;
}
