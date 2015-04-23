/** -*- c++ -*-
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

#include "Common/Compat.h"
#include <cassert>
#include <iostream>

#include "Key.h"

using namespace Hypertable;
using namespace std;


namespace {
  const char end_row_chars[3] = { (char)0xff, (char)0xff, 0 };
  const char end_root_row_chars[7] = { '0', '/', '0', ':', (char)0xff, (char)0xff, 0 };

  size_t
  write_key(uint8_t *buf, uint8_t control, uint8_t flag, const char *row, uint32_t row_len,
            uint8_t column_family_code, const char *column_qualifier, uint32_t column_qualifier_len) {
    uint8_t *ptr = buf;
    *ptr++ = control;
    strcpy((char *)ptr, row);
    ptr += row_len + 1;
    *ptr++ = column_family_code;
    if (column_qualifier) {
      strcpy((char *)ptr, column_qualifier);
      ptr += column_qualifier_len + 1;
    }
    else
      *ptr++ = 0;
    *ptr++ = flag;
    return ptr-buf;
  }

  void
  create_key_and_append(DynamicBuffer &dst_buf, uint8_t flag, const char *row,
      uint32_t row_len, uint8_t column_family_code, const char *column_qualifier,
      uint32_t column_qualifier_len, int64_t timestamp, int64_t revision,
      bool time_order_asc) {
    size_t len = 1 + row_len + 4 + column_qualifier_len;
    uint8_t control = 0;

    if (timestamp == AUTO_ASSIGN)
      control = Key::AUTO_TIMESTAMP;
    else if (timestamp != TIMESTAMP_NULL) {
      len += 8;
      control = Key::HAVE_TIMESTAMP;
      if (timestamp == revision)
        control |= Key::REV_IS_TS;
    }

    if (!time_order_asc)
      control |= Key::TS_CHRONOLOGICAL;

    if (revision != AUTO_ASSIGN) {
      if (!(control & Key::REV_IS_TS))
        len += 8;
      control |= Key::HAVE_REVISION;
    }

    if (dst_buf.remaining() < len + 6)
      dst_buf.grow(dst_buf.fill() + len + 6);
    Serialization::encode_vi32(&dst_buf.ptr, len);
    dst_buf.ptr += write_key(dst_buf.ptr, control, flag, row, row_len,
                             column_family_code, column_qualifier, column_qualifier_len);

    if (control & Key::HAVE_TIMESTAMP)
      Key::encode_ts64(&dst_buf.ptr, timestamp, time_order_asc);

    if ((control & Key::HAVE_REVISION)
        && !(control & Key::REV_IS_TS))
      Key::encode_ts64(&dst_buf.ptr, revision);

    assert(dst_buf.fill() <= dst_buf.size);

  }
}


namespace Hypertable {

  const char *Key::END_ROW_MARKER = (const char *)end_row_chars;
  const char *Key::END_ROOT_ROW   = (const char *)end_root_row_chars;

  void
  create_key_and_append(DynamicBuffer &dst_buf, const Key& key, bool time_order_asc) {
    ::create_key_and_append(
      dst_buf,
      key.flag,
      key.row,
      key.row_len,
      key.column_family_code,
      key.column_qualifier,
      key.column_qualifier_len,
      key.timestamp,
      AUTO_ASSIGN,//key.revision,
      time_order_asc);
  }

  void
  create_key_and_append(DynamicBuffer &dst_buf, uint8_t flag, const char *row,
      uint8_t column_family_code, const char *column_qualifier,
      int64_t timestamp, int64_t revision, bool time_order_asc) {
    ::create_key_and_append(
      dst_buf,
      flag,
      row,
      strlen(row),
      column_family_code,
      column_qualifier,
      column_qualifier && *column_qualifier ? strlen(column_qualifier) : 0,
      timestamp,
      revision,
      time_order_asc);
  }

  void create_key_and_append(DynamicBuffer &dst_buf, const char *row, bool time_order_asc) {
    uint8_t control = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP | Key::REV_IS_TS;
    if (!time_order_asc)
      control |= Key::TS_CHRONOLOGICAL;
    size_t row_len = strlen(row);
    size_t len = 13 + row_len;
    if (dst_buf.remaining() < len + 6)
      dst_buf.grow(dst_buf.fill() + len + 6);
    Serialization::encode_vi32(&dst_buf.ptr, len);
    dst_buf.ptr += write_key(dst_buf.ptr, control, 0, row, row_len, 0, 0, 0);
    Key::encode_ts64(&dst_buf.ptr, 0);
    assert(dst_buf.fill() <= dst_buf.size);
  }

  Key::Key(const SerializedKey& key) {
    HT_EXPECT(load(key), Error::BAD_KEY);
  }

  /**
   * TODO: Re-implement below function in terms of this function
   */
  bool Key::load(const SerializedKey& key) {
    serial = key;
    const uint8_t* ptr = serial.ptr;
    size_t len = Serialization::decode_vi32(&ptr);

    length = len + (ptr - serial.ptr);

    const uint8_t *end_ptr = ptr + len;

    control = *ptr++;
    row = (const char *)ptr;

    while (ptr < end_ptr && *ptr != 0)
      ptr++;

    row_len = ptr - (uint8_t *)row;
    assert(strlen(row) == row_len);
    ptr++;

    if (ptr >= end_ptr) {
      cerr << "row decode overrun" << endl;
      return false;
    }

    column_family_code = *ptr++;
    column_qualifier = (const char *)ptr;

    while (ptr < end_ptr && *ptr != 0)
      ptr++;

    column_qualifier_len = ptr - (uint8_t *)column_qualifier;
    assert(strlen(column_qualifier) == column_qualifier_len);
    ptr++;

    if (ptr >= end_ptr) {
      cerr << "qualifier decode overrun" << endl;
      return false;
    }

    flag_ptr = ptr;
    flag = *ptr++;

    if (control & HAVE_TIMESTAMP) {
      timestamp = decode_ts64((const uint8_t **)&ptr,
              (control&TS_CHRONOLOGICAL) == 0);
      if (control & REV_IS_TS) {
        revision = timestamp;
        assert(ptr == end_ptr);
        return true;
      }
    }
    else {
      if (control & AUTO_TIMESTAMP)
        timestamp = AUTO_ASSIGN;
      else
        timestamp = TIMESTAMP_NULL;
    }

    if (control & HAVE_REVISION)
      revision = decode_ts64((const uint8_t **)&ptr,
                             (control&TS_CHRONOLOGICAL) == 0);
    else
      revision = AUTO_ASSIGN;

    assert(ptr == end_ptr);

    return true;
  }


  std::ostream &operator<<(std::ostream &os, const Key &key) {
    bool got = false;
    os << "control=(";
    if (key.control & Key::HAVE_REVISION) {
      os << "REV";
      got = true;
    }
    if (key.control & Key::HAVE_TIMESTAMP) {
      os << ((got) ? "|TS" : "TS");
      got = true;
    }
    if (key.control & Key::AUTO_TIMESTAMP) {
      os << ((got) ? "|AUTO" : "AUTO");
      got = true;
    }
    if (key.control & Key::REV_IS_TS) {
      os << ((got) ? "|SHARED" : "SHARED");
      got = true;
    }
    os << ") row='" << key.row << "' ";
    if (key.flag == FLAG_DELETE_ROW)
      os << "ts=" << key.timestamp << " rev=" << key.revision << " DELETE_ROW";
    else {
      os << "family=" << (int)key.column_family_code;
      if (key.column_qualifier)
        os << " qualifier='" << key.column_qualifier << "'";
      os << " ts=" << key.timestamp;
      os << " rev=" << key.revision;

      if (key.flag == FLAG_DELETE_ROW)
        os << " DELETE_ROW";
      else if (key.flag == FLAG_DELETE_COLUMN_FAMILY)
        os << " DELETE_COLUMN_FAMILY";
      else if (key.flag == FLAG_DELETE_CELL)
        os << " DELETE_CELL";
      else if (key.flag == FLAG_DELETE_CELL_VERSION)
        os << " DELETE_CELL_VERSION";
      else if (key.flag == FLAG_INSERT)
        os << " INSERT";
      else
        os << key.flag << "(unrecognized flag)";
    }
    return os;
  }
}
