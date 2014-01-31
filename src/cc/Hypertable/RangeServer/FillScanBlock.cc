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
#include "FillScanBlock.h"

namespace Hypertable {

  bool
  FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf, int64_t buffer_size) {
    Key key;
    ByteString value;
    size_t value_len;
    bool more = true;
    size_t limit = buffer_size;
    size_t remaining = buffer_size;
    uint8_t *ptr;
    ScanContext *scan_context = scanner->scan_context();
    bool keys_only = scan_context->spec->keys_only;
    char numbuf[24];
    DynamicBuffer counter_value;
    bool counter;
    String empty_value("");

    assert(dbuf.base == 0);

    while ((more = scanner->get(key, value))) {
      counter = false;

      if (keys_only) {
        value.ptr = 0;
        counter_value.clear();
        value_len = 0;
      }
      else {
        counter = scan_context->cell_predicates[key.column_family_code].counter &&
          (key.flag == FLAG_INSERT);

        if (counter) {
          const uint8_t *decode;
          int64_t count;
          size_t remain = value.decode_length(&decode);
          // value must be encoded 64 bit int followed by '=' character
          if (remain != 9)
            HT_FATAL_OUT << "Expected counter to be encoded 64 bit int but remain=" << remain
              << " ,key=" << key << " ,value="<< value.str() << HT_END;

          count = Serialization::decode_i64(&decode, &remain);
          HT_ASSERT(*decode == '=');
          //convert counter to ascii
          sprintf(numbuf, "%lld", (Lld) count);
          value_len = strlen(numbuf);
          counter_value.clear();
          append_as_byte_string(counter_value, numbuf, value_len);
          value_len = counter_value.fill();
        }
        else
          value_len = value.length();
      }

      if (value.ptr == 0) {
        value.ptr = (const uint8_t *)empty_value.c_str();
        value_len = 1;
      }

      if (dbuf.base == 0) {
        if (key.length + value_len > limit) {
          limit = key.length + value_len;
          remaining = limit;
        }
        dbuf.reserve(4 + limit);
        // skip encoded length
        dbuf.ptr = dbuf.base + 4;
      }
      if (key.length + value_len <= remaining) {

        dbuf.add_unchecked(key.serial.ptr, key.length);

        if (counter)
          dbuf.add_unchecked(counter_value.base, value_len);
        else
          dbuf.add_unchecked(value.ptr, value_len);

        remaining -= (key.length + value_len);
        scanner->forward();
      }
      else
        break;
    }

    if (dbuf.base == 0) {
      dbuf.reserve(4);
      dbuf.ptr = dbuf.base + 4;
    }

    ptr = dbuf.base;
    Serialization::encode_i32(&ptr, dbuf.fill() - 4);

    return more;
  }

}
