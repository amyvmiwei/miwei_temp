/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include <Common/Compat.h>
#include "IndexTables.h"

#include <Hypertable/Lib/KeySpec.h>
#include <Hypertable/Lib/LoadDataEscape.h>

#include <Common/StaticBuffer.h>

#include <cstdio>

using namespace Hypertable;

void IndexTables::add(const Key &key, uint8_t flag,
                      const void *value, uint32_t value_len,
                      TableMutatorAsync *value_index_mutator,
                      TableMutatorAsync *qualifier_index_mutator) {

  if (value_index_mutator == 0 && qualifier_index_mutator == 0)
    return;

  // now create the key for the index
  KeySpec k;
  k.timestamp = key.timestamp;
  k.flag = flag;
  k.column_family = "v1";

  // every \t in the original row key gets escaped
  const char *escaped_row;
  const char *escaped_qualifier;
  size_t escaped_row_len;
  size_t escaped_qualifier_len;
  LoadDataEscape escape_row;
  LoadDataEscape escape_qualifier;
  LoadDataEscape escape_value;

  escape_row.escape(key.row, key.row_len, &escaped_row, &escaped_row_len);
  escape_qualifier.escape(key.column_qualifier, key.column_qualifier_len,
                          &escaped_qualifier, &escaped_qualifier_len);

  // in a normal (non-qualifier) index the format of the new row
  // key is "<value>\t<qualifier>\t<row>"
  //
  // if value has a 0 byte then we also have to escape it
  if (value_index_mutator) {
    size_t escaped_value_len;
    const char *escaped_value;
    escape_value.escape((const char *)value, (size_t)value_len, &escaped_value, &escaped_value_len);
    //std::cout << escaped_value << std::endl;
    StaticBuffer sb(4 + escaped_value_len + escaped_qualifier_len + escaped_row_len + 3);
    char *p = (char *)sb.base;
    sprintf(p, "%d,", (int)key.column_family_code);
    p     += strlen(p);
    memcpy(p, escaped_value, escaped_value_len);
    p     += escaped_value_len;
    *p++  = '\t';
    memcpy(p, escaped_qualifier, escaped_qualifier_len);
    p     += escaped_qualifier_len;
    *p++  = '\t';
    memcpy(p, escaped_row, escaped_row_len);
    p     += escaped_row_len;
    *p++  = '\0';
    k.row = sb.base;
    k.row_len = p - 1 - (const char *)sb.base; /* w/o the terminating zero */

    // and insert it
    value_index_mutator->set(k, 0, 0);
  }

  // in a qualifier index the format of the new row key is "qualifier\trow"
  if (qualifier_index_mutator) {
    StaticBuffer sb(4 + key.column_qualifier_len + escaped_row_len + 2);
    char *p = (char *)sb.base;
    sprintf(p, "%d,", (int)key.column_family_code);
    p += strlen(p);
    if (key.column_qualifier_len) {
      memcpy(p, key.column_qualifier, key.column_qualifier_len);
      p += key.column_qualifier_len;
    }
    *p++  = '\t';
    memcpy(p, escaped_row, escaped_row_len);
    p += escaped_row_len;
    *p++  = '\0';
    k.row = sb.base;
    k.row_len = p - 1 - (const char *)sb.base; /* w/o the terminating zero */

    // and insert it
    qualifier_index_mutator->set(k, 0, 0);
  }
}
