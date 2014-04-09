/* -*- c++ -*-
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

/// @file
/// Definitions for TableParts.
/// This file contains definitions for TableParts, a class that represents a
/// set of table parts (i.e. PRIMARY, VALUE_INDEX, QUALIFIER_INDEX)

#include <Common/Compat.h>
#include "TableParts.h"

using namespace Hypertable;

void TableParts::encode(uint8_t **bufp) const {
  Serialization::encode_i8(bufp, (uint8_t)m_parts);
}

void TableParts::decode(const uint8_t **bufp, size_t *remainp) {
  m_parts = Serialization::decode_i8(bufp, remainp);
}

const std::string TableParts::to_string() const {
  std::string str;
  if (m_parts == TableParts::ALL)
    str = "ALL";
  else {
    if (m_parts & PRIMARY)
      str = "PRIMARY";
    if (m_parts & VALUE_INDEX) {
      if (!str.empty())
        str += "|";
      str += "VALUE_INDEX";
    }
    if (m_parts & QUALIFIER_INDEX) {
      if (!str.empty())
        str += "|";
      str += "QUALIFIER_INDEX";
    }
  }
  return str;
}
