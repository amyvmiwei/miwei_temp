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

#ifndef HYPERTABLE_RANGEMOVESPEC_H
#define HYPERTABLE_RANGEMOVESPEC_H

#include "Common/ReferenceCount.h"

#include "Types.h"

namespace Hypertable {

/**
 * Represents a scan predicate.
 */
  class RangeMoveSpec : public ReferenceCount {
  public:

    RangeMoveSpec() { clear(); }
    RangeMoveSpec(const char *src, const char *dest, const char* table_id,
                  const char *start_row, const char *end_row) {
      clear();
      source_location = src;
      dest_location = dest;
      table.set_id(table_id);
      range.set_start_row(start_row);
      range.set_end_row(end_row);
    }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    void clear() {
      table.id = 0;
      table.generation = 0;
      range.start_row = 0;
      range.end_row = 0;
      source_location = "";
      dest_location = "";
      error = 0;
      complete = false;
    }

    TableIdentifierManaged table;
    RangeSpecManaged range;
    String source_location;
    String dest_location;
    int32_t error;
    bool complete;
  };
  typedef intrusive_ptr<RangeMoveSpec> RangeMoveSpecPtr;

  std::ostream &operator<<(std::ostream &os, const RangeMoveSpec &move_spec);

} // namespace Hypertable

#endif // HYPERTABLE_RANGEMOVESPEC_H
