/* -*- c++ -*-
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

/// @file
/// Declarations for QualifiedRangeSpec and QualifiedRangeSpecManaged.
/// This file contains declarations for QualifiedRangeSpec and
/// QualifiedRangeSpecManaged, classes for holding a qualified range
/// specification, which is a range specification with the owning table
/// identifier.

#ifndef Hypertable_Lib_QualifiedRangeSpec_h
#define Hypertable_Lib_QualifiedRangeSpec_h

#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/RangeState.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <Common/ByteString.h>
#include <Common/MurmurHash.h>
#include <Common/PageArenaAllocator.h>
#include <Common/String.h>
#include <Common/Serializable.h>

#include <cassert>
#include <utility>
#include <vector>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Qualified (with table identifier) range specification
  class QualifiedRangeSpec : public Serializable {
  public:
    QualifiedRangeSpec(const TableIdentifier &tid, const RangeSpec &rs)
      : table(tid), range(rs) {}
    QualifiedRangeSpec(CharArena &arena, const QualifiedRangeSpec &other) {
      table.generation = other.table.generation;
      table.id = arena.dup(other.table.id);
      range.start_row = arena.dup(other.range.start_row);
      range.end_row = arena.dup(other.range.end_row);
    }
    QualifiedRangeSpec() { }
    virtual ~QualifiedRangeSpec() { }

    virtual bool operator<(const QualifiedRangeSpec &other) const;
    virtual bool operator==(const QualifiedRangeSpec &other) const;
    virtual bool is_root() const;

    TableIdentifier table;
    RangeSpec range;

  protected:

    /// Returns encoding version.
    /// @return Encoding version
    uint8_t encoding_version() const override;

    /// Returns internal serialized length.
    /// @return Internal serialized length

    /// @see encode_internal() for encoding format
    size_t encoded_length_internal() const override;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_internal(uint8_t **bufp) const override;

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

  };

  /// Qualified (with table identifier) range specification, with storage
  class QualifiedRangeSpecManaged : public QualifiedRangeSpec {
  public:
    QualifiedRangeSpecManaged() { }
    QualifiedRangeSpecManaged(const QualifiedRangeSpecManaged &other) { operator=(other); }
    QualifiedRangeSpecManaged(const QualifiedRangeSpec &other) { operator=(other); }
    QualifiedRangeSpecManaged(const TableIdentifier &table, const RangeSpec &range) {
      set_table_id(table);
      set_range_spec(range);
    }

    virtual ~QualifiedRangeSpecManaged() { }

    QualifiedRangeSpecManaged &operator=(const QualifiedRangeSpecManaged &other) {
      const QualifiedRangeSpec *otherp = &other;
      return operator=(*otherp);
    }

    QualifiedRangeSpecManaged &operator=(const QualifiedRangeSpec &other) {
      m_range = other.range;
      range = m_range;
      m_table = other.table;
      table = m_table;
      return *this;
    }

    void set_range_spec(const RangeSpec &rs) {
      m_range = rs;
      range = m_range;
    }

    void set_table_id(const TableIdentifier &tid) {
      m_table = tid;
      table = m_table;
    }

    bool operator<(const QualifiedRangeSpecManaged &other) const;

    friend std::ostream &operator<<(std::ostream &os,
        const QualifiedRangeSpecManaged &qualified_range);

  private:

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    TableIdentifierManaged m_table;
    RangeSpecManaged m_range;
  };


  struct LtQualifiedRangeSpecManaged {
    bool operator()(const QualifiedRangeSpecManaged *qr1,
        const QualifiedRangeSpecManaged *qr2) {
      if (qr1)
        return ((*qr1) < *qr2);
      else
        return false;
    }
  };

  class QualifiedRangeHash {
  public:
    size_t operator()(const QualifiedRangeSpec &spec) const {
      return murmurhash2(spec.range.start_row, strlen(spec.range.start_row),
                         murmurhash2(spec.range.end_row,
                                     strlen(spec.range.end_row), 0));
    }
  };

  struct QualifiedRangeEqual {
    bool
    operator()(const QualifiedRangeSpec &x, const QualifiedRangeSpec &y) const {
      return !strcmp(x.table.id, y.table.id) &&
        !strcmp(x.range.start_row, y.range.start_row) &&
        !strcmp(x.range.end_row, y.range.end_row);
    }
  };

  std::ostream &operator<<(std::ostream &os, const QualifiedRangeSpec &qualified_range);

  std::ostream &operator<<(std::ostream &os, const QualifiedRangeSpecManaged &qualified_range);

  /// @}

}


#endif // Hypertable_Lib_QualifiedRangeSpec_h
