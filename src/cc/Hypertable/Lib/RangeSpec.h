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
/// Declarations for RangeSpec and RangeSpecManaged.
/// This file contains declarations for RangeSpec and RangeSpecManaged, classes
/// for identifying a range.

#ifndef Hypertable_Lib_RangeSpec_h
#define Hypertable_Lib_RangeSpec_h

#include <Common/Serializable.h>

#include <string>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Range specification.
  class RangeSpec : public Serializable {
  public:
    enum Type {
      ROOT=0,
      METADATA=1,
      SYSTEM=2,
      USER=3,
      UNKNOWN=4
    };
    static std::string type_str(int type);
    RangeSpec() : start_row(0), end_row(0) { return; }
    RangeSpec(const char *start, const char *end)
        : start_row(start), end_row(end) {}
    RangeSpec(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }
    virtual ~RangeSpec() { }
    bool operator==(const RangeSpec &other) const;
    bool operator!=(const RangeSpec &other) const;
    bool operator<(const RangeSpec &other) const;

    const char *start_row;
    const char *end_row;

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

  /// Wrapper for RangeSpec providing member storage.
  class RangeSpecManaged : public RangeSpec {
  public:
    RangeSpecManaged() { start_row = end_row = 0; }
    RangeSpecManaged(const RangeSpecManaged &range) { operator=(range); }
    RangeSpecManaged(const RangeSpec &range) { operator=(range); }
    virtual ~RangeSpecManaged() { }

    RangeSpecManaged &operator=(const RangeSpecManaged &other) {
      const RangeSpec *otherp = &other;
      return operator=(*otherp);
    }
    RangeSpecManaged &operator=(const RangeSpec &range) {
      if (range.start_row)
        set_start_row(range.start_row);
      else
        start_row = 0;

      if (range.end_row)
        set_end_row(range.end_row);
      else
        end_row = 0;
      return *this;
    }
    void set_start_row(const std::string &s) {
      m_start = s;
      start_row = m_start.c_str();
    }
    void set_end_row(const std::string &e) {
      m_end = e;
      end_row = m_end.c_str();
    }

  private:

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    std::string m_start;
    std::string m_end;
  };

  std::ostream &operator<<(std::ostream &os, const RangeSpec &range);

  /// @}
}


#endif // Hypertable_Lib_RangeSpec_h
