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

#ifndef Hypertable_Lib_ColumnPredicate_h
#define Hypertable_Lib_ColumnPredicate_h

#include <Common/Serializable.h>


namespace Hypertable {
namespace Lib {

  using namespace std;

  /// @addtogroup libHypertable
  /// @{

  /**
   * Represents a column predicate (e.g. WHERE cf = "value").  
   * c-string data members are not managed so caller must handle (de)allocation.
   */
  class ColumnPredicate : public Serializable {
  public:
    enum {
      NO_OPERATION = 0,
      EXACT_MATCH  = 0x0001,
      PREFIX_MATCH = 0x0002,
      REGEX_MATCH  = 0x0004,
      VALUE_MATCH  = 0x0007,
      QUALIFIER_EXACT_MATCH  = 0x0100,
      QUALIFIER_PREFIX_MATCH = 0x0200,
      QUALIFIER_REGEX_MATCH  = 0x0400,
      QUALIFIER_MATCH        = 0x0700
    };

    ColumnPredicate() { }

    ColumnPredicate(const char *family, const char *qualifier,
                    uint32_t op, const char *v, uint32_t vlen=0)
      : column_family(family), column_qualifier(qualifier),
        value(v), value_len(vlen), operation(op) {
      if (!vlen && value)
        value_len = strlen(value);
      column_qualifier_len =
        column_qualifier ? strlen(column_qualifier) : 0;
    }

    ColumnPredicate(const uint8_t **bufp, size_t *remainp) {
      decode(bufp, remainp);
    }

    virtual ~ColumnPredicate() {}

    /// Renders predicate as HQL.
    /// @return HQL string representing predidate.
    const string render_hql() const;

    const char *column_family {};
    const char *column_qualifier {};
    const char *value {};
    uint32_t column_qualifier_len {};
    uint32_t value_len {};
    uint32_t operation {};

  private:
    
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

  std::ostream &operator<<(std::ostream &os, const ColumnPredicate &cp);

  /// @}

}}

#endif // Hypertable_Lib_ColumnPredicate_h
