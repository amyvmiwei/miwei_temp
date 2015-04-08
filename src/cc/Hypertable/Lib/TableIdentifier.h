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
/// Declarations for TableIdentifier and TableIdentifierManaged.
/// This file contains declarations for TableIdentifier and
/// TableIdentifierManaged, classes for identifying a table.

#ifndef Hypertable_Lib_TableIdentifier_h
#define Hypertable_Lib_TableIdentifier_h

#include <Common/Serializable.h>

#include <cassert>
#include <cstdlib>
#include <string>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Table identifier.
  class TableIdentifier : public Serializable {

  public:
    static const char *METADATA_ID;
    static const char *METADATA_NAME;
    static const int METADATA_ID_LENGTH;
    TableIdentifier() : id(0), generation(0) { return; }
    explicit TableIdentifier(const char *s) : id(s), generation(0) {}
    virtual ~TableIdentifier() { }

    bool operator==(const TableIdentifier &other) const;
    bool operator!=(const TableIdentifier &other) const;
    bool operator<(const TableIdentifier &other) const;

    bool is_metadata() const { return !strcmp(id, METADATA_ID); }
    bool is_system() const { return !strncmp(id, "0/", 2); }
    bool is_user() const { return strncmp(id, "0/", 2); }

    int32_t index() const {
      assert(id);
      const char *ptr = id + strlen(id);
      while (ptr > id && isdigit(*(ptr-1)))
        ptr--;
      return atoi(ptr);
    }

    const char *id;
    int64_t generation;

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

  /// Wrapper for TableIdentifier providing member storage.
  class TableIdentifierManaged : public TableIdentifier {
  public:
    TableIdentifierManaged() { id = NULL; generation = 0; }
    TableIdentifierManaged(const TableIdentifierManaged &identifier) {
      operator=(identifier);
    }
    TableIdentifierManaged(const TableIdentifier &identifier) {
      operator=(identifier);
    }
    virtual ~TableIdentifierManaged() { }
    TableIdentifierManaged &operator=(const TableIdentifierManaged &other) {
      const TableIdentifier *otherp = &other;
      return operator=(*otherp);
    }
    TableIdentifierManaged &operator=(const TableIdentifier &identifier) {
      generation = identifier.generation;

      if (identifier.id) {
        m_name = identifier.id;
        id = m_name.c_str();
      }
      else
        id = 0;
      return *this;
    }

    void set_id(const std::string &new_name) {
      m_name = new_name;
      id = m_name.c_str();
    }

    void set_id(const char *new_name) {
      m_name = new_name;
      id = m_name.c_str();
    }

    const std::string& get_id() const {
      return m_name;
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

    std::string m_name;
  };

  std::ostream &operator<<(std::ostream &os, const TableIdentifier &tid);

  /// @}
}


#endif // Hypertable_Lib_TableIdentifier_h
