/*
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
/// Declarations for RecoveredServers.
/// This file contains declarations for RecoveredServers, a MetaLog entity class
/// for maintaining the set of proxy names of servers that have been recovered.

#ifndef Hypertable_Master_RecoveredServers_h
#define Hypertable_Master_RecoveredServers_h

#include <Hypertable/Lib/MetaLogEntity.h>

#include <set>
#include <string>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Set of recovered servers.
  /// This class maintains the set of proxy names of servers that have been
  /// recovered.  It is used to ensure that a server does not inadvertently get
  /// recovered twice.
  class RecoveredServers : public MetaLog::Entity {
  public:

    /// Default constructor.
    /// This constructor constructs an empty object.
    RecoveredServers();

    /// Constructor with MetaLog header.
    /// This constructor constructs an empty object to be populated by an MML
    /// entity.
    /// @param header MML entity header
    RecoveredServers(const MetaLog::EntityHeader &header);

    /// Destructor.
    virtual ~RecoveredServers() { }

    /// Adds proxy name to recovered servers set.
    /// @param location Proxy name of recovered server
    void add(const std::string &location);

    /// Check to see if proxy name is in recovered servers set.
    /// @param location Proxy name of server to check
    /// @return <i>true</i> if server identified by <code>location</code> is in
    /// the recovered servers set, <i>false</i> otherwise.
    bool contains(const std::string &location);

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

    /// MetaLog entity decode method.
    void decode(const uint8_t **bufp, size_t *remainp,
                uint16_t definition_version) override;

    /// Returns the name of the entity (RecoveredServers).
    /// @return Name of entity (RecoveredServers).
    const std::string name() override;

    /// Writes a textual representation of the entity to an output stream.
    /// @param os Output stream
    void display(std::ostream &os) override;

    /// Set of proxy names of recovered servers.
    std::set<std::string> m_servers;

  };

  /// Smart pointer to RecoveredServers
  typedef std::shared_ptr<RecoveredServers> RecoveredServersPtr;

  /// @}

}

#endif // Hypertable_Master_RecoveredServers_h
