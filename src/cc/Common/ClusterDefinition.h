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
/// Declarations for ClusterDefinition.
/// This file contains type declarations for ClusterDefinition, a class that
/// caches and provides access to the information in a cluster definition file.

#ifndef Common_ClusterDefinition_ClusterDefinition_h
#define Common_ClusterDefinition_ClusterDefinition_h

#include <ctime>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace Hypertable {
namespace ClusterDefinition {

  /// @addtogroup Common
  /// @{

  /// Represents a cluster definition.
  class ClusterDefinition {

  public:

    /// Constructor.
    /// @param fname Pathname of cluster definition file
    ClusterDefinition(const std::string &fname) : m_fname(fname) {};

    /// Get list of members of a role
    /// This method first refreshes its view of the cluster definition by
    /// reloading the cluster definition file, #m_fname, if it has not been
    /// modified since the last refresh.  If a role by the name of
    /// <code>role</code> exists in the cluster definition, <code>members</code>
    /// is populated with the corresponding members.  If the
    /// <code>generation</code> parameter is not nullptr, then it is set to the
    /// generation number of the cluster definition file (last modification
    /// time).  If the cluster definition file, #m_fname, does not exist,
    /// <code>members</code> is cleared and the <code>generation</code> is set
    /// to 0.
    /// @param role Role name
    /// @param members Reference to vector to hold members
    /// @param generation Address of variable populated with definition generation number
    void get_role_members(const std::string &role,
                          std::vector<std::string> &members,
                          int64_t *generation = nullptr);

  private:

    /// Load cluster definition from file
    /// Loads the cluster definition from the file #m_fname, if the file was
    /// modified since the last time it was loaded.
    void load_file();

    /// Parses a role definition statement
    /// Parses the role definition statement contained in <code>text</code> and
    /// adds the information to #m_roles.
    /// @param text
    void add_role(const std::string &text);

    /// Mutex for serializing access to members
    std::mutex m_mutex;

    /// Name of cluster definition file
    std::string m_fname;

    /// Last modification time of cluster definition file
    time_t m_last_mtime {};

    /// Map from role names to members
    std::unordered_map<std::string, std::vector<std::string>> m_roles;

  };

  /// @}

}}

#endif // Common_ClusterDefinition_ClusterDefinition_h
