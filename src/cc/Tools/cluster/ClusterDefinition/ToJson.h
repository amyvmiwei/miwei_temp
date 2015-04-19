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
/// Declarations for ToJson.
/// This file contains type declarations for ToJson, a class for
/// converting a cluster definition file into JSON.

#ifndef Tools_cluster_ClusterDefinition_ToJson_h
#define Tools_cluster_ClusterDefinition_ToJson_h

#include <string>

namespace Hypertable {

  /// Cluster definition file translation definitions
  namespace ClusterDefinition {

    using namespace std;

    /// @defgroup ClusterDefinition ClusterDefinition
    /// @ingroup cluster
    /// Classes for compiling a cluster definition file.
    /// @{

    /// Compiles a cluster definition file into an executable bash script.
    class ToJson {

    public:
      /// Constructor.
      /// Initializes #m_definition_file with <code>fname</code> and initializes
      /// #m_output_script with the pathname of the output script constructed as
      /// follows:
      /// <pre>
      /// ${HOME} + "/.cluster/" + m_definition_file + ".sh"
      /// </pre>
      /// It then calls compilation_needed() to determine if the script needs to
      /// be rebuilt and, if so, calls make().
      /// @param fname Pathname of cluster definition file
      ToJson(const string &fname);

      /// Returns pathname of output script
      /// @return Pathname of output script
      string str() { return m_str; }

    private:

      /// Cluster definition file
      string m_definition_file;

      /// Output JSON string
      string m_str;

    };

    /// @}
  }
}

#endif // Tools_cluster_ClusterDefinition_ToJson_h
