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
/// Declarations for Compiler.
/// This file contains type declarations for Compiler, a class for compiling a
/// cluster definition file into an executable bash script.

#ifndef Tools_cluster_ClusterDefinition_Compiler_h
#define Tools_cluster_ClusterDefinition_Compiler_h

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
    class Compiler {

    public:
      /// Constructor.
      /// @param fname Pathname of cluster definition file
      Compiler(const string &fname);

    private:

      bool compilation_needed();

      void make();

      string m_definition_file;

      string m_definition_script;

    };

    /// @}
  }
}

#endif // Tools_cluster_ClusterDefinition_Compiler_h
