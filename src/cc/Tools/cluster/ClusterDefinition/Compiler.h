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
      /// Initializes #m_definition_file with <code>fname</code> and initializes
      /// #m_output_script with the pathname of the output script constructed as
      /// follows:
      /// <pre>
      /// ${HOME} + "/.cluster/" + m_definition_file + ".sh"
      /// </pre>
      /// It then calls compilation_needed() to determine if the script needs to
      /// be rebuilt and, if so, calls make().
      /// @param fname Pathname of cluster definition file
      Compiler(const string &fname);

      /// Returns pathname of output script
      /// @return Pathname of output script
      string output_script() { return m_output_script; }

    private:

      /// Determines if script needs to be re-built.
      /// This function returns <i>true</i> if 1) the output script
      /// (#m_output_script) does not exist, or 2) the modification timestamp of
      /// the definition file (#m_definition_file) or any of its dependencies
      /// (included files) is greater than the modification timestamp of the
      /// output script, or 3) The Hypertable version string listed in the
      /// comment header of the output script does not match the current
      /// Hypertable version string.  The dependencies are listed in a comment
      /// header of the output script.
      /// @return <i>true</i> if script needs to be compiled, <i>false</i>
      /// otherwise
      bool compilation_needed();

      /// Compiles the definition file into a task/command execution script.
      /// This function compiles #m_definition_file into a task/command
      /// execution script.
      void make();

      /// Cluster definition file
      string m_definition_file;

      /// Output script
      string m_output_script;

    };

    /// @}
  }
}

#endif // Tools_cluster_ClusterDefinition_Compiler_h
