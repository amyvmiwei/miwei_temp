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

      /// Compiles the definition file into a task execution script.
      /// This function compiles #m_definition_file into a task execution
      /// script and writes it to the #m_output_script file and then does a
      /// <code>chmod 755</code> on that file.  The script is assembled as
      /// follows.  It tokenizes the input definition file with Tokenizer and
      /// translates each token using its corresponding translator
      /// (TranslatorCode, TranslatorRole, TranslatorTask, TranslatorVariable).
      /// After the translated tokens are written, a couple of built-in
      /// functions are generated:
      /// <table>
      /// <tr>
      /// <th> Function </th>
      /// <th> Description </th>
      /// </tr>
      /// <tr>
      /// <td><code> show_variables </code></td>
      /// <td> Displays all of the global variables and role
      ///      defintion variables </td>
      /// </tr>
      /// <tr>
      /// <td><code> with </code></td>
      /// <td> Runs a shell command on any of the roles </td>
      /// </tr>
      /// </table>
      /// After the built-in functions, argument parsing logic is added to
      /// handle the command line arguments:
      /// <table>
      /// <tr>
      /// <th> Argument </th>
      /// <th> Description </th>
      /// </tr>
      /// <tr>
      /// <td><code> -T, --tasks </code></td>
      /// <td> Displays all tasks with a short description </td>
      /// </tr>
      /// <tr>
      /// <td><code> -e &lt;task&gt;, --explain &lt;task&gt; </code></td>
      /// <td> Displays a detailed description of <code>&lt;task&gt;</code> </td>
      /// </tr>
      /// <tr>
      /// <td><code> &lt;function&gt; </code></td>
      /// <td> Executes <code>&lt;function&gt;</code> which is one of the task
      /// functions or or a built-in function (<code>show_variables</code>
      /// or <code>with</code>) </td>
      /// </tr>
      /// </table>
      /// A comment header is written to the output script that looks something
      /// like this:
      /// <pre>
      ///   # version: 0.9.8.1 (v0.9.8.1-35-g20a3c08)
      ///   # dependency: /home/doug/core.tasks
      ///   # dependency: /home/doug/test.tasks
      /// </pre>
      /// There is one <code>dependency:</code> line for each file that is
      /// included with an <code>include:</code> directive.  These header lines
      /// are used by the compilation_needed() function.
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
