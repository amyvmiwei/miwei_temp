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
/// Declarations for TranslatorTask.
/// This file contains type declarations for TranslatorTask, a class for
/// translating a task definiton statement.

#ifndef Tools_cluster_TranslatorTask_h
#define Tools_cluster_TranslatorTask_h

#include "TranslationContext.h"
#include "Translator.h"

#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Translates a task definition.
  class TranslatorTask : public Translator {
  public:
    /// Constructor.
    /// @param fname Filename of source file containing task definition
    /// @param lineno Line number within source file of task definition
    /// @param text Text of task definition
    TranslatorTask(const string &fname, size_t lineno, const string &text)
      : m_fname(fname), m_lineno(lineno), m_text(text) {};

    /// Translates a task definition.
    /// The best way to illustrate how tasks get translated is with an example.
    /// The following task definition:
    /// <pre>
    ///   task: display_hostnames roles: master,slave {
    ///     echo "before"
    ///     ssh: {
    ///       hostname
    ///     }
    ///     echo "after"
    ///   }
    /// </pre>
    /// gets translated into a bash function similar to the following:
    /// <pre>
    ///   display_hostnames () {
    ///     local _SSH_HOSTS="(${ROLE_master}) + (${ROLE_slave})"
    ///     if [ $# -gt 0 ] && [ $1 == "on" ]; then
    ///       shift
    ///       if [ $# -eq 0 ]; then
    ///         echo "Missing host specification in 'on' argument"
    ///         exit 1
    ///       else
    ///         _SSH_HOSTS="$1"
    ///         shift
    ///       fi
    ///     fi
    ///     echo "display_hostnames $@"
    ///     echo "before"
    ///     /opt/hypertable/0.9.8.1/bin/ht ssh " ${_SSH_HOSTS}" "hostname"
    ///     echo "after"
    ///   }
    /// </pre>
    /// Notice that the <code>ssh:</code> statemnts get translated to a call to
    /// the <code>ht_ssh</code> tool.  Also, the function includes logic to
    /// check for <code>on &lt;hostspec&gt;</code> initial arguments.  If found,
    /// the target hosts for <code>ssh:</code> statements becomes
    /// <code>&lt;hostspec&gt;</code> instead of the default roles specified by
    /// the <code>roles:</code> clause.
    /// @param context %Context object containing symbol tables
    /// @return translated task statement
    const string translate(TranslationContext &context) override;

  private:
    /// Source file name containing task definition
    string m_fname;
    /// Line number within #m_fname of task definition
    size_t m_lineno;
    /// Text of task definition
    string m_text;
  };

  /// @}

}}

#endif // Tools_cluster_TranslatorTask_h
