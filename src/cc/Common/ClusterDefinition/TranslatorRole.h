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
/// Declarations for TranslatorRole.
/// This file contains type declarations for TranslatorRole, a class for
/// translating a role definiton statement.

#ifndef Common_TranslatorRole_h
#define Common_TranslatorRole_h

#include "TranslationContext.h"
#include "Translator.h"

#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Translates a role definition.
  class TranslatorRole : public Translator {
  public:
    /// Constructor.
    /// @param fname Filename of source file
    /// @param lineno Starting offset within source file of input text
    /// @param text Input text
    TranslatorRole(const string &fname, size_t lineno, const string &text)
      : m_fname(fname), m_lineno(lineno), m_text(text) {};

    /// Translates a role definition.
    /// This function translates role definitions of the form:
    /// <pre>
    /// role: slave test[00-99]
    /// </pre>
    /// To a bash variable definition of the form:
    /// <pre>
    /// ROLE_slave="test[00-99]"
    /// </pre>
    /// As a side effect, it inserts the role name into
    /// <code>context.roles</code> and inserts the variable name representing
    /// the role (e.g. <code>ROLE_name</code>) into the
    /// <code>context.symbols</code> map, mapping it to the role value (e.g.
    /// <code>test[00-99]</code> in the above example).
    /// @param context Context object containing symbol tables
    /// @return Translated role statement.
    const string translate(TranslationContext &context) override;

  private:
    /// Source file name containing input text
    string m_fname;
    /// Starting offset within #m_fname of input text
    size_t m_lineno;
    /// Input text containing role definition statememt
    string m_text;
  };

  /// @}

}}

#endif // Common_TranslatorRole_h
