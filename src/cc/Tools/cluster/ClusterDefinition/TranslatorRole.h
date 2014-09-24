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
/// Declarations for TranslatorRole.
/// This file contains type declarations for TranslatorRole, a class for
/// translating a role definiton statement.

#ifndef Tools_cluster_TranslatorRole_h
#define Tools_cluster_TranslatorRole_h

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
    /// ????
    /// @return 
    const string translate(TranslationContext &context) override;

  private:
    /// Source file name containing input text
    string m_fname;
    /// Starting offset within #m_fname of input text
    size_t m_lineno;
    /// Input text
    string m_text;
  };

  /// @}

}}

#endif // Tools_cluster_TranslatorRole_h
