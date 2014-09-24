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
/// Declarations for TranslatorCode.
/// This file contains type declarations for TranslatorCode, a class for
/// translating a code block.

#ifndef Tools_cluster_TranslatorCode_h
#define Tools_cluster_TranslatorCode_h

#include "TranslationContext.h"
#include "Translator.h"

#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Translates a code block.
  class TranslatorCode : public Translator {
  public:
    /// Constructor.
    /// @param fname Filename of source file
    /// @param lineno Starting offset within source file of code block text
    /// @param text Text of code block
    TranslatorCode(const string &fname, size_t lineno, const string &text)
      : m_fname(fname), m_lineno(lineno), m_text(text) {};

    /// Translates a code block.
    /// This method does no translation and passes the code block text straight
    /// through untranslated.
    /// @return Unmodified code block text
    const string translate(TranslationContext &context) override;

  private:
    /// Source file name containing code block
    string m_fname;
    /// Starting offset within #m_fname of code block
    size_t m_lineno;
    /// Text of code block
    string m_text;
  };

  /// @}

}}

#endif // Tools_cluster_TranslatorCode_h
