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
    /// ????
    /// @return 
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
