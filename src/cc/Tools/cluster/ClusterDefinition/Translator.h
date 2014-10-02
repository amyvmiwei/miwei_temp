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
/// Declarations for Translator.
/// This file contains type declarations for Translator, an abstract base class
/// for classes that translate cluster definition entities
/// (e.g. role, task, ...)

#ifndef Tools_cluster_Translator_h
#define Tools_cluster_Translator_h

#include "TranslationContext.h"

#include <memory>
#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Abstract base class for translators.
  class Translator {
  public:
    /// Translates token
    /// This method is called to translate a token.
    /// @param context Context object containing symbol tables
    /// @return Translated token text
    virtual const string translate(TranslationContext &context) = 0;
  };

  /// Smart pointer to Translator
  typedef shared_ptr<Translator> TranslatorPtr;

  /// @}
}}

#endif // Tools_cluster_Translator_h
