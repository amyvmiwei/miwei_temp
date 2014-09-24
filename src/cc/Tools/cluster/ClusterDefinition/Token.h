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
/// Declarations for Token.
/// This file contains type declarations for Token, a class representing a
/// cluster definition file token.

#ifndef Tools_cluster_ClusterDefinition_Token_h
#define Tools_cluster_ClusterDefinition_Token_h

#include "Translator.h"

#include <memory>
#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Cluster definition file token.
  class Token {
  public:
    /// Enumeration for token types.
    enum {
      NONE=0,   //!< none
      INCLUDE,  //!< include: statement
      VARIABLE, //!< variable defintion      
      ROLE,     //!< role: statement      
      TASK,     //!< task: statement
      FUNCTION, //!< function defintion
      COMMENT,  //!< comment block
      CODE,     //!< code block
      BLANKLINE //!< blank line
    };

    /// Clears token state
    void clear() { type=NONE; text.clear(); line=0; fname.clear(); }

    /// Creates a translator for the token.
    /// Creates an appropriate translator for the token and sets #translator to
    /// point to it.
    void create_translator();

    /// Returns human-readable string of token type.
    /// @param type Token type
    /// @return Human-readable string of token type.
    static const char *type_to_text(int type);

    /// Token type
    int type {NONE};
    /// Translator object for token text
    TranslatorPtr translator;
    /// Token text
    string text;
    /// Starting line number of token
    size_t line {};
    /// Pathname of file from which token was extracted
    string fname;
  };

  /// @}

}}

#endif // Tools_cluster_ClusterDefinition_Token_h
