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
/// Declarations for Tokenizer.
/// This file contains type declarations for Tokenizer, a class for tokenizing a
/// cluster definition file.

#ifndef Tools_cluster_ClusterDefinition_Tokenizer_h
#define Tools_cluster_ClusterDefinition_Tokenizer_h

#include <memory>
#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  class Tokenizer {

  public:

    class Token {
    public:
      enum {
        NONE=0,
        INCLUDE,
        VARIABLE,
        ROLE,
        TASK,
        FUNCTION,
        COMMENT,
        CODE,
        BLANKLINE
      };
      static const char *type_to_text(int type);
      void clear() { type=NONE; text.clear(); line=0; fname.clear(); }
      int type {};
      string text;
      size_t line {};
      string fname;
    };

    Tokenizer(const string &fname);

    Tokenizer(const string &fname, const string &content);

    string dirname();

    bool next(Token &token);

  private:

    int identify_line_type(const char *base, const char *end);

    bool accumulate(const char **basep, const char *endp, int type, Token &token);

    string m_fname;
    string m_content;
    const char *m_next {};
    size_t m_line {};
  };

  /// Smart pointer to Tokenizer
  typedef shared_ptr<Tokenizer> TokenizerPtr;

  /// @}

}}

#endif // Tools_cluster_ClusterDefinition_Tokenizer_h
