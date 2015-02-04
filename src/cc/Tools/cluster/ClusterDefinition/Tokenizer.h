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
/// Declarations for Tokenizer.
/// This file contains type declarations for Tokenizer, a class for tokenizing a
/// cluster definition file.

#ifndef Tools_cluster_ClusterDefinition_Tokenizer_h
#define Tools_cluster_ClusterDefinition_Tokenizer_h

#include "Token.h"

#include <memory>
#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Splits cluster definition file into tokens.
  class Tokenizer {

  public:

    /// Constructor.
    /// Initializes #m_fname with <code>fname</code>, reads the contents of
    /// #m_fname into #m_content, and then sets #m_next pointing to the
    /// beginning of the content.
    /// @param fname Pathname of cluster definition file
    Tokenizer(const string &fname);

    /// Constructor.
    /// Initializes #m_fname with <code>fname</code>, sets #m_content to
    /// <code>content</code>, and then sets #m_next pointing to the beginning of
    /// the content.
    /// @param fname Pathname of cluster definition file
    /// @param content Content of cluster definition file
    Tokenizer(const string &fname, const string &content);

    /// Returns the directory path containing the cluster definition file.
    /// Returns the path of #m_fname up to the last '/' character, or "." if
    /// #m_fname does not contain any '/' character.
    /// @return The directory path containing the cluster definition file.    
    string dirname();

    /// Returns the next token from the cluster definition file.
    /// This reads the next token from the cluster definition file starting at
    /// #m_next.  It populates <code>token</code> with the token text, type
    /// identifier, line number of the start of the token, and creates a
    /// translator for it with a call to Token::create_translator().
    /// The #m_next and #m_line are advanced to the beginning of the next token
    /// (or EOF) and the last line of the returned token, respectively.
    /// @param token Next token filled in by this function
    /// @return <i>true</i> token was read, <i>false</i> on EOF.
    bool next(Token &token);

  private:

    /// Identifies token type of line starting at <code>base</code>.
    /// This function determines the token type (Token::Type) of the
    /// line starting at <code>base</code> and ending at <code>end</code>.
    /// @param base Pointer to beginning of next line
    /// @param end Pointer to end of line or end of content
    /// @return Token type of line pointed to by <code>base</code>
    /// @see Token::Type
    int identify_line_type(const char *base, const char *end);

    /// Accumulates the next token.
    /// This function populates <code>token</code> with the next token to
    /// be returned.  If <code>token</code> is already populated, it first
    /// check to see if the following token (identified by <code>basep</code>,
    /// <code>end</code>, and <code>type</code>) should be merged with the
    /// existing token and if so, it merges the token text with the existing
    /// token and advances <code>*basep</code> to <code>end+1</code>, otherwise
    /// it leaves <code>*basep</code> unmodified and returns.  Tokens that
    /// get merged are Token::COMMENT followed by Token::TASK, Token::ROLE
    /// followed by Token::CODE, and any a adjacent code tokens (e.g.
    /// Token::CODE, Token::FUNCTION, Token::BLANKLINE, Token::CONTROLFLOW).
    /// If <code>token</code> is populated with the token text starting at
    /// <code>*basep</code> because it was merged with an existing token
    /// or the existing token was empty, then it sets #m_next to
    /// <code>end</code>.
    /// @param basep Address of pointer to next token
    /// @param end Pointer to end of next token (either newline or EOF)
    /// @param type Type of next token (see Token::Type)
    /// @param token Reference to token object to hold next token
    /// @return <i>true</i> if <code>token</code> is ready to be returned
    bool accumulate(const char **basep, const char *end, int type, Token &token);

    /// Pathname of cluster definition file
    string m_fname;
    /// Content of cluster definition file
    string m_content;
    /// Pointer to beginning of next token to read
    const char *m_next {};
    /// Line number of end of last token read
    size_t m_line {};
  };

  /// Smart pointer to Tokenizer
  typedef shared_ptr<Tokenizer> TokenizerPtr;

  /// @}

}}

#endif // Tools_cluster_ClusterDefinition_Tokenizer_h
