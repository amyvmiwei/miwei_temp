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
/// Declarations for TokenizerTools.
/// This file contains type declarations for TokenizerTools, a namespace
/// containing utility functions to assist in cluster definition file
/// tokenization.

#ifndef Tools_cluster_ClusterDefinition_TokenizerTools_h
#define Tools_cluster_ClusterDefinition_TokenizerTools_h

#include <map>
#include <string>

namespace Hypertable {
  
  namespace ClusterDefinition {

    using namespace std;

    /// @addtogroup ClusterDefinition
    /// @{

    /// Tools to help cluster definition file tokenization.
    namespace TokenizerTools {

      /// Checks if character is valid bash identifier start character.
      /// This function checks to see if the character <code>c</code> is a valid
      /// bash identifier starting character (e.g. alphabetic or '_').
      /// @param c Character to check
      /// @return <i>true</i> if character is valid bash identifier start
      /// character, <i>false</i> otherwise.
      extern bool is_identifier_start_character(char c);

      /// Checks if character is valid bash identifier character.
      /// This function checks to see if the character <code>c</code> is a valid
      /// bash identifier character (e.g. alphanumeric or '_').
      /// @param c Character to check
      /// @return <i>true</i> if character is valid bash identifier character
      /// <i>false</i> otherwise.
      extern bool is_identifier_character(char c);

      /// Checks if name is a valid bash identifier.
      /// This function calls is_identifier_start_character() on the first
      /// character of <code>name</code> and is_identifier_character() on all
      /// subsequent characters.  If they all return <i>true</i>, then the
      /// function returns <i>true</i>, otherwise <i>false</i> is returned.
      /// @param name Name to checks
      /// @return <i>true</i> if name is a valid bash identifier, <i>false</i>
      /// otherwise.
      extern bool is_valid_identifier(const string &name);

      /// Checks if string is an ASCII number.
      /// Checks if <code>str</code> is an ASCII representation of a decimal
      /// number.  Every character in the string must be an ASCII digit
      /// character.
      /// @return <i>true</i> if string represents an ASCII number, <i>false</i>
      /// otherwise.
      extern bool is_number(const string &str);

      /// Finds a string token in a block of code.
      /// This function tries to locate the string token <code>token</code> in
      /// the block of code starting at <code>base</code> and ending at
      /// <code>end</code>.  It skips over comments, quoted strings, and
      /// backtick surrounded command substitution text.  If the token is found,
      /// then <code>*offsetp</code> is set to the offset of the token relative
      /// to <code>base</code>.
      /// @param token String token to search for
      /// @param base Beginning of code block to search
      /// @param end End of code block to search
      /// @param offsetp Address of offset variable filled in with token offset,
      /// if found
      /// @return <i>true</i> if token was found, <i>false</i> if not
      extern bool find_token(const string &token, const char *base,
                             const char *end, size_t *offsetp);

      /// Finds next bash identifier token in a block of text.
      /// This function finds the first valid bash identifier token in the code
      /// block pointed to by <code>base</code>.  It skips over comments, quoted
      /// strings, and backtick surrounded command substitution text.  If the
      /// token is found, then <code>*offsetp</code> is set to the offset of the
      /// token relative to <code>base</code> and <code>*lengthp</code> is set
      /// to the token length.
      /// @param base Beginning of code block to search
      /// @param offsetp Address of offset variable filled in with token offset,
      /// if found
      /// @param lengthp Address of length variable filled in with token length,
      /// if found
      /// @return <i>true</i> if token was found, <i>false</i> if not
      extern bool find_next_token(const char *base, size_t *offsetp,
                                  size_t *lengthp);

      /// Skips to end of block or quoted string in code.
      /// This function expects <code>base</code> to point to one of four
      /// characters ('"', '\'', '`', or '{') upon entry.  Depending on the
      /// starting character, it will skip to the matching end character.
      /// If the starting character is '{' it skips over quoted strings and
      /// comments and nested blocks to find the matching block close character
      /// '}'.  The parameter <code>*endp</code> is set to the end of the quoted
      /// string or code block, if found.  If <code>linep<code> is not null, it
      /// is incremented for each newline character encountered.
      /// @param base Pointer to beginning of quoted string or block
      /// @param endp Address of pointer to hold pointer to end of string or
      /// block
      /// @param linep Address of newline counter
      /// @param <i>true</i> if end of string or block was found, <i>false</i>
      /// otherwise.
      extern bool find_end_char(const char *base, const char **endp,
                                size_t *linep=nullptr);

      /// Skips over bash control flow statement.
      /// This method skips over a bash control flow statement pointed to by
      /// <code>*basep</code>.  The following control flow statements are
      /// handled:
      /// <pre>
      /// if ... fi
      /// for ... done
      /// until ... done
      /// while ... done
      /// case ... esac
      /// </pre>
      /// The function handles nested control flow statements.  If the end of
      /// the control flow statement is found, <code>*basep</code> is set to
      /// point to the character immediately following the control flow
      /// statement termination token.
      /// @param basep Address of pointer to beginning of control flow statement
      /// @return <i>true</i> if end of control flow statement was found,
      /// <i>false</i> otherwise.
      extern bool skip_control_flow_statement(const char **basep);

      /// Counts number of newlines in text.
      /// Counts the number of newline characters in the block of text starting
      /// at <code>base</code> and ending at <code>end</code> (not inclusive).
      /// @param base Start of text
      /// @param end End of text (not inclusive)
      /// @return Number of newline characters in text
      extern size_t count_newlines(const char *base, const char *end);

      /// Skips to next newline character in text.
      /// This function advances <code>*endp</code> to the next newline
      /// character found or the terminating '\0' character if no newline
      /// character is found.
      /// @param endp Address of text pointer (advanced by call)
      /// @return <i>true</i> if newline character was found, <i>false</i> if
      /// not.
      extern bool skip_to_newline(const char **endp);

      /// Does variable sustitution in a block of text.
      /// This function copies the input text <code>input</code> to
      /// <code>output</code>, performing variable substitution.  The function
      /// searches the input text for strings of the form <code>${name}</code>
      /// of <code>$name</code> and if <code>name</code> is found in
      /// <code>vmap</code>, the string is replaced with the mapped value for
      /// <code>name</code> in <code>vmap</code>.  If the variable reference is
      /// escaped (i.e. <code>\$name</code>) then it is skipped.
      /// @param intput Input text
      /// @param output Output text with variables substituted
      /// @param vmap Variable map mapping name -> sustitution text
      /// @return <i>true</i> if any variable sustitutions were performed,
      /// <i>false</i> otherwise.
      extern bool substitute_variables(const string &input, string &output,
                                       map<string, string> &vmap);

    }

    /// @}
  }
}

#endif // Tools_cluster_ClusterDefinition_TokenizerTools_h
