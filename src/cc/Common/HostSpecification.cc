/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Definitions for HostSpecification.
/// This file contains type definitions for HostSpecification, a class to
/// convert a host specification pattern into a list of host names

#include <Common/Compat.h>

#include "HostSpecification.h"

#include <Common/Error.h>
#include <Common/Logger.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <set>
#include <stack>
#include <string>

using namespace Hypertable;
using namespace std;

namespace {

  /// Hostname with pattern components.
  class ClusterHostname {
  public:
    /// Constructor.
    /// @param name Full host name
    /// @param prefix Prefix of host name *before* numeric component
    /// @param num Numeric component
    /// @param postfix Postfix of host name *after* numeric component
    ClusterHostname(const string &name, const string &prefix,
                    int num, const string &postfix)
      : name(name), prefix(prefix), postfix(postfix), num(num) { }
    /// Constructor.
    /// Parses <code>name</code> to extract #prefix, #postfix, and #num.  The
    /// numeric component is assumed to be the first string of digits found in
    /// <code>name</code>.
    /// @param name Full host name
    ClusterHostname(const string &name) : name(name) {
      const char *base = name.c_str();
      const char *ptr = base;
      while (*ptr && !isdigit(*ptr))
        ptr++;
      prefix.append(base, (size_t)(ptr-base));
      if (*ptr) {
        char *end;
        num = (int)strtol(ptr, &end, 10);
        postfix.append(end);
      }
    }
    /// Full hostname
    const string name;
    /// Prefix of host name *before* numeric component
    string prefix;
    /// Postfix of host name *after* numeric component
    string postfix;
    /// Numeric component
    int num {-1};
  };

  /// Less-than operator.
  /// If both <code>ch1</code> and <code>ch2</code> look like they're part of
  /// the same pattern (e.g. prefix and postfix lengths match and there is a
  /// numeric component), then the comparison is performed
  /// component-by-component with the numeric component compared as integers.
  /// Otherwise, the full host names are compared lexicographically.
  /// @param ch1 Left hand side of comparison
  /// @param ch2 Right hand side of comparison
  inline bool operator<(const ClusterHostname &ch1, const ClusterHostname &ch2) {
    if (ch1.prefix.length() == ch2.prefix.length() &&
        ch1.postfix.length() == ch2.postfix.length() &&
        ch1.num != -1 && ch2.num != -1) {
      int cmp = ch1.prefix.compare(ch2.prefix);
      if (cmp)
        return cmp < 0;
      if (ch1.num != ch2.num)
        return ch1.num < ch2.num;
      cmp = ch1.postfix.compare(ch2.postfix);
      return cmp < 0;
    }
    return ch1.name.compare(ch2.name) < 0;
  }

  /// Context for managing nested expressions.
  class Frame {
  public:
    /// Last operator seen was '-'
    bool subtract {};
    /// Set of host names
    set<ClusterHostname> hosts;
  };

  /// Smart pointer to Frame
  typedef std::shared_ptr<Frame> FramePtr;

  /// Input stream token.
  /// Abstraction for input stream token to facilitate host specification
  /// parsing.  A host pattern is represented by a #value of 0 and will have
  /// #hosts populated with the expansion of the pattern.
  class Token {
  public:
    /// Token value (one of: 0 '(' '(' '+' '-' ',')
    char value {};
    /// Set of hosts represented by host pattern token
    set<ClusterHostname> hosts;
  };

  /// Tokenizes a host specification pattern.
  class Tokenizer {
  public:
    /// Constructor.
    /// Sets #m_ptr to beginning of spec
    /// @param spec Host specification pattern
    Tokenizer(const std::string &spec) : m_spec(spec) {
      m_ptr = m_spec.c_str();
    }
    /// Gets the next token.
    /// @param token Reference to token
    /// @returns <i>true</i> if <code>token</code> filled in with valid token,
    /// <i>false</i> on EOS
    /// @throws Exception with code set to Error::BAD_FORMAT on bad input
    bool next(Token &token);
    /// Gets character offset of current position.
    /// @return Offset from beginning of spec of current position.
    int current_position() {
      return (m_ptr - m_spec.c_str()) + 1;
    }
  private:
    /// Skips over whitespace.
    /// Advances #m_ptr to next non-whitespace character
    void skip_whitespace() {
      while (*m_ptr && isspace(*m_ptr))
        m_ptr++;
    }
    /// Saves current position.
    /// Sets #m_saved_ptr to current value of #m_ptr.
    void save_position() { m_saved_ptr = m_ptr; }
    /// Gets character offset of saved position.
    /// @return Offset from beginning of spec of saved position.
    int saved_position() {
      return (m_saved_ptr - m_spec.c_str()) + 1;
    }
    /// Returns human-readable string representing char at current position.
    const char *current_character() {
      int index {};
      if (*m_ptr == '\'' || *m_ptr == '\"' || *m_ptr == '\?' ||
               *m_ptr == '\\' || *m_ptr == '\a' || *m_ptr == '\b' || 
               *m_ptr == '\f' || *m_ptr == '\n' || *m_ptr == '\r' || 
               *m_ptr == '\t' || *m_ptr == '\v') {
        character_buf[index++] = '\'';
        character_buf[index++] = '\\';
        if (*m_ptr == '\'' || *m_ptr == '\"' || *m_ptr == '\?' ||
            *m_ptr == '\\')
          character_buf[index++] = *m_ptr;
        else if (*m_ptr == '\a')
          character_buf[index++] = 'a';
        else if (*m_ptr == '\b')
          character_buf[index++] = 'b';
        else if (*m_ptr == '\f')
          character_buf[index++] = 'f';
        else if (*m_ptr == '\n')
          character_buf[index++] = 'n';
        else if (*m_ptr == '\r')
          character_buf[index++] = 'r';
        else if (*m_ptr == '\t')
          character_buf[index++] = 't';
        else if (*m_ptr == '\v')
          character_buf[index++] = 'v';
        character_buf[index++] = '\'';
        character_buf[index++] = '\0';
      }
      else if (*m_ptr >= 32) {
        character_buf[index++] = '\'';
        character_buf[index++] = *m_ptr;
        character_buf[index++] = '\'';
        character_buf[index++] = '\0';
      }
      else
        sprintf(character_buf, "0x%x", *m_ptr);
      return character_buf;
    }
    
    std::string extract_numeric_range(int *beginp, int *endp);
    /// Host specification
    std::string m_spec {};
    /// Current position within #m_spec
    const char *m_ptr {};
    /// Saved position within #m_spec
    const char *m_saved_ptr {};
    /// Temporary buffer to hold value returned by current_character()
    char character_buf[8];
  };

  bool Tokenizer::next(Token &token) {

    skip_whitespace();

    if (*m_ptr == 0)
      return false;

    if (*m_ptr == '(' || *m_ptr == ')' || *m_ptr == '+' ||
        *m_ptr == ',' || *m_ptr == '-') {
      token.value = *m_ptr++;
      return true;
    }

    if (!isalnum(*m_ptr) && *m_ptr == ']')
      HT_THROWF(Error::BAD_FORMAT, "Invalid character encountered at position %d",
                current_position());

    string prefix {};
    string host_pattern {};
    int postfix_len {};
    int range_begin {};
    int range_end {};

    if (isalnum(*m_ptr)) {
      const char *base = m_ptr;
      while (*m_ptr != 0 && (isalnum(*m_ptr) || *m_ptr == '.' || *m_ptr == '-'))
        m_ptr++;
      prefix = string(base, m_ptr-base);
    }

    if (*m_ptr == '[') {
      host_pattern = prefix;
      host_pattern += extract_numeric_range(&range_begin, &range_end);
      if (isalnum(*m_ptr) || *m_ptr == '.' || *m_ptr == '-') {
        const char *base = m_ptr;
        while (*m_ptr != 0 && (isalnum(*m_ptr) || *m_ptr == '.' || *m_ptr == '-'))
          m_ptr++;
        postfix_len = m_ptr - base;
        host_pattern.append(base, postfix_len);
      }
    }

    if (*m_ptr != 0 && !isspace(*m_ptr) && *m_ptr != '(' && *m_ptr != ')' &&
        *m_ptr != '+' && *m_ptr != ',' && *m_ptr != '-')
      HT_THROWF(Error::BAD_FORMAT, "Invalid character %s encountered at position %d",
                current_character(), current_position());

    token.hosts.clear();
    token.value = 0;
    if (!host_pattern.empty()) {
      for (int i= range_begin; i<= range_end; i++) {
        string name = format(host_pattern.c_str(), i);
        string postfix = name.substr( name.length() - postfix_len);
        token.hosts.insert( ClusterHostname(name, prefix, i, postfix) );
      }
    }
    else
      token.hosts.insert( ClusterHostname(prefix) );

    return true;
  }

  string Tokenizer::extract_numeric_range(int *beginp, int *endp) {

    save_position();

    HT_ASSERT(*m_ptr == '[');
    m_ptr++;

    skip_whitespace();

    // sanity check to make sure the next thing we see is a number
    if (*m_ptr == '\0')
      HT_THROWF(Error::BAD_FORMAT,
                "Truncated range pattern at position %d",
                current_position());
    else if (!isdigit(*m_ptr))
      HT_THROWF(Error::BAD_FORMAT,
                "Invalid character %s in range pattern at position %d",
                current_character(), current_position());
    bool range_begin_leading_zero = *m_ptr == '0';

    // skip over begin number
    const char *base = m_ptr;
    while (*m_ptr && isdigit(*m_ptr))
      m_ptr++;
    size_t range_begin_width = m_ptr - base;
    *beginp = atoi(base);

    skip_whitespace();

    // Verify the range contains '-' separation character
    if (*m_ptr != '-') {
      string msg;
      if (*m_ptr == ']')
        msg = format("Missing range separation character '-' at position %d",
                     current_position());
      else if (*m_ptr == '\0')
        msg = format("Truncated range pattern at position %d",
                     current_position());
      else
        msg = format("Invalid character %s in range pattern at position %d",
                     current_character(), current_position());
      HT_THROW(Error::BAD_FORMAT, msg);
    }
                  
    m_ptr++;

    skip_whitespace();

    // sanity check to make sure the next thing we see is a number
    if (*m_ptr == '\0')
      HT_THROWF(Error::BAD_FORMAT,
                "Truncated range pattern at position %d",
                current_position());
    else if (!isdigit(*m_ptr))
      HT_THROWF(Error::BAD_FORMAT,
                "Invalid character %s in range pattern at position %d",
                current_character(), current_position());
    bool range_end_leading_zero = *m_ptr == '0';

    // skip over end number
    base = m_ptr;
    while (*m_ptr && isdigit(*m_ptr))
      m_ptr++;
    size_t range_end_width = m_ptr - base;
    *endp = atoi(base);

    skip_whitespace();

    if (*m_ptr == '\0')
      HT_THROWF(Error::BAD_FORMAT,
                "Truncated range pattern at position %d",
                current_position());
    else if (*m_ptr != ']')
      HT_THROWF(Error::BAD_FORMAT,
                "Invalid character %s in range pattern at position %d",
                current_character(), current_position());
    else if (range_begin_width != range_end_width &&
        (range_begin_leading_zero || range_end_leading_zero))
      HT_THROWF(Error::BAD_FORMAT, "Fixed-width numeric range specifiers must be of equal length (position %d)",
                saved_position());
    m_ptr++;
      
    if (range_begin_width != range_end_width)
      return "%d";

    return format("%%0%dd", (int)range_begin_width);
  
  }
}

HostSpecification::operator std::vector<std::string>() {
  char last_token {(char)255};
  stack<FramePtr> frame_stack;

  frame_stack.push(std::make_shared<Frame>());

  Token token;
  Tokenizer tokenizer(m_spec);
  while (tokenizer.next(token)) {
    if (token.value == 0) {
      if (frame_stack.top()->subtract) {
        for (auto & host : token.hosts)
          frame_stack.top()->hosts.erase(host);
      }
      else {
        for (auto & host : token.hosts)
          frame_stack.top()->hosts.insert(host);
      }
    }
    else if (token.value == '(')
      frame_stack.push(std::make_shared<Frame>());
    else if (token.value == ')') {
      if (last_token == '+' || last_token == ',' || last_token == '-')
        HT_THROWF(Error::BAD_FORMAT, "Missing operand for '%c' operator at position %d",
                  last_token, tokenizer.current_position() - 1);
      if (frame_stack.size() == 1)
        HT_THROW(Error::BAD_FORMAT, "Mis-matched parenthesis");
      FramePtr top = frame_stack.top();
      frame_stack.pop();
      if (frame_stack.top()->subtract) {
        for (auto & host : top->hosts)
          frame_stack.top()->hosts.erase(host);
      }
      else {
        for (auto & host : top->hosts)
          frame_stack.top()->hosts.insert(host);
      }
    }
    else if (token.value == '+' || token.value == ',') {
      if (last_token != 0 && last_token != ')')
        HT_THROWF(Error::BAD_FORMAT, "Missing operand for '%c' operator at position %d",
                  token.value, tokenizer.current_position() - 1);
      frame_stack.top()->subtract = false;
    }
    else if (token.value == '-') {
      if (last_token != 0 && last_token != ')')
        HT_THROWF(Error::BAD_FORMAT, "Missing operand for '-' operator at position %d",
                  tokenizer.current_position() - 1);
      frame_stack.top()->subtract = true;
    }
    else
      HT_THROWF(Error::BAD_FORMAT, "Unrecognized character '%c'", token.value);
    last_token = token.value;
  }

  if (frame_stack.size() != 1)
    HT_THROW(Error::BAD_FORMAT, "Mis-matched parenthesis");

  if (last_token == '+' || last_token == ',' || last_token == '-')
    HT_THROWF(Error::BAD_FORMAT, "Missing operand for '%c' operator at position %d",
              last_token, tokenizer.current_position() - 1);

  vector<string> hosts;

  hosts.reserve(frame_stack.top()->hosts.size());
  for (auto & ch : frame_stack.top()->hosts)
    hosts.push_back(ch.name);

  return move(hosts);
}
