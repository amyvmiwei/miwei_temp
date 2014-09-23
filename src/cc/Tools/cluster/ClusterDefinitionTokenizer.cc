/*
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
#include <Common/Compat.h>

#include "ClusterDefinitionTokenizer.h"

#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>

#include <cerrno>
#include <ctime>
#include <iostream>

extern "C" {
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <cctype>
#include <stack>

using namespace Hypertable;
using namespace std;

namespace {

  bool is_identifier_start_character(char c) {
    return isalpha(c) || c == '_';
  }

  bool is_identifier_character(char c) {
    return is_identifier_start_character(c) || isdigit(c);
  }

  const char *find_role_end(const char *base, size_t *linep) {
    const char *end = base;
    while (*end && *end != '\n')
      end++;
    return end;
  }

  bool find_end_char(const char *base, const char **endp, size_t *linep) {
    stack<char> scope;

    HT_ASSERT(*base == '"' || *base == '\'' || *base == '`' || *base == '{');

    scope.push(*base);
    const char *ptr = base+1;

    while (*ptr) {

      if (scope.top() == '"') {
        if (*ptr == '"' && *(ptr-1) != '\\') {
          scope.pop();
          if (scope.empty()) {
            *endp = ptr+1;
            return true;
          }
        }
      }
      else if (scope.top() == '\'') {
        if (*ptr == '\'' && *(ptr-1) != '\\') {
          scope.pop();
          if (scope.empty()) {
            *endp = ptr+1;
            return true;
          }
        }
      }
      else if (scope.top() == '`') {
        if (*ptr == '`') {
          scope.pop();
          if (scope.empty()) {
            *endp = ptr+1;
            return true;
          }
        }
      }
      else {
        HT_ASSERT(scope.top() == '{');
        if (*ptr == '}') {
          scope.pop();
          if (scope.empty()) {
            *endp = ptr+1;
            return true;
          }
        }
        else if (*ptr == '"' || *ptr == '\'' || *ptr == '`' || *ptr == '{')
          scope.push(*ptr);
      }
      ptr++;
    }

    return false;
  }
}

const char *ClusterDefinitionTokenizer::Token::type_to_text(int type) {
  switch (type) {
  case(NONE):
    return "NONE";
  case(VARIABLE):
    return "VARIABLE";
  case(ROLE):
    return "ROLE";
  case(TASK):
    return "TASK";
  case(FUNCTION):
    return "FUNCTION";
  case(COMMENT):
    return "COMMENT";
  case(CODE):
    return "CODE";
  case(BLANKLINE):
    return "BLANKLINE";
  default:
    HT_ASSERT(!"Unknown token type");
  }
  return nullptr;
}

ClusterDefinitionTokenizer::ClusterDefinitionTokenizer(const string &content)
  : m_content(content) {
  m_next = m_content.c_str();
}

bool ClusterDefinitionTokenizer::next(Token &token) {
  const char *base = m_next;
  const char *end;
  const char *ptr;
  token.clear();

  if (*base == 0)
    return false;

  while (*base) {

    end = base;

    while (*end && *end != '\n')
      end++;

    m_line++;

    int line_type = identify_line_type(base, end);

    switch (line_type) {

    case (Token::VARIABLE):
      ptr = strchr(base, '=');
      ptr++;
      if (*ptr == '\'' || *ptr == '"' || *ptr == '`') {
        if (!find_end_char(ptr, &end, &m_line))
          HT_THROWF(Error::SYNTAX_ERROR,
                    "Unterminated string starting on line %d",(int)m_line);
      }
      accumulate(&base, end, Token::VARIABLE, token);
      return true;

    case (Token::ROLE):
      end = find_role_end(base, &m_line);
      if (accumulate(&base, end, Token::ROLE, token))
        return true;
      break;

    case (Token::TASK):
      if ((ptr = strchr(base, '{')) == 0)
        HT_THROWF(Error::SYNTAX_ERROR,
                  "Mal-formed task: statement starting on line %d",(int)m_line);
      if (!find_end_char(ptr, &end, &m_line))
        HT_THROWF(Error::SYNTAX_ERROR, "Missing terminating '}' character in "
                  "task: statement starting on line %d", (int)m_line);
      accumulate(&base, end, Token::TASK, token);
      return true;

    case (Token::FUNCTION):
      if ((ptr = strchr(base, '{')) == 0)
        HT_THROWF(Error::SYNTAX_ERROR,
                  "Mal-formed function starting on line %d",(int)m_line);
      if (!find_end_char(ptr, &end, &m_line))
        HT_THROWF(Error::SYNTAX_ERROR, "Missing terminating '}' character in "
                  "function starting on line %d", (int)m_line);
      if (accumulate(&base, end, Token::FUNCTION, token))
        return true;
      break;

    case (Token::COMMENT):
      if (accumulate(&base, end, Token::COMMENT, token))
        return true;
      break;

    case (Token::CODE):
      if (accumulate(&base, end, Token::CODE, token))
        return true;
      break;

    case (Token::BLANKLINE):
      if (accumulate(&base, end, Token::BLANKLINE, token))
        return true;
      break;

    default:
      HT_FATALF("Unknown token type - %u", (unsigned int)line_type);

    }
  }
  return true;
}


int ClusterDefinitionTokenizer::identify_line_type(const char *base, const char *end) {
  const char *ptr = base;

  // skip leading whitespace
  while (ptr < end && isspace(*ptr))
    ptr++;

  if (ptr == end)
    return Token::BLANKLINE;

  if (is_identifier_start_character(*ptr)) {
    ptr++;
    while (is_identifier_character(*ptr))
      ptr++;
    if (*ptr == '=')
      return Token::VARIABLE;
    else if (*ptr == ':') {
      if (!strncmp(base, "role", 4))
        return Token::ROLE;
      else if (!strncmp(base, "task", 4))
        return Token::TASK;
      else {
        string tag(base, ptr-base);
        HT_THROWF(Error::SYNTAX_ERROR,
                  "Unrecognized meta tag '%s:' on line %u",
                  tag.c_str(), (unsigned int)m_line);
      }
    }
    else if (isspace(*ptr)) {
      if (!strncmp(base, "function", 8))
        return Token::FUNCTION;
      ptr++;
      while (ptr < end && isspace(*ptr))
        ptr++;
      if (*ptr == '(')
        return Token::FUNCTION;
      return Token::CODE;
    }
  }
  else if (*ptr == '#')
    return Token::COMMENT;

  return Token::CODE;
}


bool ClusterDefinitionTokenizer::accumulate(const char **basep,
                                            const char *end,
                                            int type, Token &token) {

  if (token.type == Token::ROLE && type == Token::CODE)
    type = Token::ROLE;
  else {
    if (token.type == Token::COMMENT && type == Token::TASK)
      token.type = Token::TASK;
    else if (type == Token::FUNCTION || type == Token::BLANKLINE)
      type = Token::CODE;

    if (token.type != Token::NONE &&
        (type != token.type || type == Token::ROLE)) {
      m_next = *basep;
      return true;
    }
  }

  if (*end)
    end++;
  token.text.append(*basep, end-*basep);
  token.type = type;
  *basep = end;
  m_next = end;
  return *m_next == 0;
}
