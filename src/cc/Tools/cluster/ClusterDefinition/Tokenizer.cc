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

/// @file
/// Definitions for Tokenizer.
/// This file contains type definitions for Tokenizer, a class for tokenizing a
/// cluster definition file.

#include <Common/Compat.h>

#include "Tokenizer.h"
#include "TokenizerTools.h"

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
using namespace Hypertable::ClusterDefinition;
using namespace std;

Tokenizer::Tokenizer(const string &fname)
  : m_fname(fname) {
  if (FileUtils::read(m_fname, m_content) < 0)
    exit(1);
  m_next = m_content.c_str();
}

Tokenizer::Tokenizer(const string &fname,
                                                       const string &content)
  : m_fname(fname), m_content(content) {
  m_next = m_content.c_str();
}

string Tokenizer::dirname() {
  size_t lastslash = m_fname.find_last_of('/');
  if (lastslash != string::npos)
    return m_fname.substr(0, lastslash);
  return ".";
}

bool Tokenizer::next(Token &token) {
  const char *base = m_next;
  const char *end;
  const char *ptr;
  token.clear();

  if (*base == 0)
    return false;

  token.line = m_line + 1;
  token.fname = m_fname;

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
        int starting_line = (int)m_line;
        if (!TokenizerTools::find_end_char(ptr, &end, &m_line))
          HT_THROWF(Error::SYNTAX_ERROR,
                    "Unterminated string starting on line %d", starting_line);
        TokenizerTools::skip_to_newline(&end);
      }
      accumulate(&base, end, Token::VARIABLE, token);
      return true;

    case (Token::ROLE):
      if (accumulate(&base, end, Token::ROLE, token))
        return true;
      break;

    case (Token::TASK):
      if ((ptr = strchr(base, '{')) == 0)
        HT_THROWF(Error::SYNTAX_ERROR,
                  "Mal-formed task: statement starting on line %d",(int)m_line);
      {
        int starting_line = (int)m_line;
        if (!TokenizerTools::find_end_char(ptr, &end, &m_line))
          HT_THROWF(Error::SYNTAX_ERROR, "Missing terminating '}' character in "
                    "task: statement on line %d of file '%s'",
                    starting_line, m_fname.c_str());
        TokenizerTools::skip_to_newline(&end);
      }
      accumulate(&base, end, Token::TASK, token);
      return true;

    case (Token::FUNCTION):
      if ((ptr = strchr(base, '{')) == 0)
        HT_THROWF(Error::SYNTAX_ERROR,
                  "Mal-formed function starting on line %d",(int)m_line);
      {
        int starting_line = (int)m_line;
        if (!TokenizerTools::find_end_char(ptr, &end, &m_line))
          HT_THROWF(Error::SYNTAX_ERROR, "Missing terminating '}' character in "
                    "function on line %d of file '%s'",
                    starting_line, m_fname.c_str());
        TokenizerTools::skip_to_newline(&end);
      }
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

    case (Token::INCLUDE):
      if (accumulate(&base, end, Token::INCLUDE, token))
        return true;
      break;

    default:
      HT_FATALF("Unknown token type - %u", (unsigned int)line_type);

    }
  }
  return true;
}


int Tokenizer::identify_line_type(const char *base, const char *end) {
  const char *ptr = base;

  // skip leading whitespace
  while (ptr < end && isspace(*ptr))
    ptr++;

  if (ptr == end)
    return Token::BLANKLINE;

  if (TokenizerTools::is_identifier_start_character(*ptr)) {
    ptr++;
    while (TokenizerTools::is_identifier_character(*ptr))
      ptr++;
    if (*ptr == '=')
      return Token::VARIABLE;
    else if (*ptr == ':') {
      if (!strncmp(base, "include", 4))
        return Token::INCLUDE;
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


bool Tokenizer::accumulate(const char **basep,
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
      m_line--;
      token.create_translator();
      return true;
    }
  }

  if (*end)
    end++;
  token.text.append(*basep, end-*basep);
  token.type = type;
  *basep = end;
  m_next = end;
  if (type == Token::VARIABLE || type == Token::TASK || *m_next == 0) {
    token.create_translator();
    return true;
  }
  return false;
}
