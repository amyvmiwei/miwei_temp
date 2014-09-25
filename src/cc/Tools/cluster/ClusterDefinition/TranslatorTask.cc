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
/// Definitions for TranslatorTask.
/// This file contains type definitions for TranslatorTask, a class for
/// translating a task definition statement.

#include <Common/Compat.h>

#include "TokenizerTools.h"
#include "TranslatorTask.h"

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/String.h>
#include <Common/System.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include <cctype>
#include <vector>

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace boost;
using namespace std;

namespace {

  bool translate_ssh_statement(const char *base, const char *end,
                               const char **nextp, string &ssh_command) {
    HT_ASSERT(strncmp(base, "ssh:", 4) == 0);

    base += 4;

    const char *open_curly = base;
    const char *close_curly;
    while (open_curly < end && *open_curly != '{')
      open_curly++;

    if (open_curly == end || !TokenizerTools::find_end_char(open_curly, &close_curly))
      return false;
    
    string options(base, open_curly-base);
    
    // parse options

    // build ssh command
    ssh_command.clear();
    ssh_command.append(Hypertable::format("%s/bin/ht ssh \"${_SSH_HOSTS}\" \"", System::install_dir.c_str()));
    open_curly++;
    string contents(open_curly, close_curly-open_curly);
    char_separator<char> sep("\n\r");
    string line;
    bool first = true;
    tokenizer<char_separator<char>> tokens(contents, sep);
    for (const auto& token : tokens) {
      line = token;
      trim_if(line, is_any_of(" \t;"));
      if (first)
        first = false;
      else
        ssh_command.append("; ");
      ssh_command.append(line);
    }
    ssh_command.append("\"");

    *nextp = close_curly + 1;

    return true;
  }


}

const string TranslatorTask::translate(TranslationContext &context) {
  string translated_text;
  string short_description;
  string long_description;

  const char *base = m_text.c_str();
  const char *end = strchr(base, '\n');
  string line;

  while (base) {
    line.clear();
    if (end == 0)
      HT_THROWF(Error::SYNTAX_ERROR, "Bad task definition on line %d of '%s'",
                (int)m_lineno, m_fname.c_str());
    line.append(base, end-base);
    boost::trim(line);
    if (line[0] == '#')
      long_description.append(line.substr(1));
    else
      break;
    base = end + 1;
    end = strchr(base, '\n');
  }

  if (base == 0)
    HT_THROWF(Error::SYNTAX_ERROR, "Bad task definition on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());

  const char *ptr;
  if ((ptr = strchr(base, '{')) == 0)
    HT_THROWF(Error::SYNTAX_ERROR, "Bad task definition on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());

  if (!TokenizerTools::find_end_char(ptr, &end))
    HT_THROWF(Error::SYNTAX_ERROR,
              "Missing terminating '}' in task definition on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());

  string text(base, ptr-base);

  vector<string> words;
  {
    char_separator<char> sep(" ");
    tokenizer<char_separator<char>> tokens(text, sep);
    for (const auto& t : tokens)
      words.push_back(t);
  }

  if (words.size() < 2 || words.size() == 3 || words[0].compare("task:") != 0 ||
      (words.size() >= 3 && words[2].compare("roles:") != 0))      
    HT_THROWF(Error::SYNTAX_ERROR, "Bad task definition on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());

  string task_name(words[1]);

  if (!TokenizerTools::is_valid_identifier(task_name))
    HT_THROWF(Error::SYNTAX_ERROR, "Invalid task name (%s) on line %d of '%s'",
              task_name.c_str(), (int)m_lineno, m_fname.c_str());

  translated_text.append(task_name);
  translated_text.append(" () {\n  local _SSH_HOSTS=\"");;

  if (words.size() > 3) {
    text.clear();
    for (size_t i=3; i<words.size(); i++) {
      text.append(words[i]);
      text.append(" ");
    }
    char_separator<char> sep(" ,");
    tokenizer<char_separator<char>> tokens(text, sep);
    bool first = true;
    for (const auto& t : tokens) {
      if (context.roles.count(t) == 0)
        HT_THROWF(Error::SYNTAX_ERROR, "Undefined role (%s) on line %d of '%s'",
                  t.c_str(), (int)m_lineno, m_fname.c_str());
      if (first)
        first = false;
      else
        translated_text.append(" + ");
      translated_text.append("(${ROLE_");
      translated_text.append(t);
      translated_text.append("})");
    }
  }
  else {
    bool first = true;
    for (auto & role : context.roles) {
      if (first)
        first = false;
      else
        translated_text.append(" + ");
      translated_text.append("(${ROLE_");
      translated_text.append(role);
      translated_text.append("})");
    }
  }
  translated_text.append("\"\n  ");

  // skip '{' and whitespace
  base = ptr + 1;
  while (base < end && isspace(*base))
    base++;

  HT_ASSERT(base < end);

  string task_body;
  string ssh_command;
  size_t lineno = m_lineno;
  size_t offset;

  while (TokenizerTools::find_token("ssh:", base, end, &offset)) {
    lineno += TokenizerTools::count_newlines(base, base+offset);
    task_body.append(base, offset);
    base += offset;
    if (!translate_ssh_statement(base, end, &ptr, ssh_command))
      HT_THROWF(Error::SYNTAX_ERROR,"Invalid ssh: statement on line %d of '%s'",
                (int)lineno, m_fname.c_str());
    task_body.append(ssh_command);
    base = ptr;
  }

  if (base < end) {
    string contents(base, end-base);
    trim_right(contents);
    task_body.append(contents);
  }

  /*
  string task_body(ptr, end-ptr);
  boost::trim(task_body);
  */

  translated_text.append(task_body);

  translated_text.append("\n}\n");

  return translated_text;
}

