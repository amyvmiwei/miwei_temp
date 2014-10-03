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

  const char *builtin_task_name[] = {
    "CLUSTER_BUILTIN_display_line",
    "show_variables",
    "with",
    nullptr
  };

  bool translate_ssh_statement(const char *base, const char *end,
                               const char **nextp, string &ssh_command,
                               string &errmsg) {
    HT_ASSERT(strncmp(base, "ssh:", 4) == 0);
    errmsg.clear();

    base += 4;

    const char *open_curly = base;
    while (open_curly < end && *open_curly != '{')
      open_curly++;

    const char *close_curly;
    if (open_curly == end || !TokenizerTools::find_end_char(open_curly, &close_curly)) {
      errmsg.append("curly brace not found");
      return false;
    }
        
    // parse options
    string options;
    {
      string text(base, open_curly-base);
      char_separator<char> sep(" \t\n\r");
      tokenizer<char_separator<char>> tokens(text, sep);
      for (const auto& token : tokens) {
        if (!strncmp(token.c_str(), "random-start-delay=", 19)) {
          if (!TokenizerTools::is_number(token.substr(19))) {
            errmsg.append("invalid random-start-delay argument");
            return false;
          }
          options.append(" --");
          options.append(token.substr(0, 18));
          options.append(" ");
          options.append(token.substr(19));
        }
        else if (token.compare("in-series") == 0) {
          options.append(" --");
          options.append(token);
        }
        else {
          errmsg.append(Hypertable::format("Unknown option '%s'", token.c_str()));
          return false;
        }
      }
    }

    // build ssh command
    ssh_command.clear();
    ssh_command.append(Hypertable::format("%s/bin/ht ssh%s \" ${_SSH_HOSTS}\" \"",
                                          System::install_dir.c_str(), options.c_str()));
    open_curly++;
    string content(open_curly, close_curly-open_curly);
    trim(content);

    string escaped_content;
    escaped_content.reserve(content.length()+32);
    for (const char *ptr = content.c_str(); *ptr; ptr++) {
      if (*ptr == '"')
        escaped_content.append(1, '\\');
      escaped_content.append(1, *ptr);
    }

    ssh_command.append(escaped_content);
    ssh_command.append("\"");

    *nextp = close_curly + 1;

    return true;
  }

}




const string TranslatorTask::translate(TranslationContext &context) {
  string translated_text;
  string description;

  const char *base = m_text.c_str();
  const char *end;
  string line;

  while (*base) {

    end = strchr(base, '\n');
    line.clear();
    if (end == 0)
      line.append(base);
    else
      line.append(base, end-base);
    boost::trim(line);

    if (line[0] == '#') {
      line = line.substr(1);
      trim(line);
      if (description.empty())
        description.append(line);
      else if (description[description.length()-1] == '.') {
        description.append("  ");
        description.append(line);
      }
      else {
        description.append(" ");
        description.append(line);
      }
    }
    else
      break;
    if (end)
      base = end;
    else
      base += strlen(base);
    if (*base)
      base++;
  }

  // Do variable substitution
  while (TokenizerTools::substitute_variables(description, description,
                                              context.symbols))
    ;

  if (*base == 0)
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

  for (size_t i=0; builtin_task_name[i]; i++) {
    if (task_name.compare(builtin_task_name[i]) == 0)
      HT_THROWF(Error::SYNTAX_ERROR,
                "Task name '%s' conflicts with built-in on line %d of '%s'",
                task_name.c_str(), (int)m_lineno, m_fname.c_str());
  }

  if (!TokenizerTools::is_valid_identifier(task_name))
    HT_THROWF(Error::SYNTAX_ERROR, "Invalid task name (%s) on line %d of '%s'",
              task_name.c_str(), (int)m_lineno, m_fname.c_str());

  translated_text.append(task_name);
  translated_text.append(" () {\n  local _SSH_HOSTS=\"");;

  string task_roles;
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
      if (!task_roles.empty())
        task_roles.append(", ");
      task_roles.append(t);
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
  translated_text.append("\"\n");
  translated_text.append("  if [ $# -gt 0 ] && [ $1 == \"on\" ]; then\n");
  translated_text.append("    shift\n");
  translated_text.append("    if [ $# -eq 0 ]; then\n");
  translated_text.append("      echo \"Missing host specification in 'on' argument\"\n");
  translated_text.append("      exit 1\n");
  translated_text.append("    else\n");
  translated_text.append("      _SSH_HOSTS=\"$1\"\n");
  translated_text.append("      shift\n");
  translated_text.append("    fi\n");
  translated_text.append("  fi\n");
  translated_text.append("  echo \"");
  translated_text.append(task_name);
  translated_text.append(" $@\"\n  ");

  size_t lineno = m_lineno + TokenizerTools::count_newlines(m_text.c_str(), base);

  // skip '{' and whitespace
  base = ptr + 1;
  while (base < end && isspace(*base)) {
    if (*base == '\n')
      lineno++;
    base++;
  }

  HT_ASSERT(base <= end);

  string task_body;
  string ssh_command;
  string error_msg;
  size_t offset;

  // Check for missing ':' character
  if (TokenizerTools::find_token("ssh", base, end, &offset)) {
    size_t tmp_lineno = lineno + TokenizerTools::count_newlines(base, base+offset);
    ptr = base + offset + 3;
    while (*ptr && isspace(*ptr))
      ptr++;
    if (*ptr == '{' || *ptr == 0 || !strncmp(ptr, "random-start-delay=", 19) ||
        !strncmp(ptr, "in-series", 9))
      HT_THROWF(Error::SYNTAX_ERROR,"Invalid ssh: statement on line %d of '%s'",
                (int)tmp_lineno, m_fname.c_str());
  }

  while (TokenizerTools::find_token("ssh:", base, end, &offset)) {
    lineno += TokenizerTools::count_newlines(base, base+offset);
    task_body.append(base, offset);
    base += offset;
    if (!translate_ssh_statement(base, end, &ptr, ssh_command, error_msg))
      HT_THROWF(Error::SYNTAX_ERROR,"Invalid ssh: statement (%s) on line %d of '%s'",
                error_msg.c_str(), (int)lineno, m_fname.c_str());
    lineno += TokenizerTools::count_newlines(base, ptr);
    task_body.append(ssh_command);
    base = ptr;
  }

  if (base < end) {
    string content(base, end-base);
    trim_right(content);
    task_body.append(content);
  }

  translated_text.append(task_body);

  translated_text.append("\n}\n");

  context.tasks[task_name] = description;
  context.task_roles[task_name] = task_roles;

  return translated_text;
}

