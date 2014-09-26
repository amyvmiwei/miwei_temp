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
/// Ddefinitions for Compiler.
/// This file contains type ddefinitions for Compiler, a class for compiling a
/// cluster definition file into an executable bash script.

#include <Common/Compat.h>

#include "Compiler.h"
#include "Tokenizer.h"

#include <Common/FileUtils.h>
#include <Common/Logger.h>

#include <boost/algorithm/string.hpp>

#include <cerrno>
#include <ctime>
#include <iostream>
#include <stack>
#include <string>
#include <vector>

extern "C" {
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

namespace {

  string extract_short_description(const string &description) {
    string short_description;
    const char *ptr = strstr(description.c_str(), ". ");
    if (ptr) {
      if ((ptr - description.c_str()) < 52)
        short_description = description.substr(0, ptr - description.c_str());
      else
        short_description = description.substr(0, 51);
    }
    else if (description.length() < 52)
      short_description = description;
    else
      short_description = description.substr(0, 51);
    return short_description;
  }

  void extract_long_description(const string &description, vector<string> &lines) {
    
    string long_description;
    lines.clear();
    const char *base = description.c_str();
    const char *end = base + description.length();
    const char *ptr;
    while (base < end) {
      // skip whitespace
      while (*base && isspace(*base))
        base++;

      if ((end-base) > 79) {
        ptr = base + 79;
        for (size_t i=0; i<20; i++) {
          if (isspace(*ptr)) {
            lines.push_back(string(base, ptr-base));
            base = ptr;
            break;
          }
          ptr--;
        }
        if (ptr != base) {
          ptr = base + 79;
          while (*ptr && !isspace(*ptr))
            ptr++;
          lines.push_back(string(base, ptr-base));
          base = ptr;
        }
      }
      else {
        lines.push_back(base);
        break;
      }
    }
    return;
  }
  
}

Compiler::Compiler(const string &fname) : m_definition_file(fname) {
  struct passwd *pw = getpwuid(getuid());
  m_output_script.append(pw->pw_dir);
  m_output_script.append("/.cluster");
  m_output_script.append(m_definition_file);
  m_output_script.append(".sh");

  if (compilation_needed())
    make();
}


bool Compiler::compilation_needed() {

  if (!FileUtils::exists(m_output_script))
    return true;

  struct stat statbuf;

  if (stat(m_definition_file.c_str(), &statbuf) < 0) {
    cout << "stat('" << m_definition_file << "') - " << strerror(errno) << endl;
    exit(1);
  }
  time_t definition_modification_time = statbuf.st_mtime;

  if (stat(m_output_script.c_str(), &statbuf) < 0) {
    cout << "stat('" << m_output_script << "') - " << strerror(errno) << endl;
    exit(1);
  }
  time_t script_modification_time = statbuf.st_mtime;

  return definition_modification_time > script_modification_time;

}

void Compiler::make() {
  size_t lastslash = m_output_script.find_last_of('/');
  HT_ASSERT(lastslash != string::npos);

  string script_directory = m_output_script.substr(0, lastslash);
  if (!FileUtils::mkdirs(script_directory)) {
    cout << "mkdirs('" << script_directory << "') - " << strerror(errno) << endl;
    exit(1);
  }

  stack<TokenizerPtr> definitions;

  definitions.push( make_shared<Tokenizer>(m_definition_file) );

  string output;
  Token token;
  TranslationContext context;
  bool first;

  output.append("#!/bin/bash\n");

  while (definitions.top()->next(token)) {
    if (token.translator)
      output.append(token.translator->translate(context));
    if (token.type == Token::INCLUDE) {
      string include_file = token.text.substr(token.text.find_first_of("include:")+8);
      boost::trim_if(include_file, boost::is_any_of("'\" \t\n\r"));
      if (include_file[0] != '/')
        include_file = definitions.top()->dirname() + "/" + include_file;
      definitions.push( make_shared<Tokenizer>(include_file) );      
    }
  }

  output.append("\n");
  output.append("display_line () {\n");
  output.append("  let size=$1\n");
  output.append("  for ((i=0; i<$size; i++)); do\n");
  output.append("    echo -n \"-\"\n");
  output.append("  done\n");
  output.append("  echo\n");
  output.append("}\n");


  const char *dots = "..........................";
  output.append("\n");
  output.append("if [ $1 == \"-T\" ] || [ $1 == \"--tasks\" ]; then\n");
  output.append("  echo\n");
  output.append("  echo \"TASK                        DESCRIPTION\"\n");
  output.append("  echo \"=========================== ===================================================\"\n");
  for (auto & entry : context.tasks) {
    output.append("  echo \"");
    output.append(entry.first);
    output.append(" ");
    if (entry.first.length() < 26)
      output.append(dots, 26-entry.first.length());
    else
      output.append("..");
    output.append(" ");
    output.append(extract_short_description(entry.second));
    output.append("\"\n");
  }
  output.append("  echo\n");
  output.append("  exit 0\n");
  output.append("elif [ $1 == \"-e\" ] || [ $1 == \"--explain\" ]; then\n");
  output.append("  shift\n");
  output.append("  if [ $# -eq 0 ]; then\n");
  output.append("    echo \"Missing task name in -e option\"\n");
  output.append("    exit 1\n");
  output.append("  fi\n");

  if (!context.tasks.empty()) {
    vector<string> lines;
    first = true;
    for (auto & entry : context.tasks) {
      if (first) {
        output.append("  if [ $1 == \"");
        first = false;
      }
      else
        output.append("  elif [ $1 == \"");
      output.append(entry.first);
      output.append("\" ]; then\n");
      output.append("  echo\n");
      output.append("  echo \"$1\"\n");
      output.append("  display_line ${#1}\n");
      extract_long_description(entry.second, lines);
      for (auto & line : lines) {
        output.append("    echo \"");
        output.append(line);
        output.append("\"\n");
      }
    }
    output.append("  else\n");
    output.append("    echo \"Task '$1' is not defined.\"\n");
    output.append("    exit 1\n");
    output.append("  fi\n");
    output.append("  echo\n");
    output.append("  exit 0\n");
  }
  else {
    output.append("  echo \"Task '$1' is not defined.\"\n");
    output.append("  exit 1\n");
  }

  output.append("fi\n");

  // Generate task targets
  if (!context.tasks.empty()) {
    first = true;
    for (auto & entry : context.tasks) {
      if (first) {
        output.append(format("if [ $1 == \"%s\" ]; then\n  shift\n  %s $@\n",
                             entry.first.c_str(), entry.first.c_str()));
        first = false;
      }
      else
        output.append(format("elif [ $1 == \"%s\" ]; then\n  shift\n  %s $@\n",
                             entry.first.c_str(), entry.first.c_str()));
    }
    output.append("fi\n");
  }

  if (FileUtils::write(m_output_script, output) < 0)
    exit(1);

  if (chmod(m_output_script.c_str(), 0755) < 0) {
    cout << "chmod('" << m_output_script << "', 0755) failed - "
         << strerror(errno) << endl;
    exit(1);
  }

}
