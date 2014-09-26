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
}

Compiler::Compiler(const string &fname) : m_definition_file(fname) {
  struct passwd *pw = getpwuid(getuid());
  m_definition_script.append(pw->pw_dir);
  m_definition_script.append("/.cluster");
  m_definition_script.append(m_definition_file);
  m_definition_script.append(".sh");

  if (compilation_needed())
    make();
}


bool Compiler::compilation_needed() {

  if (!FileUtils::exists(m_definition_script))
    return true;

  struct stat statbuf;

  if (stat(m_definition_file.c_str(), &statbuf) < 0) {
    cout << "stat('" << m_definition_file << "') - " << strerror(errno) << endl;
    exit(1);
  }
  time_t definition_modification_time = statbuf.st_mtime;

  if (stat(m_definition_script.c_str(), &statbuf) < 0) {
    cout << "stat('" << m_definition_script << "') - " << strerror(errno) << endl;
    exit(1);
  }
  time_t script_modification_time = statbuf.st_mtime;

  return definition_modification_time > script_modification_time;

}

void Compiler::make() {
  size_t lastslash = m_definition_script.find_last_of('/');
  HT_ASSERT(lastslash != string::npos);

  string script_directory = m_definition_script.substr(0, lastslash);
  if (!FileUtils::mkdirs(script_directory)) {
    cout << "mkdirs('" << script_directory << "') - " << strerror(errno) << endl;
    exit(1);
  }

  stack<TokenizerPtr> definitions;

  definitions.push( make_shared<Tokenizer>(m_definition_file) );

  string output;
  Token token;
  TranslationContext context;

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
  output.append("fi\n");

  bool first = true;
  for (auto & entry : context.tasks) {
    if (first) {
      output.append(format("if [ $1 == \"%s\" ]; then\n  %s\n",
                           entry.first.c_str(), entry.first.c_str()));
      first = false;
    }
    else
      output.append(format("elif [ $1 == \"%s\" ]; then\n  %s\n",
                           entry.first.c_str(), entry.first.c_str()));
  }
  output.append("fi\n");

  if (FileUtils::write(m_definition_script, output) < 0)
    exit(1);

  if (chmod(m_definition_script.c_str(), 0755) < 0) {
    cout << "chmod('" << m_definition_script << "', 0755) failed - "
         << strerror(errno) << endl;
    exit(1);
  }

}
