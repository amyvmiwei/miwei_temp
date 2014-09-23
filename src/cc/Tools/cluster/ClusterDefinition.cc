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

#include "ClusterDefinition.h"
#include "ClusterDefinitionTokenizer.h"

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

using namespace Hypertable;
using namespace std;

ClusterDefinition::ClusterDefinition(const string &fname) : m_definition_file(fname) {
  struct passwd *pw = getpwuid(getuid());
  m_definition_script.append(pw->pw_dir);
  m_definition_script.append("/.cluster");
  m_definition_script.append(m_definition_file);
  m_definition_script.append(".sh");

  if (compilation_needed())
    make();
}


bool ClusterDefinition::compilation_needed() {

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

void ClusterDefinition::make() {
  size_t lastslash = m_definition_script.find_last_of('/');
  HT_ASSERT(lastslash != string::npos);

  string script_directory = m_definition_script.substr(0, lastslash);
  if (!FileUtils::mkdirs(script_directory)) {
    cout << "mkdirs('" << script_directory << "') - " << strerror(errno) << endl;
    exit(1);
  }
  string contents;
  if (FileUtils::read(m_definition_file, contents) < 0)
    exit(1);

  ClusterDefinitionTokenizer tokenizer(contents);

  /*
  if (FileUtils::write(m_definition_script, contents) < 0)
    exit(1);
  */

  cout << "make" << endl;
}
