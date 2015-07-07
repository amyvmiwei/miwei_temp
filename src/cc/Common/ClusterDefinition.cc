/*
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
/// Definitions for ClusterDefinition.
/// This file contains type definitions for ClusterDefinition, a class that
/// caches and provides access to the information in a cluster definition file.

#include <Common/Compat.h>

#include "ClusterDefinition.h"

#include <Common/HostSpecification.h>
#include <Common/Logger.h>
#include <Common/ClusterDefinition/Tokenizer.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include <cerrno>
#include <cstring>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace std;


void
ClusterDefinition::ClusterDefinition::get_role_members(const string &role,
                                                       vector<string> &members,
                                                       int64_t *generation){
  lock_guard<mutex> lock(m_mutex);
  load_file();
  auto iter = m_roles.find(role);
  if (iter == m_roles.end()) {
    members.clear();
    if (generation)
      *generation = 0;
    return;
  }
  members = iter->second;
  if (generation)
    *generation = (int64_t)m_last_mtime;
}

void ClusterDefinition::ClusterDefinition::load_file() {
  struct stat stats;

  if (stat(m_fname.c_str(), &stats)) {
    if (errno == ENOENT) {
      m_roles.clear();
      m_last_mtime = 0;
      return;
    }
    HT_FATALF("Unable to access file %s - %s",
              m_fname.c_str(), strerror(errno));
  }

  if (stats.st_mtime > m_last_mtime) {
    TokenizerPtr tokenizer(new Tokenizer(m_fname));
    Token token;
    while (tokenizer->next(token)) {
      if (token.type == Token::ROLE)
        add_role(token.text);
    }
    m_last_mtime = stats.st_mtime;
  }
  
}

void ClusterDefinition::ClusterDefinition::add_role(const std::string &text) {
  const char *base = text.c_str();
  const char *ptr;
  for (ptr=base; *ptr && !isspace(*ptr); ptr++)
    ;
  string token(base, ptr-base);
  boost::trim(token);
  if (strcasecmp(token.c_str(), "role:"))
    HT_THROWF(Error::SYNTAX_ERROR, "Mal-formed role definition: %s", text.c_str());
  while (*ptr && isspace(*ptr))
    ptr++;
  base = ptr;
  while (*ptr && !isspace(*ptr))
    ptr++;
  string name(base, ptr-base);
  boost::trim(name);
  if (name.empty())
    HT_THROWF(Error::SYNTAX_ERROR, "Mal-formed role definition: %s", text.c_str());
  string input(ptr);
  string value;

  boost::char_separator<char> sep(" \t\n\r");
  boost::tokenizer< boost::char_separator<char> > tokens(input, sep);
  for (auto &t : tokens)
    value += t + " ";
  boost::trim(value);

  m_roles[name] = HostSpecification(value);
}
