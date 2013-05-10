/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Definitions for AccessGroupHintsFile.
 * This file contains method definitions for AccessGroupHintsFile, a class
 * used to read and write the hints file used for access group state
 * boostrapping.
 */

#include "Common/Compat.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StaticBuffer.h"
#include "Common/String.h"
#include "Common/md5.h"

#include "Global.h"

#include "AccessGroupHintsFile.h"

#include <boost/tokenizer.hpp>

using namespace Hypertable;

AccessGroupHintsFile::AccessGroupHintsFile(const String &table,
                                           const String &end_row) :
  m_table_id(table) {
  change_end_row(end_row);
}

void AccessGroupHintsFile::change_end_row(const String &end_row) {
  char md5DigestStr[33];
  md5_trunc_modified_base64(end_row.c_str(), md5DigestStr);
  md5DigestStr[16] = 0;
  m_range_dir = md5DigestStr;
}

namespace {
  const char *ag_hint_format = "%s: {\n"
    "  LatestStoredRevision: %lld,\n"
    "  DiskUsage: %llu,\n"
    "  Files: %s\n}\n";
}

void AccessGroupHintsFile::write(const std::vector<AccessGroup::Hints> &hints) {
  String contents;
  int32_t fd = -1;
  bool first_try = true;

  String parent_dir = format("%s/tables/%s/default/%s",
                             Global::toplevel_dir.c_str(),
                             m_table_id.c_str(), m_range_dir.c_str());

  foreach_ht (const AccessGroup::Hints &h, hints)
    contents += format(ag_hint_format, h.ag_name.c_str(),
                       (Llu)h.latest_stored_revision,
                       (Lld)h.disk_usage, h.files.c_str());

 try_again:

  try {
    if (!first_try) {
      if (fd != -1)
        Global::dfs->close(fd, (DispatchHandler *)0);
      if (!Global::dfs->exists(parent_dir))
        Global::dfs->mkdirs(parent_dir);
    }
    fd = Global::dfs->create(parent_dir + "/hints",
                             Filesystem::OPEN_FLAG_OVERWRITE, -1, -1, -1);
    StaticBuffer sbuf(contents.length());
    memcpy(sbuf.base, contents.c_str(), contents.length());
    Global::dfs->append(fd, sbuf, Filesystem::O_FLUSH);
    Global::dfs->close(fd, (DispatchHandler *)0);
  }
  catch (Exception &e) {
    HT_INFOF("Exception caught writing hints file %s/hints - %s",
             parent_dir.c_str(), Error::get_text(e.code()));
    if (first_try) {
      first_try = false;
      goto try_again;
    }
    HT_ERRORF("Problem writing hints file %s/hints - %s",
              parent_dir.c_str(), Error::get_text(e.code()));
  }

}

void AccessGroupHintsFile::read(std::vector<AccessGroup::Hints> &hints) {
  int32_t fd = -1;
  AccessGroup::Hints h;
  DynamicBuffer dbuf;

  hints.clear();

  String filename = format("%s/tables/%s/default/%s/hints",
                           Global::toplevel_dir.c_str(),
                           m_table_id.c_str(), m_range_dir.c_str());
  try {
    int64_t length = Global::dfs->length(filename);
    size_t nread = 0;

    dbuf.grow(length+1);

    fd = Global::dfs->open(filename, 0);
    nread = Global::dfs->read(fd, dbuf.base, length);
    dbuf.base[nread] = 0;

    const char *base = (char *)dbuf.base;
    const char *ptr = base;

    h.clear();
    while ((ptr = strchr(base, ':')) != 0) {
      h.ag_name = String((const char *)base, ptr-base);
      boost::trim(h.ag_name);
      if (h.ag_name.empty())
        HT_THROW(Error::BAD_FORMAT, "");
      base = ptr+1;
      while (*base && isspace(*base))
        base++;
      if (*base == '{') {
        base++;
        ptr = strchr(base, '}');
        if (ptr == 0)
          HT_THROW(Error::BAD_FORMAT, "");
        String text = String(base, ptr-base);
        boost::trim(text);
        boost::char_separator<char> sep(",");
        boost::tokenizer< boost::char_separator<char> > tokens(text, sep);
        foreach_ht (const String &mapping, tokens) {
          char *end;
          const char *ptr2 = strchr(mapping.c_str(), ':');
          if (ptr2 == 0)
            HT_THROW(Error::BAD_FORMAT, "");
          String key = String(mapping, 0, ptr2-mapping.c_str());
          boost::trim(key);
          String value = String(mapping, (ptr2-mapping.c_str())+1);
          boost::trim(value);
          if (key.empty())
            HT_THROW(Error::BAD_FORMAT, "");            
          if (key == "LatestStoredRevision") {
            h.latest_stored_revision = strtoll(value.c_str(), &end, 0);
            if (value.empty() || *end != 0)
              HT_THROW(Error::BAD_FORMAT, "");
          }
          else if (key == "DiskUsage") {
            h.disk_usage = strtoull(value.c_str(), &end, 0);
            if (value.empty() || *end != 0)
              HT_THROW(Error::BAD_FORMAT, "");
          }
          else if (key == "Files")
            h.files = value;
          else {
            HT_WARNF("Unrecognized key (%s) in hints file %s",
                     key.c_str(), filename.c_str());
          }
        }
        hints.push_back(h);
        h.clear();
        base = ptr+1;
      }
      else
        HT_THROW(Error::BAD_FORMAT, "");
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::DFSBROKER_BAD_FILENAME)
      HT_INFOF("Hints file %s does not exist, skipping...", filename.c_str());
    else
      HT_ERRORF("Problem loading hints file %s - %s", filename.c_str(),
                Error::get_text(e.code()));
    hints.clear();
  }
}
