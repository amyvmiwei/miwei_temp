/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "Common/Error.h"

#include "Global.h"
#include "MetadataRoot.h"

using namespace Hypertable;
using namespace Hyperspace;


MetadataRoot::MetadataRoot(SchemaPtr &schema) : m_next(0) {

  foreach_ht(const Schema::AccessGroup *ag, schema->get_access_groups())
    m_agnames.push_back(ag->name);

  try {
    m_handle = Global::hyperspace->open(Global::toplevel_dir + "/root",
                                        OPEN_FLAG_READ);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem opening Hyperspace root file '%s/root' - %s - "
              "%s", Global::toplevel_dir.c_str(), Error::get_text(e.code()), e.what());
    HT_ABORT;
  }

}


MetadataRoot::~MetadataRoot() {
  try {
    Global::hyperspace->close(m_handle);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}


void MetadataRoot::reset_files_scan() {
  m_next = 0;
}


bool MetadataRoot::get_next_files(String &ag_name, String &files, uint32_t *nextcsidp) {

  while (m_next < m_agnames.size()) {
    std::vector<String> attrs;
    attrs.push_back((String)"files." + m_agnames[m_next]);
    attrs.push_back((String)"nextcsid." + m_agnames[m_next]);
    ag_name = m_agnames[m_next];
    m_next++;

    *nextcsidp = 0;

    // Read files and nextcsid
    try {
      std::vector<DynamicBufferPtr> values;
      Global::hyperspace->attrs_get(m_handle, attrs, values);
      if (values.front()) {
        if (values.back())
          *nextcsidp = atoi((const char *)values.back()->base);
        files = (const char *)values.front()->base;
        return true;
      }
    }
    catch (Exception &e) {
      HT_ERRORF("Problem getting attributes %s/%s on Hyperspace file "
                "'%s/root' - %s", attrs.front().c_str(), attrs.back().c_str(),
                Global::toplevel_dir.c_str(), Error::get_text(e.code()));
      return false;
    }
  }

  return false;
}


void MetadataRoot::write_files(const String &ag_name, const String &files, int64_t total_blocks) {
  std::vector<Attribute> attrs;
  String files_attrname = (String)"files." + ag_name;
  String blockcount_attrname = (String)"blockcount." + ag_name;
  char blockcount_buf[32];

  sprintf(blockcount_buf, "%llu", (Llu)total_blocks);

  attrs.push_back(Attribute(files_attrname.c_str(), files.c_str(), files.length()));
  attrs.push_back(Attribute(blockcount_attrname.c_str(), blockcount_buf, strlen(blockcount_buf)));

  // Write "Files" and "BlockCount"
  try {
    Global::hyperspace->attr_set(m_handle, attrs);
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, (String)"Problem creating attributes '" + files_attrname
              + "/" + blockcount_attrname + "' on Hyperspace file '/hypertable/root'");
  }

}

void MetadataRoot::write_files(const String &ag_name, const String &files, int64_t total_blocks, uint32_t nextcsid) {
  String files_attrname = (String)"files." + ag_name;
  String nextcsid_attrname = (String)"nextcsid." + ag_name;
  String blockcount_attrname = (String)"blockcount." + ag_name;
  char nextcsid_buf[32], blockcount_buf[32];

  sprintf(nextcsid_buf, "%u", (unsigned)nextcsid);
  sprintf(blockcount_buf, "%llu", (Llu)total_blocks);

  std::vector<Attribute> attrs;
  attrs.push_back(Attribute(files_attrname.c_str(), files.c_str(), files.length()));
  attrs.push_back(Attribute(nextcsid_attrname.c_str(), nextcsid_buf, strlen(nextcsid_buf)));
  attrs.push_back(Attribute(blockcount_attrname.c_str(), blockcount_buf, strlen(blockcount_buf)));

  // Write "Files", "NextCSID", and "BlockCount"
  try {
    Global::hyperspace->attr_set(m_handle, attrs);
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, (String)"Problem creating attributes '" + files_attrname
              + "/" + nextcsid_attrname + "/" + blockcount_attrname + 
              "' on Hyperspace file '/hypertable/root'");
  }

}
