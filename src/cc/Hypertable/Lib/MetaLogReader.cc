/* -*- c++ -*-
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
 * Definitions for MetaLog::Reader.
 * This file contains definitions for MetaLog::Reader, a class for reading
 * a %MetaLog.
 */

#include "Common/Compat.h"
#include "Common/Checksum.h"
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"
#include "Common/FileUtils.h"
#include "Common/Path.h"

#include "Common/StringExt.h"

#include <algorithm>
#include <cstdio>

#include <boost/algorithm/string.hpp>
#include <boost/shared_ptr.hpp>

#include "MetaLog.h"
#include "MetaLogReader.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

Reader::Reader(FilesystemPtr &fs, DefinitionPtr &definition) :
  m_fs(fs), m_definition(definition), m_version(0) {
}


Reader::Reader(FilesystemPtr &fs, DefinitionPtr &definition, const String &path) :
  m_fs(fs), m_definition(definition), m_version(0) {

  // Setup FS path name
  m_path = path;
  boost::trim_right_if(m_path, boost::is_any_of("/"));

  // Setup local backup path name
  Path data_dir = Config::properties->get_str("Hypertable.DataDirectory");
  m_backup_path = (data_dir /= String("/run/log_backup/") + String(m_definition->name()) + "/" +
                   String(m_definition->backup_label())).string();

  reload();
}

void Reader::get_entities(std::vector<EntityPtr> &entities) {
  std::map<EntityHeader, EntityPtr>::iterator iter;
  for (iter = m_entity_map.begin(); iter != m_entity_map.end(); ++iter)
    entities.push_back(iter->second);
}

void Reader::get_all_entities(std::vector<EntityPtr> &entities) {
  entities = m_entities;
}


void Reader::reload() {
  try {
    scan_log_directory(m_fs, m_path, m_file_nums, &m_next_filenum);
    std::sort(m_file_nums.begin(), m_file_nums.end());
    if (!m_file_nums.empty()) {
      verify_backup(m_file_nums.back());
      load_file(m_path + "/" + m_file_nums.back());
    }
  }
  catch (Exception &e) {
    if (e.code() & Error::METALOG_ERROR)
      throw;
    HT_THROW2_(Error::METALOG_READ_ERROR, e);
  }
}


namespace {
  const uint32_t READAHEAD_BUFSZ = 1024 * 1024;
  const uint32_t OUTSTANDING_READS = 1;
  void close_descriptor(FilesystemPtr fs, int *fdp) {
    try {
      fs->close(*fdp);
    }
    catch (Exception &e) {
      HT_ERRORF("Problem closing MetaLog: %s - %s",
                Error::get_text(e.code()), e.what());
    }
  }
}

void Reader::verify_backup(int32_t file_num) {

  String fname = m_path + "/" + file_num;
  String backup_filename = m_backup_path + "/" + file_num;

  if (!FileUtils::exists(backup_filename))
    return;

  size_t file_length = m_fs->length(fname);
  size_t backup_file_length = FileUtils::size(backup_filename);

  if (backup_file_length < file_length)
    HT_THROWF(Error::METALOG_BACKUP_FILE_MISMATCH,
          "MetaLog file '%s' has length %lld backup file '%s' length is %lld",
              fname.c_str(), (Lld)file_length, backup_filename.c_str(),
              (Lld)backup_file_length);
}


void Reader::load_file(const String &fname) {
  int64_t file_length = m_fs->length(fname);
  int fd = m_fs->open_buffered(fname, 0, READAHEAD_BUFSZ, OUTSTANDING_READS);
  bool found_recover_entry = false;
  int64_t cur_offset = 0;

  HT_MAYBE_FAIL("bad-rsml");

  m_entity_map.clear();

  HT_ON_SCOPE_EXIT(&close_descriptor, m_fs, &fd);

  read_header(fd, &cur_offset);

  try {
    size_t remaining;
    EntityHeader header;
    DynamicBuffer buf;

    buf.reserve(EntityHeader::LENGTH);

    while (cur_offset < file_length) {

      buf.clear();

      size_t nread = m_fs->read(fd, buf.base, EntityHeader::LENGTH);

      if (nread != EntityHeader::LENGTH)
        HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading entity header");

      remaining = EntityHeader::LENGTH;
      const uint8_t *ptr = buf.base;
      header.decode(&ptr, &remaining);

      cur_offset += nread;

      if (header.type == EntityType::RECOVER) {
        found_recover_entry = true;
        continue;
      }
      else if (header.flags & EntityHeader::FLAG_REMOVE) {
        std::map<EntityHeader, EntityPtr>::iterator iter = m_entity_map.find(header);
        if (iter != m_entity_map.end())
          m_entity_map.erase(iter);
        continue;
      }

      EntityPtr entity = m_definition->create(header);

      buf.clear();
      buf.ensure(header.length);

      nread = m_fs->read(fd, buf.base, header.length);

      if (nread != (size_t)header.length)
        HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading entity payload");

      cur_offset += nread;

      if (entity) {
        remaining = header.length;
        ptr = buf.base;
        entity->decode(&ptr, &remaining, m_version);

        // verify checksum
        int32_t computed_checksum = fletcher32(buf.base, header.length);
        if (header.checksum != computed_checksum)
          HT_THROWF(Error::METALOG_CHECKSUM_MISMATCH,
                    "MetaLog entry checksum mismatch header=%d, computed=%d",
                    header.checksum, computed_checksum);

        m_entity_map[header] = entity;
        m_entities.push_back(entity);
      }

    }

  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error reading metalog file: %s: read %llu/%llu",
               fname.c_str(), (Llu)cur_offset, (Llu)file_length);
  }

  if (!found_recover_entry)
    HT_THROW(Error::METALOG_MISSING_RECOVER_ENTITY, fname.c_str());

}


void Reader::read_header(int fd, int64_t *offsetp) {
  MetaLog::Header header;
  uint8_t buf[Header::LENGTH];
  const uint8_t *ptr = buf;
  size_t remaining = Header::LENGTH;

  size_t nread = m_fs->read(fd, buf, Header::LENGTH);

  if (nread != Header::LENGTH)
    HT_THROWF(Error::METALOG_BAD_HEADER,
              "Short read of %s header (expected %d, got %d)",
              m_definition->name(), (int)Header::LENGTH, (int)nread);

  *offsetp += nread;

  header.decode(&ptr, &remaining);

  if (strcmp(header.name, m_definition->name()))
    HT_THROWF(Error::METALOG_BAD_HEADER, "Wrong name in %s header ('%s' != '%s')",
              m_definition->name(), header.name, m_definition->name());

  m_version = header.version;

  if (m_definition->version() < m_version)
    HT_THROWF(Error::METALOG_VERSION_MISMATCH,
              "Unsuported %s version %d (definition version is %d)",
              m_definition->name(), m_version, m_definition->version());
}
