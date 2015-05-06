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

/** @file
 * Definitions for MetaLog::Writer.
 * This file contains definitions for MetaLog::Writer, a class for writing
 * a %MetaLog.
 */

#include <Common/Compat.h>

#include "MetaLog.h"
#include "MetaLogWriter.h"

#include <Common/Config.h>
#include <Common/FileUtils.h>
#include <Common/Path.h>
#include <Common/StringExt.h>

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>
#include <thread>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
}

using namespace Hypertable;
using namespace Hypertable::MetaLog;
using namespace std;

namespace {
  const int32_t FS_BUFFER_SIZE = -1;
  const int64_t FS_BLOCK_SIZE = -1;
}

bool Writer::skip_recover_entry = false;


Writer::Writer(FilesystemPtr &fs, DefinitionPtr &definition, const string &path,
               std::vector<EntityPtr> &initial_entities)
  : m_fs(fs), m_definition(definition) {

  HT_EXPECT(Config::properties, Error::FAILED_EXPECTATION);

  // Setup FS path name
  m_path = path;
  boost::trim_right_if(m_path, boost::is_any_of("/"));
  if (!m_fs->exists(m_path))
    m_fs->mkdirs(m_path);

  // Setup local backup path name
  Path data_dir = Config::properties->get_str("Hypertable.DataDirectory");
  m_backup_path = (data_dir /= String("/run/log_backup/") + String(m_definition->name()) + "/" +
                   String(m_definition->backup_label())).string();
  if (!FileUtils::exists(m_backup_path))
    FileUtils::mkdirs(m_backup_path);

  scan_log_directory(m_fs, m_path, m_file_ids);

  m_history_size = Config::properties->get_i32("Hypertable.MetaLog.HistorySize");

  m_max_file_size = Config::properties->get_i64("Hypertable.MetaLog.MaxFileSize");

  // get replication
  m_replication = Config::properties->get_i32("Hypertable.Metadata.Replication");

  // get flush method
  m_flush_method = convert(Config::properties->get_str("Hypertable.LogFlushMethod.Meta"));

  int32_t next_id = m_file_ids.empty() ? 0 : m_file_ids.front()+1;

  // Open FS file
  m_filename = m_path + "/" + next_id;
  m_fd = m_fs->create(m_filename, 0, FS_BUFFER_SIZE, m_replication, FS_BLOCK_SIZE);

  // Open backup file
  m_backup_filename = m_backup_path + "/" + next_id;
  m_backup_fd = ::open(m_backup_filename.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0644);

  m_file_ids.push_front(next_id);

  purge_old_log_files();

  write_header();

  m_write_scheduler = make_shared<WriteScheduler>(this);

  // Write existing entries
  record_state(initial_entities);

  // Write "Recover" entity
  if (!skip_recover_entry)
    record_state(make_shared<EntityRecover>());

}

Writer::~Writer() {
  m_write_scheduler.reset();
  close();
}

void Writer::close() {
  unique_lock<mutex> lock(m_mutex);
  try {
    if (m_fd != -1) {
      m_fs->close(m_fd);
      m_fd = -1;
      ::close(m_backup_fd);
      m_backup_fd = -1;
    }
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error closing metalog: %s (fd=%d)", m_filename.c_str(), m_fd);
  }

}


void Writer::purge_old_log_files() {

  while (m_file_ids.size() > m_history_size) {

    // remove from brokered FS
    string tmp_name = m_path + String("/") + m_file_ids.back();
    m_fs->remove(tmp_name);

    // remove local backup
    tmp_name = m_backup_path + String("/") + m_file_ids.back();
    if (FileUtils::exists(tmp_name))
      FileUtils::unlink(tmp_name);

    m_file_ids.pop_back();
  }

}

void Writer::roll() {

  // Close descriptors
  if (m_fd != -1) {
    m_fs->close(m_fd);
    m_fd = -1;
    ::close(m_backup_fd);
    m_backup_fd = -1;
  }

  int32_t next_id = m_file_ids.front() + 1;

  // Open next brokered FS file
  m_filename = m_path + "/" + next_id;
  m_fd = m_fs->create(m_filename, 0, FS_BUFFER_SIZE, m_replication, FS_BLOCK_SIZE);

  // Open next backup file
  m_backup_filename = m_backup_path + "/" + next_id;
  m_backup_fd = ::open(m_backup_filename.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0644);

  m_file_ids.push_front(next_id);

  purge_old_log_files();

  m_offset = 0;

  write_header();

  // Compute total length
  uint32_t total_length {};
  for (auto &entry : m_entity_map)
    total_length += entry.second.first;
  total_length += EntityHeader::LENGTH;  // For Recover entity

  // Create initial file contents
  StaticBuffer buf (new uint8_t [total_length], total_length);
  uint8_t *ptr = buf.base;
  for (auto &entry : m_entity_map) {
    memcpy(ptr, entry.second.second.get(), entry.second.first);
    ptr += entry.second.first;
  }
  EntityRecover er;
  er.encode_entry(&ptr);
  HT_ASSERT((ptr-buf.base) == (ptrdiff_t)buf.size);

  m_offset += buf.size;

  // Write contents to file(s)
  FileUtils::write(m_backup_fd, buf.base, buf.size);
  m_fs->append(m_fd, buf, m_flush_method);
  
}

void Writer::service_write_queue() {

  if (!m_write_ready)
    return;

  m_write_ready = false;

  if (!m_write_queue.empty()) {
    size_t total {};
    for (auto & sb : m_write_queue)
      total += sb->size;

    StaticBuffer buf(total);
    uint8_t *ptr = buf.base;

    for (auto & sb : m_write_queue) {
      memcpy(ptr, sb->base, sb->size);
      ptr += sb->size;
    }
    HT_ASSERT((ptr-buf.base) == (ptrdiff_t)buf.size);

    m_offset += buf.size;

    FileUtils::write(m_backup_fd, buf.base, buf.size);
    m_fs->append(m_fd, buf, m_flush_method);

    m_write_queue.clear();

    if (m_offset > m_max_file_size)
      roll();
  }

}


Writer::WriteScheduler::WriteScheduler(Writer *writer)
  : m_writer(writer) {
  m_interval = Config::properties->get_i32("Hypertable.MetaLog.WriteInterval");
  m_comm = Comm::instance();
}

Writer::WriteScheduler::~WriteScheduler() {
  unique_lock<mutex> lock(m_mutex);
  if (m_scheduled)
    m_cond.wait(lock);
}


void Writer::WriteScheduler::schedule() {
  unique_lock<mutex> lock(m_mutex);
  if (m_scheduled)
    return;
  auto duration = chrono::system_clock::now().time_since_epoch();
  auto millis = chrono::duration_cast<chrono::milliseconds>(duration).count();
  uint32_t duration_millis = m_interval - (millis % m_interval);
  int error;
  if ((error = m_comm->set_timer(duration_millis, shared_from_this())) != Error::OK)
    HT_FATALF("Problem setting MetaLog timer - %s", Error::get_text(error));
  m_scheduled = true;
}


void Writer::WriteScheduler::handle(EventPtr &event) {
  {
    unique_lock<mutex> lock(m_mutex);
    m_scheduled = false;
  }
  m_writer->signal_write_ready();
  m_cond.notify_all();
}


void Writer::write_header() {
  StaticBuffer buf(Header::LENGTH);
  uint8_t backup_buf[Header::LENGTH];
  Header header;

  assert(strlen(m_definition->name()) < sizeof(header.name));

  header.version = m_definition->version();
  memset(header.name, 0, sizeof(header.name));
  strcpy(header.name, m_definition->name());

  uint8_t *ptr = buf.base;

  header.encode(&ptr);

  assert((ptr-buf.base) == Header::LENGTH);
  memcpy(backup_buf, buf.base, Header::LENGTH);

  FileUtils::write(m_backup_fd, backup_buf, Header::LENGTH);
  if (m_fs->append(m_fd, buf, m_flush_method) != Header::LENGTH)
    HT_THROWF(Error::FSBROKER_IO_ERROR, "Error writing %s "
              "metalog header to file: %s", m_definition->name(),
              m_filename.c_str());

  m_offset += Header::LENGTH;
}


void Writer::record_state(EntityPtr entity) {
  unique_lock<mutex> lock(m_mutex);
  size_t length;
  StaticBufferPtr buf;

  if (m_fd == -1)
    HT_THROWF(Error::CLOSED, "MetaLog '%s' has been closed", m_path.c_str());

  {
    Locker<Entity> lock(*entity);
    length = EntityHeader::LENGTH + (entity->marked_for_removal() ? 0 : entity->encoded_length());
    buf = make_shared<StaticBuffer>(length);
    uint8_t *ptr = buf->base;

    if (entity->marked_for_removal())
      entity->header.encode( &ptr );
    else
      entity->encode_entry( &ptr );

    HT_ASSERT((ptr-buf->base) == (ptrdiff_t)buf->size);
  }

  shared_ptr<uint8_t> backup_buf(new uint8_t [length], default_delete<uint8_t[]>());
  memcpy(backup_buf.get(), buf->base, buf->size);

  // Add to entity map
  if (dynamic_cast<EntityRecover *>(entity.get()) == nullptr) {
    m_entity_map.erase(entity->header.id);
    if (!entity->marked_for_removal())
      m_entity_map[entity->header.id] = SerializedEntityT(length, backup_buf);
  }

  m_write_queue.push_back(buf);

  m_write_scheduler->schedule();

  m_cond.wait(lock);

  service_write_queue();
}

void Writer::record_state(std::vector<EntityPtr> &entities) {
  unique_lock<mutex> lock(m_mutex);
  shared_ptr<StaticBuffer> buffers( new StaticBuffer[entities.size()], default_delete<StaticBuffer[]>() );
  uint8_t *ptr;
  size_t length = 0;
  size_t total_length = 0;

  if (entities.empty())
    return;

  if (m_fd == -1)
    HT_THROWF(Error::CLOSED, "MetaLog '%s' has been closed", m_path.c_str());

  size_t i=0;
  for (auto & entity : entities) {
    Locker<Entity> lock(*entity);
    length = EntityHeader::LENGTH + (entity->marked_for_removal() ? 0 : entity->encoded_length());
    buffers.get()[i].set(new uint8_t [length], length);
    ptr = buffers.get()[i].base;
    if (entity->marked_for_removal())
      entity->header.encode( &ptr );
    else
      entity->encode_entry( &ptr );

    // Add to entity map
    HT_ASSERT(dynamic_cast<EntityRecover *>(entity.get()) == nullptr);
    m_entity_map.erase(entity->header.id);
    if (!entity->marked_for_removal()) {
      shared_ptr<uint8_t> backup_buf(new uint8_t [length], default_delete<uint8_t[]>());
      memcpy(backup_buf.get(), buffers.get()[i].base, length);
      m_entity_map[entity->header.id] = SerializedEntityT(length, backup_buf);
    }

    HT_ASSERT((ptr-buffers.get()[i].base) == (ptrdiff_t)buffers.get()[i].size);
    total_length += length;
    i++;
  }

  StaticBufferPtr buf = make_shared<StaticBuffer>(total_length);
  ptr = buf->base;
  for (i=0; i<entities.size(); i++) {
    memcpy(ptr, buffers.get()[i].base, buffers.get()[i].size);
    ptr += buffers.get()[i].size;
  }
  HT_ASSERT((ptr-buf->base) == (ptrdiff_t)buf->size);

  m_write_queue.push_back(buf);

  m_write_scheduler->schedule();

  m_cond.wait(lock);

  service_write_queue();

}

void Writer::record_removal(EntityPtr entity) {
  unique_lock<mutex> lock(m_mutex);
  StaticBufferPtr buf = make_shared<StaticBuffer>(EntityHeader::LENGTH);
  uint8_t *ptr = buf->base;

  if (m_fd == -1)
    HT_THROWF(Error::CLOSED, "MetaLog '%s' has been closed", m_path.c_str());

  entity->header.flags |= EntityHeader::FLAG_REMOVE;
  entity->header.length = 0;
  entity->header.checksum = 0;

  entity->header.encode( &ptr );

  HT_ASSERT((ptr-buf->base) == (ptrdiff_t)buf->size);

  // Remove from entity map
  m_entity_map.erase(entity->header.id);

  m_write_queue.push_back(buf);

  m_write_scheduler->schedule();

  m_cond.wait(lock);

  service_write_queue();
}


void Writer::record_removal(std::vector<EntityPtr> &entities) {
  unique_lock<mutex> lock(m_mutex);

  if (entities.empty())
    return;

  if (m_fd == -1)
    HT_THROWF(Error::CLOSED, "MetaLog '%s' has been closed", m_path.c_str());

  size_t length = entities.size() * EntityHeader::LENGTH;
  StaticBufferPtr buf = make_shared<StaticBuffer>(length);
  uint8_t *ptr = buf->base;

  for (auto &entity : entities) {
    entity->header.flags |= EntityHeader::FLAG_REMOVE;
    entity->header.length = 0;
    entity->header.checksum = 0;
    entity->header.encode( &ptr );
    // Remove from entity map
    m_entity_map.erase(entity->header.id);
  }

  HT_ASSERT((ptr-buf->base) == (ptrdiff_t)buf->size);

  m_write_queue.push_back(buf);

  m_write_scheduler->schedule();

  m_cond.wait(lock);

  service_write_queue();

}


void Writer::signal_write_ready() {
  unique_lock<mutex> lock(m_mutex);
  m_write_ready = true;
  m_cond.notify_all();
}
