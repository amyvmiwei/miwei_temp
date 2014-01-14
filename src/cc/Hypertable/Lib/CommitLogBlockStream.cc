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

/// @file
/// Definitions for CommitLogBlockStream.
/// This file contains definitions for CommitLogBlockStream, a class abstraction
/// for reading a stream of blocks from a commit log.

#include <Common/Compat.h>

#include "CommitLogBlockStream.h"

#include <Common/Checksum.h>
#include <Common/Error.h>
#include <Common/Logger.h>

#include <cctype>
#include <cstdlib>
#include <cstring>

using namespace Hypertable;

namespace {
  const uint32_t READAHEAD_BUFFER_SIZE = 131072;
  const uint32_t LatestVersion = 1;
  const uint32_t BlockHeaderVersions[LatestVersion+1] = { 0, 1 };
}

bool CommitLogBlockStream::ms_assert_on_error = true;


CommitLogBlockStream::CommitLogBlockStream(FilesystemPtr &fs)
  : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0) {
}


CommitLogBlockStream::CommitLogBlockStream(FilesystemPtr &fs,
    const String &log_dir, const String &fragment)
  : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0) {
  load(log_dir, fragment);
}


CommitLogBlockStream::~CommitLogBlockStream() {
  close();
}


void CommitLogBlockStream::load(const String &log_dir, const String &fragment) {
  if (m_fd != -1)
    close();
  m_fragment = fragment;
  m_fname = log_dir + "/" + fragment;
  m_log_dir = log_dir;
  m_file_length = m_fs->length(m_fname);
  m_fd = m_fs->open_buffered(m_fname, Filesystem::OPEN_FLAG_DIRECTIO,
          READAHEAD_BUFFER_SIZE, 2);

  if (!read_header(m_fs, m_fd, &m_version, &m_cur_offset)) {
    // If invalid header, assume file is in original format w/o header
    m_fs->close(m_fd);
    m_fd = m_fs->open_buffered(m_fname, Filesystem::OPEN_FLAG_DIRECTIO,
                               READAHEAD_BUFFER_SIZE, 2);
  }

  if (m_version > LatestVersion)
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER,
              "Unsupported commit log file version %u", (unsigned)m_version);

  BlockHeaderCommitLog temp(BlockHeaderVersions[m_version]);
  m_block_buffer.grow(temp.encoded_length());
}


void CommitLogBlockStream::close() {
  if (m_fd != -1) {
    try {
      m_fs->close(m_fd);
    }
    catch (Hypertable::Exception &e) {
      HT_ERRORF("Problem closing file %s - %s (%s)",
                m_fname.c_str(), e.what(), Error::get_text(e.code()));
    }
    m_fd = -1;
  }
}


bool
CommitLogBlockStream::next(CommitLogBlockInfo *infop,
                           BlockHeaderCommitLog *header) {
  uint32_t nread;

  assert(m_fd != -1);

  if (m_cur_offset >= m_file_length)
    return false;

  memset(infop, 0, sizeof(CommitLogBlockInfo));
  infop->log_dir = m_log_dir.c_str();
  infop->file_fragment = m_fragment.c_str();
  infop->start_offset = m_cur_offset;

  *header = BlockHeaderCommitLog( BlockHeaderVersions[m_version] );

  if ((infop->error = load_next_valid_header(header)) != Error::OK) {
    infop->end_offset = m_cur_offset;
    return false;
  }

  // check for truncation
  if ((m_file_length - m_cur_offset) < header->get_data_zlength()) {
    HT_WARNF("Commit log fragment '%s' truncated (entry start position %llu)",
             m_fname.c_str(), (Llu)(m_cur_offset-header->encoded_length()));
    infop->end_offset = m_file_length;
    infop->error = Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
    m_cur_offset = m_file_length;
    return true;
  }

  m_block_buffer.ensure(header->encoded_length() + header->get_data_zlength());

  nread = m_fs->read(m_fd, m_block_buffer.ptr, header->get_data_zlength());

  if (nread != header->get_data_zlength()) {
    HT_WARNF("Commit log fragment '%s' truncated (entry start position %llu)",
             m_fname.c_str(), (Llu)(m_cur_offset-header->encoded_length()));
    infop->end_offset = m_file_length;
    infop->error = Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
    m_cur_offset = m_file_length;
    return true;
  }

  m_block_buffer.ptr += nread;
  m_cur_offset += nread;
  infop->end_offset = m_cur_offset;
  infop->block_ptr = m_block_buffer.base;
  infop->block_len = m_block_buffer.fill();

  return true;
}

namespace {
  const size_t HEADER_SIZE=8;
}


bool
CommitLogBlockStream::read_header(FilesystemPtr &fs, int32_t fd,
                                  uint32_t *versionp, uint64_t *next_offset) {
  char buf[HEADER_SIZE];

  if (fs->read(fd, buf, HEADER_SIZE) != HEADER_SIZE) {
    *versionp = 0;
    return false;
  }

  // Sanity check header
  if (buf[0] != 'C' || buf[1] != 'L' || buf[6] != '\f' || buf[7] != '\n' ||
      !isdigit(buf[2]) || !isdigit(buf[3]) || !isdigit(buf[4]) ||
      !isdigit(buf[5])) {
    *versionp = 0;
    *next_offset = 0;
    return false;
  }

  *versionp = (uint32_t)atoi((const char *)&buf[2]);
  *next_offset = HEADER_SIZE;
  return true;
}



void
CommitLogBlockStream::write_header(FilesystemPtr &fs, int32_t fd) {
  String header_str = format("CL%04u\f\n", (unsigned)LatestVersion);
  StaticBuffer buf(HEADER_SIZE);
  HT_ASSERT(header_str.size() == HEADER_SIZE);
  memcpy(buf.base, header_str.c_str(), HEADER_SIZE);
  fs->append(fd, buf, Filesystem::O_FLUSH);
}

uint64_t CommitLogBlockStream::header_size() {
  return HEADER_SIZE;
}



int
CommitLogBlockStream::load_next_valid_header(
    BlockHeaderCommitLog *header) {
  size_t remaining = header->encoded_length();
  try {
    size_t nread = 0;
    size_t toread = header->encoded_length();

    m_block_buffer.ptr = m_block_buffer.base;

    while ((nread = m_fs->read(m_fd, m_block_buffer.ptr, toread)) < toread) {
      toread -= nread;
      m_block_buffer.ptr += nread;
    }

    m_block_buffer.ptr = m_block_buffer.base;
    header->decode((const uint8_t **)&m_block_buffer.ptr, &remaining);
    m_cur_offset += header->encoded_length();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (ms_assert_on_error)
      HT_ABORT;
    return e.code();
  }
  return Error::OK;
}
