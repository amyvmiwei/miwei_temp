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
/// Declarations for CommitLogBlockStream.
/// This file contains declarations for CommitLogBlockStream, a class abstraction
/// for reading a stream of blocks from a commit log.

#ifndef HYPERTABLE_COMMITLOGBLOCKSTREAM_H
#define HYPERTABLE_COMMITLOGBLOCKSTREAM_H

#include <Hypertable/Lib/BlockHeaderCommitLog.h>

#include <Common/DynamicBuffer.h>
#include <Common/String.h>
#include <Common/Filesystem.h>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Holds information about an individual block
  struct CommitLogBlockInfo {
    /// Log directory
    const char *log_dir;
    /// File name of log fragment within #log_dir
    const char *file_fragment;
    /// Pointer to beginning of compressed block
    uint8_t *block_ptr;
    /// Length of block
    size_t block_len;
    /// Starting offset of block within fragment file
    uint64_t start_offset;
    /// Ending offset of block within fragment file
    uint64_t end_offset;
    /// %Error (if any) encountered while reading block
    int error;
  };


  /// Abstraction for reading a stream of blocks from a commit log file.
  class CommitLogBlockStream {

  public:

    CommitLogBlockStream(FilesystemPtr &fs);
    CommitLogBlockStream(FilesystemPtr &fs, const String &log_dir,
                         const String &fragment);
    virtual ~CommitLogBlockStream();

    void load(const String &log_dir, const String &fragment);
    void close();
    bool next(CommitLogBlockInfo *, BlockHeaderCommitLog *);

    String &get_fname() { return m_fname; }

    static bool ms_assert_on_error;

    /** Reads commit log file header.
     * This function assumes that <code>fd</code> is a valid file descriptor
     * for a commit log file that is opened for reading and positioned at the
     * beginning of the file.  It reads the header and extracts the version
     * number, saving it to <code>*versionp</code>, and sets
     * <code>*next_offset</code> to the file offset immediately following the
     * header.  If the header is invalid, it implies that the file is in the
     * original format that did not have the header.  In this case,
     * <code>*versionp</code> is set to zero, <code>*next_offset</code> is set
     * to zero, and <i>false</i> is returned.
     * @param fs Reference to filesystem object
     * @param fd File descriptor of newly opened commit log file
     * @param versionp Address of variable to hold version read from header
     * @param next_offset Address of file offset variable positioned to first
     *                    byte after the header
     * @return <i>true</i> if valid header was encountered, <i>false</i>
     * otherwise
     */
    static bool read_header(FilesystemPtr &fs, int32_t fd,
                            uint32_t *versionp, uint64_t *next_offset);

    /** Writes commit log file header.
     * This function assumes that <code>fd</code> is a valid file descriptor
     * for a commit log file that is opened for writing and positioned
     * at the beginning of the file.  The header that is written consists of
     * eight bytes and has the following format:
     * <pre>
     * vNNNN\f\n\0
     * </pre>
     * The <code>NNNN</code> component consists of four ASCII digits that
     * represent the latest version number.  The formfeed+newline allows the
     * verison number to be displayed by running the Unix <code>more</code> on
     * the file, which will display the version number and then pause the
     * output.
     * @param fs Reference to filesystem object
     * @param fd File descriptor of newly opened commit log file
     */
    static void write_header(FilesystemPtr &fs, int32_t fd);

    /** Size of header.
     * @return Size of header in bytes
     */
    static uint64_t header_size();

  private:

    int load_next_valid_header(BlockHeaderCommitLog *header);

    /// Pointer to filesystem
    FilesystemPtr m_fs;

    /// Directory containing commit log fragment file
    String m_log_dir;

    /// Fragment file name within commit log directory
    String m_fragment;

    /// Full pathname of commit log fragment file
    String m_fname;

    /// Version of commit log fragment file format
    uint32_t m_version;

    /// File descriptor
    int32_t m_fd;

    /// Current read offset within the fragment file
    uint64_t m_cur_offset;

    /// Length of commit log fragment file
    uint64_t m_file_length;

    /// Buffer holding most recently loaded block
    DynamicBuffer m_block_buffer;
  };

  /// @}
}

#endif // HYPERTABLE_COMMITLOGBLOCKSTREAM_H

