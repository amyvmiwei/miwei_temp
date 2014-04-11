/* -*- c++ -*-
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

/** @file
 * Abstract base class for a filesystem.
 * All commands have synchronous and asynchronous versions. Commands that
 * operate on the same file descriptor are serialized by the underlying
 * filesystem. In other words, if you issue three asynchronous commands,
 * they will get carried out and their responses will come back in the
 * same order in which they were issued. Unless otherwise mentioned, the
 * methods could throw Exception.
 */

#ifndef HYPERTABLE_FILESYSTEM_H
#define HYPERTABLE_FILESYSTEM_H

#include <vector>
#include "Common/String.h"
#include "Common/StaticBuffer.h"
#include "Common/ReferenceCount.h"
#include "AsyncComm/DispatchHandler.h"

#define HT_DIRECT_IO_ALIGNMENT      512

#define HT_IO_ALIGNED(size)                     \
    (((size) % HT_DIRECT_IO_ALIGNMENT) == 0)

#define HT_IO_ALIGNMENT_PADDING(size)           \
    (HT_DIRECT_IO_ALIGNMENT - ((size) % HT_DIRECT_IO_ALIGNMENT))

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Abstract base class for a filesystem.  All commands have synchronous and
   * asynchronous versions.  Commands that operate on the same file descriptor
   * are serialized by the underlying filesystem. In other words, if you issue
   * three asynchronous commands, they will get carried out and their responses
   * will come back in the same order in which they were issued. Unless other-
   * wise mentioned, the methods could throw Exception.
   *
   * This abstract base class is overwritten by the various FsBrokers.
   */
  class Filesystem : public ReferenceCount {
  public:
    enum OptionType { O_FLUSH = 1 };

    enum OpenFlags {
      OPEN_FLAG_DIRECTIO = 0x00000001,
      OPEN_FLAG_OVERWRITE = 0x00000002,
      OPEN_FLAG_VERIFY_CHECKSUM = 0x00000004
    };

    class Dirent {
    public:
      Dirent() : length(0), last_modification_time(0), is_dir(false) { }
      size_t encoded_length() const;
      void encode(uint8_t **bufp) const;
      void decode(const uint8_t **bufp, size_t *remainp);
      String name;
      uint64_t length;
      time_t last_modification_time;
      bool is_dir;
    };

    virtual ~Filesystem() { }

    /** Opens a file asynchronously.  Issues an open file request.  The caller
     * will get notified of successful completion or error via the given
     * dispatch handler. It is up to the caller to deserialize the returned
     * file descriptor from the MESSAGE event object.
     *
     * @param name Absolute path name of file to open
     * @param flags Open flags (OPEN_FLAG_DIRECTIO or 0)
     * @param handler The dispatch handler which will handle the reply
     */
    virtual void open(const String &name, uint32_t flags,
            DispatchHandler *handler) = 0;

    /** Opens a file.  Issues an open file request and waits for it to complete.
     *
     * @param name Absolute path name of file to open
     * @param flags Open flags (OPEN_FLAG_DIRECTIO or 0)
     * @return The new file handle
     */
    virtual int open(const String &name, uint32_t flags) = 0;

    /** Opens a file in buffered (readahead) mode.  Issues an open file request
     * and waits for it to complete. Turns on readahead mode so that data is
     * prefetched.
     *
     * @param name Absolute path name of file to open
     * @param flags Open flags (OPEN_FLAG_DIRECTIO or 0)
     * @param buf_size Read ahead buffer size
     * @param outstanding Maximum number of outstanding reads
     * @param start_offset Starting read offset
     * @param end_offset Ending read offset
     * @return The new file handle
     */
    virtual int open_buffered(const String &name, uint32_t flags,
            uint32_t buf_size, uint32_t outstanding, uint64_t start_offset = 0,
            uint64_t end_offset = 0) = 0;

    /** Decodes the response from an open request
     *
     * @param event_ptr reference to response event
     * @return file descriptor
     */
    static int decode_response_open(EventPtr &event_ptr);

    /** Creates a file asynchronously.  Issues a create file request with
     * various create mode parameters. The caller will get notified of
     * successful completion or error via the given dispatch handler.  It is
     * up to the caller to deserialize the returned file descriptor from the
     * MESSAGE event object.
     *
     * @param name Absolute path name of file to open
     * @param flags Open flags (OPEN_FLAG_DIRECTIO or OPEN_FLAG_OVERWRITE)
     * @param bufsz Buffer size to use for the underlying FS
     * @param replication Replication factor to use for this file
     * @param blksz Block size to use for the underlying FS
     * @param handler The dispatch handler which will handle the reply
     */
    virtual void create(const String &name, uint32_t flags, int32_t bufsz,
            int32_t replication, int64_t blksz, DispatchHandler *handler) = 0;

    /** Creates a file.  Issues a create file request and waits for completion
     *
     * @param name Absolute path name of file to open
     * @param flags Open flags (OPEN_FLAG_DIRECTIO or OPEN_FLAG_OVERWRITE)
     * @param bufsz Buffer size to use for the underlying FS
     * @param replication Replication factor to use for this file
     * @param blksz Block size to use for the underlying FS
     * @return The new file handle
     */
    virtual int create(const String &name, uint32_t flags, int32_t bufsz,
            int32_t replication, int64_t blksz) = 0;

    /** Decodes the response from a create request
     *
     * @param event_ptr Reference to the response event
     * @return The new file handle
     */
    static int decode_response_create(EventPtr &event_ptr);

    /** Closes a file asynchronously.  Issues a close file request.
     * The caller will get notified of successful completion or error via
     * the given dispatch handler.  This command will get serialized along
     * with other commands issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param handler The dispatch handler
     */
    virtual void close(int fd, DispatchHandler *handler) = 0;

    /** Closes a file.  Issues a close command and waits for it
     * to complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     */
    virtual void close(int fd) = 0;

    /** Reads data from a file at the current position asynchronously.  Issues
     * a read request.  The caller will get notified of successful completion or
     * error via the given dispatch handler.  It's up to the caller to
     * deserialize the returned data in the MESSAGE event object.  EOF is
     * indicated by a short read. This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param len Amount of data to read
     * @param handler The dispatch handler
     */
    virtual void read(int fd, size_t len, DispatchHandler *handler) = 0;

    /** Reads data from a file at the current position.  Issues a read
     * request and waits for it to complete, returning the read data.
     * EOF is indicated by a short read.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param dst The destination buffer for read data
     * @param len The amount of data to read
     * @return The amount read (in bytes)
     */
    virtual size_t read(int fd, void *dst, size_t len) = 0;

    /** Decodes the response from a read request
     *
     * @param event_ptr The reference to the response event
     * @param dst The destination buffer for read data
     * @param len The destination buffer size
     * @return The amount read (in bytes)
     */
    static size_t decode_response_read(EventPtr &event_ptr,
            void *dst, size_t len);

    /** Decodes the header from the response from a read request
     *
     * @param event_ptr The reference to a response event
     * @param offsetp The address of variable offset of data read.
     * @param dstp The address of pointer to hold pointer to data read
     * @return The amount read (in bytes)
     */
    static size_t decode_response_read_header(EventPtr &event_ptr,
            uint64_t *offsetp, uint8_t **dstp = 0);

    /** Appends data to a file asynchronously.  Issues an append request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.  This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param buffer The buffer to append
     * @param flags Flags for this operation: O_FLUSH or 0
     * @param handler The dispatch handler
     */
    virtual void append(int fd, StaticBuffer &buffer, uint32_t flags,
            DispatchHandler *handler) = 0;

    /**
     * Appends data to a file.  Issues an append request and waits for it to
     * complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param buffer The buffer to append
     * @param flags Flags for this operation: O_FLUSH or 0
     */
    virtual size_t append(int fd, StaticBuffer &buffer, uint32_t flags = 0) = 0;

    /** Decodes the response from an append request
     *
     * @param event_ptr A reference to the response event
     * @param offsetp The address of variable to hold offset
     * @return The amount appended (in bytes)
     */
    static size_t decode_response_append(EventPtr &event_ptr,
            uint64_t *offsetp);

    /** Seeks current file position asynchronously.  Issues a seek request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.  This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param offset The absolute offset to seek to
     * @param handler The dispatch handler
     */
    virtual void seek(int fd, uint64_t offset, DispatchHandler *handler) = 0;

    /** Seeks current file position.  Issues a seek request and waits for it to
     * complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param offset The absolute offset to seek to
     */
    virtual void seek(int fd, uint64_t offset) = 0;

    /** Removes a file asynchronously.  Issues a remove request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name The absolute pathname of file to delete
     * @param handler The dispatch handler
     */
    virtual void remove(const String &name, DispatchHandler *handler) = 0;

    /** Removes a file.  Issues a remove request and waits for it to
     * complete.
     *
     * @param name The absolute pathname of file to delete
     * @param force If true then ignore non-existence error
     */
    virtual void remove(const String &name, bool force = true) = 0;

    /** Gets the length of a file asynchronously.  Issues a length request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name The absolute pathname of file
     * @param accurate Whether the accurate or an estimated file length
     *      is required (an hdfs performance optimization)
     * @param handler The dispatch handler
     */
    virtual void length(const String &name, bool accurate,
            DispatchHandler *handler) = 0;

    /** Gets the length of a file.  Issues a length request and waits for it
     * to complete.
     *
     * @param name The absolute pathname of file
     * @param accurate Whether the accurate or an estimated file length
     *      is required (an hdfs performance optimization)
     */
    virtual int64_t length(const String &name, bool accurate = true) = 0;

    /** Decodes the response from a length request
     *
     * @param event_ptr Reference to response event
     * @return length of the file, in bytes
     */
    static int64_t decode_response_length(EventPtr &event_ptr);

    /** Reads data from a file at the specified position asynchronously.
     * Issues a pread request.  The caller will get notified of successful
     * completion or error via the given dispatch handler.  It's up to the
     * caller to deserialize the returned data in the MESSAGE event object.
     * EOF is indicated by a short read.
     *
     * @param fd The open file descriptor
     * @param offset The starting offset of read
     * @param amount The amount of data to read (in bytes)
     * @param handler The dispatch handler
     */
    virtual void pread(int fd, size_t amount, uint64_t offset,
            DispatchHandler *handler) = 0;

    /** Reads data from a file at the specified position.  Issues a pread
     * request and waits for it to complete, returning the read data.
     * EOF is indicated by a short read.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd The open file descriptor
     * @param dst The destination buffer for read data
     * @param len The amount of data to read
     * @param offset starting The offset of read
     * @param verify_checksum Tells filesystem to perform checksum verification
     * @return The amount of data read (in bytes)
     */
    virtual size_t pread(int fd, void *dst, size_t len, uint64_t offset,
            bool verify_checksum = true) = 0;

    /** Decodes the response from a pread request
     *
     * @param event_ptr A reference to the response event
     * @param dst The destination buffer for read data
     * @param len The destination buffer size
     * @return The amount of data read
     */
    static size_t decode_response_pread(EventPtr &event_ptr, void *dst,
            size_t len);

    /** Creates a directory asynchronously.  Issues a mkdirs request which
     * creates a directory, including all its missing parents.  The caller
     * will get notified of successful completion or error via the given
     * dispatch handler.
     *
     * @param name The absolute pathname of directory to create
     * @param handler The dispatch handler
     */
    virtual void mkdirs(const String &name, DispatchHandler *handler) = 0;

    /** Creates a directory.  Issues a mkdirs request which creates a directory,
     * including all its missing parents, and waits for it to complete.
     *
     * @param name The absolute pathname of the directory to create
     */
    virtual void mkdirs(const String &name) = 0;

    /** Recursively removes a directory asynchronously.  Issues a rmdir request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name The absolute pathname of directory to remove
     * @param handler The dispatch handler
     */
    virtual void rmdir(const String &name, DispatchHandler *handler) = 0;

    /** Recursively removes a directory.  Issues a rmdir request and waits for
     * it to complete.
     *
     * @param name The absolute pathname of directory to remove
     * @param force If true then don't throw an error if file does not exist
     */
    virtual void rmdir(const String &name, bool force = true) = 0;

    /** Obtains a listing of all files in a directory asynchronously.  Issues a
     * readdir request.  The caller will get notified of successful completion
     * or error via the given dispatch handler.
     *
     * @param name The absolute pathname of directory
     * @param handler The dispatch handler
     */
    virtual void readdir(const String &name, DispatchHandler *handler) = 0;

    /** Obtains a listing of all files in a directory.  Issues a readdir request
     * and waits for it to complete.
     *
     * @param name Absolute pathname of directory
     * @param listing Reference to output vector of Dirent objects for each entry
     */
    virtual void readdir(const String &name, std::vector<Dirent> &listing) = 0;

    /** Decodes the response from a readdir request
     *
     * @param event_ptr A reference to the response event
     * @param listing Reference to output vector of Dirent objects for each entry
     */
    static void decode_response_readdir(EventPtr &event_ptr,
					std::vector<Dirent> &listing);

    /** directory entries for the posix_readdir request */
    struct DirectoryEntry {
      String name;
      uint32_t flags;
      uint32_t length;
    };

    /** DirectoryEntry is a directory */
    static const int DIRENT_DIRECTORY = 1;

    /** Obtains a listing of all files in a directory, including additional
     * information for each file. Issues a posix_readdir request
     * and waits for it to complete.
     *
     * @param name absolute pathname of directory
     * @param listing reference to vector of directory entries
     */
    virtual void posix_readdir(const String &name,
            std::vector<DirectoryEntry> &listing) = 0;

    /** Decodes the response from a posix-style readdir request
     *
     * @param event_ptr reference to response event
     * @param listing reference to vector of DirectoryEntries
     */
    static void decode_response_posix_readdir(EventPtr &event_ptr,
                                        std::vector<DirectoryEntry> &listing);

    /** Flushes a file asynchronously.  Isues a flush command which causes all
     * buffered writes to get persisted to disk.  The caller will get notified
     * of successful completion or error via the given dispatch handler.  This
     * command will get serialized along with other commands issued with the
     * same file descriptor.
     *
     * @param fd The open file descriptor
     * @param handler The dispatch handler
     */
    virtual void flush(int fd, DispatchHandler *handler) = 0;

    /** Flushes a file.  Isues a flush command which causes all buffered
     * writes to get persisted to disk, and waits for it to complete.
     * This command will get serialized along with other commands issued
     * with the same file descriptor.
     *
     * @param fd The open file descriptor
     */
    virtual void flush(int fd) = 0;

    /** Determines if a file exists asynchronously.  Issues an exists request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name The absolute pathname of file
     * @param handler The dispatch handler
     */
    virtual void exists(const String &name, DispatchHandler *handler) = 0;

    /** Determines if a file exists.
     *
     * @param name The absolute pathname of the file
     * @return true if the file exists, otherwise false
     */
    virtual bool exists(const String &name) = 0;

    /** Decodes the response from an exists request.
     *
     * @param event_ptr A reference to the response event
     * @return true if the file exists, otherwise false
     */
    static bool decode_response_exists(EventPtr &event_ptr);

    /** Rename a path asynchronously
     *
     * @param src The source path
     * @param dst The destination path
     * @param handler The dispatch/callback handler
     */
    virtual void rename(const String &src, const String &dst,
            DispatchHandler *handler) = 0;

    /** Rename a path
     *
     * @param src The source path
     * @param dst The destination path
     */
    virtual void rename(const String &src, const String &dst) = 0;

    /** Decodes the response from an request that only returns an error code
     *
     * @param event_ptr A reference to the response event
     * @return The error code
     */
    static int decode_response(EventPtr &event_ptr);

    /** Invokes debug request asynchronously
     *
     * @param command debug command identifier
     * @param serialized_parameters command specific serialized parameters
     */
    virtual void debug(int32_t command,
            StaticBuffer &serialized_parameters) = 0;

    /** Invokes debug request
     *
     * @param command The debug command identifier
     * @param serialized_parameters The command specific serialized parameters
     * @param handler The dispatch/callback handler
     */
    virtual void debug(int32_t command, StaticBuffer &serialized_parameters,
            DispatchHandler *handler) = 0;

    /** 
     * A posix-compliant dirname() which strips the last component from a
     * file name
     *
     *     /usr/bin/ -> /usr
     *     stdio.h   -> .
     *
     * @param name The directory name
     * @param separator The path separator
     * @return The stripped directory
     */
    static String dirname(String name, char separator = '/');

    /** 
     * A posix-compliant basename() which strips directory names from a
     * filename
     *
     *    /usr/bin/sort -> sort
     *
     * @param name The directory name
     * @param separator The path separator
     * @return The basename
     */
    static String basename(String name, char separator = '/');
  };

  typedef intrusive_ptr<Filesystem> FilesystemPtr;

  inline bool operator< (const Filesystem::Dirent& lhs,
			 const Filesystem::Dirent& rhs) {
    return lhs.name.compare(rhs.name) < 0;
  }

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_FILESYSTEM_H

