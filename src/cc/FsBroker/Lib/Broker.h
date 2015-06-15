/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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

#ifndef FsBroker_Lib_Broker_h
#define FsBroker_Lib_Broker_h

#include "OpenFileMap.h"
#include "Response/Callback/Open.h"
#include "Response/Callback/Read.h"
#include "Response/Callback/Append.h"
#include "Response/Callback/Length.h"
#include "Response/Callback/Readdir.h"
#include "Response/Callback/Status.h"
#include "Response/Callback/Exists.h"

#include <Common/StaticBuffer.h>

#include <memory>

namespace Hypertable {

/// File system broker definitions
namespace FsBroker {

/// File system broker framework and client library
namespace Lib {

  /// @addtogroup FsBrokerLib
  /// @{

  /** Abstract class to be implemented by brokers
   */
  class Broker {
  public:
    virtual ~Broker() { return; }
    /**
     * Open a file and pass the fd to the callback on success.
     *
     * @param fname The filename to be opened.
     * @param flags An enum Hypertable::Filesystem::OpenFlags
     * @param bufsz Buffer size
     * @param cb
     */
    virtual void open(Response::Callback::Open *cb, const char *fname,
                      uint32_t flags, uint32_t bufsz) = 0;

    /**
     * Open a file, and create it if it doesn't exist, optionally
     * overwriting the contents. Fd is passed to the callback upon success.
     *
     * @param fname       The file to be opened and/or created.
     * @param flags       An enum Hypertable::Filesystem::OpenFlags. If OPEN_FLAG_OVERWRITE is set, then overwrite the file.
     * @param bufsz       Buffer size.
     * @param replication Amount of replication of file.
     * @param blksz       Block size of file.
     * @param cb
     */
    virtual void create(Response::Callback::Open *cb, const char *fname,
                        uint32_t flags, int32_t bufsz,
                        int16_t replication, int64_t blksz) = 0;

    /**
     * Close open file
     *
     * @param fd An open file descriptor of the file to close.
     * @param cb
     */
    virtual void close(ResponseCallback *cb, uint32_t fd) = 0;

    /**
     * Read data from an open file
     * @param fd An open file descriptor.
     * @param amount Number of bytes to read.
     * @param cb
     */
    virtual void read(Response::Callback::Read *cb, uint32_t fd,
                      uint32_t amount) = 0;

    /**
     * Append data to open file.
     * @param cb Response callback
     * @param fd An open file descriptor.
     * @param amount Number of bytes to write.
     * @param data   The data to write.
     * @param flags Flags (FLUSH or SYNC)
     */
    virtual void append(Response::Callback::Append *cb, uint32_t fd,
                        uint32_t amount, const void *data, Filesystem::Flags flags) = 0;

    /**
     * Seek open file.
     * @param fd An open file descriptor.
     * @param offset Position to seek to.
     * @param cb
     */
    virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) = 0;

    /**
     * Remove a file or directory
     * @param fname File to remove.
     * @param cb
     */
    virtual void remove(ResponseCallback *cb, const char *fname) = 0;

    /**
     * Get length of file.
     * @param fname File to get length of.
     * @param accurate Should the length be accurate or an estimation.
     * @param cb
     */
    virtual void length(Response::Callback::Length *cb, const char *fname,
                        bool accurate = true) = 0;

    /**
     * Read from file at position.
     * @param fd Open fd to read from.
     * @param offset Postion to read from.
     * @param amount Nubmer of bytes to read.
     * @param verify_checksum Verify checksum of data read
     * @param cb
     */
    virtual void pread(Response::Callback::Read *cb, uint32_t fd, uint64_t offset,
                       uint32_t amount, bool verify_checksum) = 0;


    /**
     * Make a directory hierarcy, If the parent dirs are not,
     * present, they are also created.
     *
     * @param dname The pathname
     * @param cb
     */
    virtual void mkdirs(ResponseCallback *cb, const char *dname) = 0;

    /**
     * Remove a directory
     *
     * @param dname The directory to be removed.
     * @param cb
     */
    virtual void rmdir(ResponseCallback *cb, const char *dname) = 0;

    /**
     * Read a directory's contents
     *
     * @param dname The directory to list.
     * @param cb
     */
    virtual void readdir(Response::Callback::Readdir *cb, const char *dname) = 0;


    /**
     * Flush data that has been written.
     *
     * @param fd An open file.
     * @param cb
     */
    virtual void flush(ResponseCallback *cb, uint32_t fd) = 0;

    /**
     * Sync out data that has been written.
     *
     * @param fd An open file.
     * @param cb
     */
    virtual void sync(ResponseCallback *cb, uint32_t fd) = 0;

    /**
     * Check status of FSBroker.
     * @param cb call cb->response_ok()
     */
    virtual void status(Response::Callback::Status *cb) = 0;

    /**
     * Gracefully shutdown broker, closeing open files.
     * @param cb Callbock to be called upon finishing task.
     */
    virtual void shutdown(ResponseCallback *cb) = 0;

    /**
     * Check for the existence of a file.
     *
     * @param cb Response callback
     * @param fname The file to be checked.
     */
    virtual void exists(Response::Callback::Exists *cb, const char *fname) = 0;

    /**
     * Rename a file from src to dst.
     *
     * @param src The file to be renamed.
     * @param dst The path the file should be renamed to.
     * @param cb
     */
    virtual void rename(ResponseCallback *cb, const char *src,
                        const char *dst) = 0;

    /**
     * Debug command
     * @param command
     * @param serialized_parameters
     * @param cb
     */
    virtual void debug(ResponseCallback *cb, int32_t command,
                       StaticBuffer &serialized_parameters) = 0;


    /**
     * @retval Returns a refrence to the open file map.
     */
    OpenFileMap &get_open_file_map() { return m_open_file_map; }

  protected:
    /**
     * A map of open files.
     * This map keeps track of files opened by each client, and
     * optionally their filename and other important information.
     */
    OpenFileMap m_open_file_map;
  };

  /// Smart pointer to Broker
  typedef std::shared_ptr<Broker> BrokerPtr;

  /// @}

}}}

#endif // FsBroker_Lib_Broker_h
