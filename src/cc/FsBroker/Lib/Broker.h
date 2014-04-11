/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/ReferenceCount.h"
#include "Common/StaticBuffer.h"

#include "FsBroker/Lib/OpenFileMap.h"

#include "ResponseCallbackOpen.h"
#include "ResponseCallbackRead.h"
#include "ResponseCallbackAppend.h"
#include "ResponseCallbackLength.h"
#include "ResponseCallbackReaddir.h"
#include "ResponseCallbackPosixReaddir.h"
#include "ResponseCallbackExists.h"


namespace Hypertable {

  namespace FsBroker {

    /** Abstract class to be implemented by brokers
     */
    class Broker : public ReferenceCount {
    public:
      virtual ~Broker() { return; }
      /**
       * Open a file and pass the fd to the callback on success.
       *
       * @param[in]  fname The filename to be opened.
       * @param[in]  flags An enum Hypertable::Filesystem::OpenFlags
       * @param[in]  bufsz Buffer size
       *
       * @param[out] cb
       */
      virtual void open(ResponseCallbackOpen *cb, const char *fname,
                        uint32_t flags, uint32_t bufsz) = 0;

      /**
       * Open a file, and create it if it doesn't exist, optionally
       * overwriting the contents. Fd is passed to the callback upon success.
       *
       * @param[in]  fname       The file to be opened and/or created.
       * @param[in]  flags       An enum Hypertable::Filesystem::OpenFlags. If OPEN_FLAG_OVERWRITE is set, then overwrite the file.
       * @param[in]  bufsz       Buffer size.
       * @param[in]  replication Amount of replication of file.
       * @param[in]  blksz       Block size of file.
       *
       * @param[out] cb
       */
      virtual void create(ResponseCallbackOpen *cb, const char *fname,
                          uint32_t flags, int32_t bufsz,
                          int16_t replication, int64_t blksz) = 0;

      /**
       * Close open file
       *
       * @param[in]  fd An open file descriptor of the file to close.
       * @param[out] cb
       */
      virtual void close(ResponseCallback *cb, uint32_t fd) = 0;

      /**
       * Read data from an open file
       *
       * @param[in]  fd     An open file descriptor.
       * @param[in]  amount Number of bytes to read.
       * @param[out] cb
       */
      virtual void read(ResponseCallbackRead *cb, uint32_t fd,
                        uint32_t amount) = 0;

      /**
       * Append data to open file.
       *
       * @param[in]  fd     An open file descriptor.
       * @param[in]  amount Number of bytes to write.
       * @param[in]  data   The data to write.
       * @param[in]  flush  Sync data to disk.
       * @param[out] cb
       */
      virtual void append(ResponseCallbackAppend *cb, uint32_t fd,
                          uint32_t amount, const void *data, bool flush) = 0;

      /**
       * Seek open file.
       *
       * @param[in]  fd     An open file descriptor.
       * @param[in]  offset Position to seek to.
       * @param[out] cb
       */
      virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) = 0;

      /**
       * Remove a file or directory
       *
       * @param[in]  fname File to remove.
       * @param[out] cb
       */
      virtual void remove(ResponseCallback *cb, const char *fname) = 0;

      /**
       * Get length of file.
       *
       * @param[in]  fname    File to get length of.
       * @param[in]  accurate Should the length be accurate or an estimation.
       * @param[out] cb
       */
      virtual void length(ResponseCallbackLength *cb, const char *fname,
                        bool accurate = true) = 0;

      /**
       * Read from file at position.
       *
       * @param[in]  fd       Open fd to read from.
       * @param[in]  offset   Postion to read from.
       * @param[in]  amount   Nubmer of bytes to read.
       * @param[in]  verify_checksum 
       * @param[out] cb
       */
      virtual void pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                         uint32_t amount, bool verify_checksum) = 0;


      ///TODO: document this
      virtual void posix_readdir(ResponseCallbackPosixReaddir *,
                        const char *dname) = 0;

      /**
       * Make a directory hierarcy, If the parent dirs are not,
       * present, they are also created.
       *
       * @param[in]  dname The pathname
       * @param[out] cb
       */
      virtual void mkdirs(ResponseCallback *cb, const char *dname) = 0;

      /**
       * Remove a directory
       *
       * @param[in]  dname The directory to be removed.
       * @param[out] cb
       */
      virtual void rmdir(ResponseCallback *cb, const char *dname) = 0;

      /**
       * Read a directory's contents
       *
       * @param[in]  dname The directory to list.
       * @param[out] cb
       */
      virtual void readdir(ResponseCallbackReaddir *cb, const char *dname) = 0;


      /**
       * Sync out data that has been written.
       *
       * @param[in]  fd An open file.
       * @param[out] cb
       */
      virtual void flush(ResponseCallback *cb, uint32_t fd) = 0;

      /**
       * Check status of FSBroker.
       * @param[out] cb call cb->response_ok()
       */
      virtual void status(ResponseCallback *cb) = 0;

      /**
       * Gracefully shutdown broker, closeing open files.
       * @param[out] cb Callbock to be called upon finishing task.
       */
      virtual void shutdown(ResponseCallback *cb) = 0;

      /**
       * Check for the existence of a file.
       *
       * @param[in]  fname The file to be checked.
       */
      virtual void exists(ResponseCallbackExists *cb, const char *fname) = 0;

      /**
       * Rename a file from src to dst.
       *
       * @param[in]  src The file to be renamed.
       * @param[in]  dst The path the file should be renamed to.
       * @param[out] cb
       */
      virtual void rename(ResponseCallback *cb, const char *src,
                          const char *dst) = 0;

      /**
       * Debug command
       * @param[in]  command
       * @param[in]  serialized_parameters
       * @param[out] cb
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
    typedef boost::intrusive_ptr<Broker> BrokerPtr;

  }
}

#endif // FsBroker_Lib_Broker_h
