/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

#ifndef HYPERTABLE_QFSROKER_H
#define HYPERTABLE_QFSBROKER_H

#include <FsBroker/Lib/Broker.h>

#include <Common/String.h>
#include <Common/Properties.h>

#include <atomic>

extern "C" {
#include <unistd.h>
}

namespace KFS {
  class KfsClient;
}

namespace Hypertable {
  using namespace FsBroker;


  /**
   * A subclass of Hypertable::FsBroker::OpenFileData to hold the fields needed by the QfsBroker
   */
  class OpenFileDataQfs: public OpenFileData {
  public:
  OpenFileDataQfs(const std::string &fname, int fd, KFS::KfsClient *client)
    : fname(fname), fd(fd), m_client(client) {};
    virtual ~OpenFileDataQfs();
    std::string fname;
    int fd;
  private:
    KFS::KfsClient *const m_client;
  };


  /**
   */
  class OpenFileDataQfsPtr : public OpenFileDataPtr {
  public:
  OpenFileDataQfsPtr() : OpenFileDataPtr() { }
  OpenFileDataQfsPtr(OpenFileDataQfs *ofdq) : OpenFileDataPtr(ofdq, true) {}
    OpenFileDataQfs *operator->() const {
      return (OpenFileDataQfs*) get();
    }
  };

  /**
   * QfsBroker implementation class. @see Hypertable::FsBroker::Broker
   */
  class QfsBroker : public FsBroker::Broker {
  public:
    QfsBroker(PropertiesPtr& cfg);
    virtual ~QfsBroker();

    virtual void open(ResponseCallbackOpen *cb, const char *fname, uint32_t flags, uint32_t bufsz);
    virtual void close(ResponseCallback *cb, uint32_t fd);

    virtual void create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags, int32_t bufsz, int16_t replication, int64_t blksz);
    virtual void read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount);
    virtual void append(ResponseCallbackAppend *, uint32_t fd, uint32_t amount, const void *data, bool flush);
    virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset);
    virtual void remove(ResponseCallback *cb, const char *fname);
    virtual void length(ResponseCallbackLength *cb, const char *fname,
                        bool accurate = true);
    virtual void pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                       uint32_t amount, bool verify_checksum);
    virtual void mkdirs(ResponseCallback *cb, const char *dname);
    virtual void rmdir(ResponseCallback *cb, const char *dname);
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(ResponseCallback *cb);
    virtual void shutdown(ResponseCallback *cb);
    virtual void readdir(ResponseCallbackReaddir *cb, const char *dname);
    virtual void posix_readdir(ResponseCallbackPosixReaddir *cb,
			       const char *dname);
    virtual void exists(ResponseCallbackExists *cb, const char *fname);
    virtual void rename(ResponseCallback *cb, const char *src, const char *dst);
    virtual void debug(ResponseCallback *cb, int32_t command,
                       StaticBuffer &serialized_parameters);
  private:

    /// Atomic counter for file descriptor assignment
    static std::atomic<int> ms_next_fd;

    std::string m_host;
    int m_port;
    void report_error(ResponseCallback *cb, int error);
    KFS::KfsClient* const m_client;
  };
}
#endif // HYPERTABLE_QFSBROKER_H
