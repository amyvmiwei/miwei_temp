/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#ifndef FsBroker_qfs_QfsBroker_h
#define FsBroker_qfs_QfsBroker_h

#include <FsBroker/Lib/Broker.h>
#include <FsBroker/Lib/MetricsHandler.h>

#include <Common/Properties.h>
#include <Common/String.h>

#include <atomic>
#include <string>

extern "C" {
#include <unistd.h>
}

namespace KFS {
  class KfsClient;
}

namespace Hypertable {
namespace FsBroker {
  using namespace Lib;


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


  class OpenFileDataQfsPtr : public OpenFileDataPtr {
  public:
  OpenFileDataQfsPtr() : OpenFileDataPtr() { }
  OpenFileDataQfsPtr(OpenFileDataQfs *ofdq) : OpenFileDataPtr(ofdq, true) {}
    OpenFileDataQfs *operator->() const {
      return (OpenFileDataQfs*) get();
    }
  };

  class QfsBroker : public FsBroker::Broker {
  public:
    QfsBroker(PropertiesPtr& cfg);
    virtual ~QfsBroker();

    virtual void open(Response::Callback::Open *cb, const char *fname, uint32_t flags, uint32_t bufsz);
    virtual void close(ResponseCallback *cb, uint32_t fd);

    virtual void create(Response::Callback::Open *cb, const char *fname, uint32_t flags, int32_t bufsz, int16_t replication, int64_t blksz);
    virtual void read(Response::Callback::Read *cb, uint32_t fd, uint32_t amount);
    virtual void append(Response::Callback::Append *, uint32_t fd, uint32_t amount, const void *data, bool flush);
    virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset);
    virtual void remove(ResponseCallback *cb, const char *fname);
    virtual void length(Response::Callback::Length *cb, const char *fname,
                        bool accurate = true);
    virtual void pread(Response::Callback::Read *cb, uint32_t fd, uint64_t offset,
                       uint32_t amount, bool verify_checksum);
    virtual void mkdirs(ResponseCallback *cb, const char *dname);
    virtual void rmdir(ResponseCallback *cb, const char *dname);
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(Response::Callback::Status *cb);
    virtual void shutdown(ResponseCallback *cb);
    virtual void readdir(Response::Callback::Readdir *cb, const char *dname);
    virtual void exists(Response::Callback::Exists *cb, const char *fname);
    virtual void rename(ResponseCallback *cb, const char *src, const char *dst);
    virtual void debug(ResponseCallback *cb, int32_t command,
                       StaticBuffer &serialized_parameters);
  private:

    /// Atomic counter for file descriptor assignment
    static std::atomic<int> ms_next_fd;

    /// Metrics collection handler
    MetricsHandlerPtr m_metrics_handler;

    std::string m_host;
    int m_port;
    void report_error(ResponseCallback *cb, int error);
    KFS::KfsClient* const m_client;
  };
}}

#endif // FsBroker_qfs_QfsBroker_h
