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

#ifndef HYPERTABLE_MAPRBROKER_H
#define HYPERTABLE_MAPRBROKER_H

#include <string>

extern "C" {
#include <unistd.h>
}

#include "Common/atomic.h"
#include "Common/String.h"
#include "Common/atomic.h"
#include "Common/Properties.h"

#include "FsBroker/Lib/Broker.h"

#include "hdfs.h"

namespace Hypertable {
  using namespace FsBroker;

  /**
   *
   */
  class OpenFileDataMapr : public OpenFileData {
  public:
  OpenFileDataMapr(hdfsFS _fs, const String &fname, hdfsFile _file, int _flags) 
    : fs(_fs), file(_file), flags(_flags), filename(fname) { }
    virtual ~OpenFileDataMapr() {
      HT_INFOF("hdfsCloseFile(%s)", filename.c_str());
      if (hdfsCloseFile(fs, file) != 0) {
        HT_ERRORF("Error closing file '%s' - %s", filename.c_str(), strerror(errno));
      }
    }
    hdfsFS fs;
    hdfsFile file;
    int  flags;
    String filename;
  };

  /**
   *
   */
  class OpenFileDataMaprPtr : public OpenFileDataPtr {
  public:
    OpenFileDataMaprPtr() : OpenFileDataPtr() { }
    OpenFileDataMaprPtr(OpenFileDataMapr *ofdl)
      : OpenFileDataPtr(ofdl, true) { }
    OpenFileDataMapr *operator->() const {
      return (OpenFileDataMapr *)get();
    }
  };


  /**
   *
   */
  class MaprBroker : public FsBroker::Broker {
  public:
    MaprBroker(PropertiesPtr &props);
    virtual ~MaprBroker();

    virtual void open(ResponseCallbackOpen *cb, const char *fname,
                      uint32_t flags, uint32_t bufsz);
    virtual void create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags,
           int32_t bufsz, int16_t replication, int64_t blksz);
    virtual void close(ResponseCallback *cb, uint32_t fd);
    virtual void read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount);
    virtual void append(ResponseCallbackAppend *cb, uint32_t fd,
                        uint32_t amount, const void *data, bool sync);
    virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset);
    virtual void remove(ResponseCallback *cb, const char *fname);
    virtual void length(ResponseCallbackLength *cb, const char *fname,
                        bool accurate = true);
    virtual void pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                       uint32_t amount, bool verify_checksum);
    virtual void mkdirs(ResponseCallback *cb, const char *dname);
    virtual void rmdir(ResponseCallback *cb, const char *dname);
    virtual void readdir(ResponseCallbackReaddir *cb, const char *dname);
    virtual void posix_readdir(ResponseCallbackPosixReaddir *cb,
            const char *dname);
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(ResponseCallback *cb);
    virtual void shutdown(ResponseCallback *cb);
    virtual void exists(ResponseCallbackExists *cb, const char *fname);
    virtual void rename(ResponseCallback *cb, const char *src, const char *dst);
    virtual void debug(ResponseCallback *, int32_t command,
                       StaticBuffer &serialized_parameters);


  private:

    static atomic_t ms_next_fd;

    virtual void report_error(ResponseCallback *cb);

    bool         m_verbose;
    hdfsFS       m_filesystem;
    bool         m_aggregate_writes;
    bool         m_readbuffering;
    String       m_namenode_host;
    uint16_t     m_namenode_port;
  };

}

#endif // HYPERTABLE_MAPRBROKER_H
