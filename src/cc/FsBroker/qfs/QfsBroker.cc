/*
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
#include <Common/Compat.h>
#include "QfsBroker.h"

#include <Common/Filesystem.h>
#include <Common/System.h>

#include <kfs/KfsClient.h>

#include <cerrno>
#include <cstring>
#include <cstdlib>

extern "C" {
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <time.h>
}

using namespace Hypertable;
using namespace KFS;

std::atomic<int> QfsBroker::ms_next_fd {};

OpenFileDataQfs::~OpenFileDataQfs() {
  HT_INFOF("close(%d) file: %s", fd, fname.c_str());
  m_client->Close(fd);
}

QfsBroker::QfsBroker(PropertiesPtr &cfg)
    : m_host(cfg->get_str("host")),
    m_port(cfg->get_i16("port")),
    m_client(KFS::Connect(m_host, m_port)) {
}

QfsBroker::~QfsBroker() {
  delete m_client;
}

void QfsBroker::open(ResponseCallbackOpen *cb, const char *fname, uint32_t flags, uint32_t bufsz) {
  
  if (flags & Filesystem::OPEN_FLAG_VERIFY_CHECKSUM) {
    int status = m_client->VerifyDataChecksums(fname);
    if (status < 0)
      HT_WARNF("Checksum verification of %s failed - %s", fname,
               KFS::ErrorCodeToStr(status).c_str());
  }

  int qfs_fd = m_client->Open(fname, O_RDONLY);
  if (qfs_fd < 0) {
    HT_ERRORF("open(%s) failure (%d) - %s", fname, -qfs_fd, KFS::ErrorCodeToStr(qfs_fd).c_str());
    report_error(cb, qfs_fd);
  }
  else {
    int fd = ++ms_next_fd;
    HT_INFOF("open(%s) -> fd=%d, qfs_fd=%d", fname, fd, qfs_fd);
    struct sockaddr_in addr;
    cb->get_address(addr);
    OpenFileDataQfsPtr fdata(new OpenFileDataQfs(fname, qfs_fd, m_client));
    m_open_file_map.create(fd, addr, fdata);
    if(bufsz)
      m_client->SetIoBufferSize(qfs_fd, bufsz);
    cb->response(fd);
  }

}

void QfsBroker::close(ResponseCallback *cb, uint32_t fd) {
  HT_DEBUGF("close(%d)", fd);
  m_open_file_map.remove(fd);
  int error;
  if ((error = cb->response_ok()) != Error::OK)
    HT_ERRORF("Problem sending response for close(fd=%d) - %s", (int)fd, Error::get_text(error));
}

void QfsBroker::create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags, int32_t bufsz, int16_t replication, int64_t blksz) {
  int qfs_fd;
  if(flags & Filesystem::OPEN_FLAG_OVERWRITE) {
    qfs_fd = m_client->Open(fname, O_CREAT | O_TRUNC | O_RDWR);
  }
  else
    qfs_fd = m_client->Open(fname, O_CREAT|O_WRONLY);

  if (qfs_fd < 0) {
    HT_ERRORF("create(%s) failure (%d) - %s", fname, -qfs_fd, KFS::ErrorCodeToStr(qfs_fd).c_str());
    report_error(cb, qfs_fd);
  }
  else {
    int fd = ++ms_next_fd;
    HT_INFOF("open(%s) -> fd=%d, qfs_fd=%d", fname, fd, qfs_fd);
    struct sockaddr_in addr;
    cb->get_address(addr);
    OpenFileDataQfsPtr fdata(new OpenFileDataQfs(fname, qfs_fd, m_client));
    m_open_file_map.create(fd, addr, fdata);
    if(bufsz)
      m_client->SetIoBufferSize(qfs_fd, bufsz);
    cb->response(fd);
  }
}

void QfsBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {

  OpenFileDataQfsPtr fdata;
  if (!m_open_file_map.get(fd, fdata)) {
    cb->error(Error::FSBROKER_BAD_FILE_HANDLE, format("%d", (int)fd));
    return;
  }

  chunkOff_t status = m_client->Seek(fdata->fd, offset);
  if(status == (chunkOff_t)-1) {
    HT_ERRORF("seek(fd=%d,qfsFd=%d,%lld) failure (%d) - %s", (int)fd,
              (int)fdata->fd, (Lld)offset, (int)-status,
              KFS::ErrorCodeToStr(status).c_str());
    report_error(cb, status);
  }
  else
    cb->response_ok();
}

void QfsBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {

  OpenFileDataQfsPtr fdata;
  if (!m_open_file_map.get(fd, fdata)) {
    cb->error(Error::FSBROKER_BAD_FILE_HANDLE, format("%d", (int)fd));
    return;
  }

  uint64_t offset = m_client->Tell(fdata->fd);

  StaticBuffer buf((size_t)amount, (size_t)HT_DIRECT_IO_ALIGNMENT);
  int len = m_client->Read(fdata->fd, reinterpret_cast<char*>(buf.base), amount);
  if(len<0) {
    HT_ERRORF("read(fd=%d,qfsFd=%d,%lld) failure (%d) - %s", (int)fd,
              (int)fdata->fd, (Lld)amount, -len,
              KFS::ErrorCodeToStr(len).c_str());
    report_error(cb, len);
  }
  else {
    buf.size = (uint32_t)len;
    cb->response(offset, buf);
  }
}

void QfsBroker::append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, const void *data, bool flush) {

  OpenFileDataQfsPtr fdata;
  if (!m_open_file_map.get(fd, fdata)) {
    cb->error(Error::FSBROKER_BAD_FILE_HANDLE, format("%d", (int)fd));
    return;
  }

  uint64_t offset = m_client->Tell(fdata->fd);
  ssize_t written = m_client->Write(fdata->fd, reinterpret_cast<const char*>(data), amount);
  if(written < 0) {
    HT_ERRORF("append(fd=%d,qfsFd=%d,%lld,%s) failure (%d) - %s", (int)fd,
              (int)fdata->fd, (Lld)amount, flush ? "flush" : "",
              (int)-written, KFS::ErrorCodeToStr(written).c_str());
    report_error(cb, written);
  }
  else {
    if(flush) {
      int error = m_client->Sync(fdata->fd);
      if(error) {
        HT_ERRORF("append(fd=%d,qfsFd=%d,%lld,%s) failure (%d) - %s", (int)fd,
                  (int)fdata->fd, (Lld)amount, flush ? "flush" : "",
                  (int)-written, KFS::ErrorCodeToStr(written).c_str());
        return report_error(cb, error);
      }
    }
    cb->response(offset, written);
  }
}

void QfsBroker::remove(ResponseCallback *cb, const char *fname) {
  int status = m_client->Remove(fname);
  if(status == 0)
    cb->response_ok();
  else {
    HT_ERRORF("remove(%s) failure (%d) - %s", fname, -status, KFS::ErrorCodeToStr(status).c_str());
    report_error(cb, status);
  }
}

void QfsBroker::length(ResponseCallbackLength *cb, const char *fname, bool accurate)
{
  KfsFileAttr result;
  int err = m_client->Stat(fname, result);
  if(err == 0)
    cb->response(result.fileSize);
  else {
    HT_ERRORF("length(%s) failure (%d) - %s", fname, -err, KFS::ErrorCodeToStr(err).c_str());
    report_error(cb, err);
  }
}

void QfsBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                      uint32_t amount, bool verify_checksum) {

  OpenFileDataQfsPtr fdata;
  if (!m_open_file_map.get(fd, fdata)) {
    cb->error(Error::FSBROKER_BAD_FILE_HANDLE, format("%d", (int)fd));
    return;
  }

  if (verify_checksum) {
    int status = m_client->VerifyDataChecksums(fdata->fd);
    if (status < 0)
      HT_WARNF("Checksum verification of fd=%d (qfsFd=%d) failed - %s", fd,
               fdata->fd, KFS::ErrorCodeToStr(status).c_str());
  }

  StaticBuffer buf((size_t)amount, (size_t)HT_DIRECT_IO_ALIGNMENT);
  ssize_t status = m_client->PRead(fdata->fd, offset,
                                   reinterpret_cast<char*>(buf.base), amount);
  if(status < 0) {
    HT_ERRORF("pread(fd=%d,qfsFd=%d,%lld,%lld) failure (%d) - %s", (int)fd,
              (int)fdata->fd, (Lld)offset, (Lld)amount, (int)-status,
              KFS::ErrorCodeToStr(status).c_str());
    report_error(cb, status);
  }
  else {
    buf.size = (uint32_t)status;
    cb->response(offset, buf);
  }
}

void QfsBroker::mkdirs(ResponseCallback *cb, const char *dname) {
  int status = m_client->Mkdirs(dname);
  if(status < 0) {
    HT_ERRORF("mkdirs(%s) failure (%d) - %s", dname, -status, KFS::ErrorCodeToStr(status).c_str());
    report_error(cb, status);
  }
  else
    cb->response_ok();
}

void QfsBroker::rmdir(ResponseCallback *cb, const char *dname) {
  int status = m_client->Rmdirs(dname);
  if(status < 0 && status != -ENOENT) {
    HT_ERRORF("rmdir(%s) failure (%d) - %s", dname, -status, KFS::ErrorCodeToStr(status).c_str());
    report_error(cb, status);
  }
  else
    cb->response_ok();
}

void QfsBroker::flush(ResponseCallback *cb, uint32_t fd) {

  OpenFileDataQfsPtr fdata;
  if (!m_open_file_map.get(fd, fdata)) {
    cb->error(Error::FSBROKER_BAD_FILE_HANDLE, format("%d", (int)fd));
    return;
  }

  int status = m_client->Sync(fdata->fd);
  if(status < 0) {
    HT_ERRORF("flush(fd=%d,qfsFd=%d) failure (%d) - %s", (int)fd,
              (int)fdata->fd, -status,
              KFS::ErrorCodeToStr(status).c_str());
    report_error(cb, status);
  }
  else
    cb->response_ok();
}

void QfsBroker::readdir(ResponseCallbackReaddir *cb, const char *dname) {
  std::vector<KfsFileAttr> result;
  std::vector<Filesystem::Dirent> listing;
  int err = m_client->ReaddirPlus(dname, result);

  Filesystem::Dirent entry;
  for (std::vector<KfsFileAttr>::iterator it = result.begin();
       it != result.end(); ++it) {
    if(it->filename != "." && it->filename != "..") {
      entry.name.clear();
      entry.name.append(it->filename);
      entry.is_dir = it->isDirectory;
      entry.length = (uint64_t)it->fileSize;
      entry.last_modification_time = it->mtime.tv_sec;
      listing.push_back(entry);
    }
  }

  if(err == 0)
    cb->response(listing);
  else {
    HT_ERRORF("readdir(%s) failure (%d) - %s", dname, -err, KFS::ErrorCodeToStr(err).c_str());
    report_error(cb,err);
  }
}

void QfsBroker::posix_readdir(ResponseCallbackPosixReaddir *cb,
			      const char *dname) {
  HT_ASSERT(!"posix_readdir() not yet implemented.");
}


void QfsBroker::exists(ResponseCallbackExists *cb, const char *fname) {
  cb->response(m_client->Exists(fname));
}

void QfsBroker::rename(ResponseCallback *cb, const char *src, const char *dst) {
  int err = m_client->Rename(src, dst);
  if(err == 0)
    cb->response_ok();
  else {
    HT_ERRORF("rename(%s,%s) failure (%d) - %s", src, dst, -err, KFS::ErrorCodeToStr(err).c_str());
    report_error(cb, err);
  }
}

void QfsBroker::debug(ResponseCallback *cb, int32_t command, StaticBuffer &serialized_parameters) {
  cb->error(Error::NOT_IMPLEMENTED, format("Unsupported debug command - %d",
                                           command));
}

void QfsBroker::status(ResponseCallback *cb) {
  cb->response_ok();
}

void QfsBroker::shutdown(ResponseCallback *cb) {
  m_open_file_map.remove_all();
  cb->response_ok();
}

void QfsBroker::report_error(ResponseCallback *cb, int error) {
  string errors = KFS::ErrorCodeToStr(error);
  switch(-error) {
  case ENOTDIR:
  case ENAMETOOLONG:
  case ENOENT:
    cb->error(Error::FSBROKER_BAD_FILENAME, errors);
    break;

  case EACCES:
  case EPERM:
    cb->error(Error::FSBROKER_PERMISSION_DENIED, errors);
    break;

  case EBADF:
    cb->error(Error::FSBROKER_BAD_FILE_HANDLE, errors);
    break;

  case EINVAL:
    cb->error(Error::FSBROKER_INVALID_ARGUMENT, errors);
    break;

  default:
    cb->error(Error::FSBROKER_IO_ERROR, errors);
    break;
  }

#ifndef  NDEBUG
  std::clog << "ERROR " << errors << std::endl;
#endif //NDEBUG
}
