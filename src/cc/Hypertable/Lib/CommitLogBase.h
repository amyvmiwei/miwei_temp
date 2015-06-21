/* -*- c++ -*-
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

#ifndef Hypertable_Lib_CommitLogBase_h
#define Hypertable_Lib_CommitLogBase_h

#include "KeySpec.h"
#include "CommitLogBlockStream.h"

#include <Common/String.h>
#include <Common/StringExt.h>

#include <boost/algorithm/string.hpp>

#include <deque>
#include <memory>
#include <mutex>

namespace Hypertable {

  class CommitLogFileInfo {
  public:
    CommitLogFileInfo() { }
    ~CommitLogFileInfo() { verify(); verification = 0; }
    void verify() {
      HT_ASSERT(verification == 123456789LL);
    }
    bool remove_ok(StringSet &remove_ok_logs) {
      return parent == 0 || remove_ok_logs.count(log_dir);
    }
    std::string to_str(StringSet &remove_ok_logs) {
      std::string msg = format("FileInfo=(logdir=%s,num=%d,revision=%lld,references=%d,rmOk=%d)",
                          log_dir.c_str(), (int)num, (Lld)revision, (int)references,
                          (int)remove_ok_logs.count(log_dir));
      if (parent)
        msg += format(" parent=(logdir=%s,num=%d,revision=%lld,references=%d,rmOk=%d)",
                      parent->log_dir.c_str(), (int)parent->num, (Lld)parent->revision,
                      (int)parent->references, (int)remove_ok_logs.count(parent->log_dir));
      return msg;
    }
    std::string log_dir;
    uint32_t num {};
    uint64_t size {};
    int64_t revision {};
    int64_t log_dir_hash {};
    size_t references {};
    CommitLogBlockStream *block_stream {};
    CommitLogFileInfo *parent {};
    StringSet  purge_dirs;
    int64_t verification {123456789LL};
  };

  struct LtClfip {
    bool operator()(CommitLogFileInfo *x, CommitLogFileInfo *y) const {
      return x->revision < y->revision;
    }
  };

  typedef std::deque<CommitLogFileInfo *> LogFragmentQueue;

  /**
   */
  class CommitLogBase {
  public:
    CommitLogBase(const std::string &log_dir)
      : m_log_dir(log_dir), m_latest_revision(TIMESTAMP_MIN),
        m_range_reference_required(true) {

      boost::trim_right_if(m_log_dir, boost::is_any_of("/"));
      
      size_t lastslash = m_log_dir.find_last_of('/');

      m_log_name = (lastslash == String::npos) ? m_log_dir
                                               : m_log_dir.substr(lastslash+1);
    }

    /**
     * This method assumes that the other commit log is not being
     * concurrently used which is why it doesn't lock it's mutex
     */
    void stitch_in(CommitLogBase *other) {
      std::lock_guard<std::mutex>lock(m_mutex);
      for (LogFragmentQueue::iterator iter = other->m_fragment_queue.begin();
           iter != other->m_fragment_queue.end(); iter++)
        m_fragment_queue.push_back(*iter);
      other->m_fragment_queue.clear();
    }

    std::string &get_log_dir() { return m_log_dir; }

    int64_t get_latest_revision() { return m_latest_revision; }

    bool empty() { std::lock_guard<std::mutex>lock(m_mutex); return m_fragment_queue.empty(); }

    bool range_reference_required() { return m_range_reference_required; }

    LogFragmentQueue &fragment_queue() { return m_fragment_queue; }

    uint32_t toplevel_fragment_id(CommitLogFileInfo *finfo) {
      while (finfo->parent)
        finfo = finfo->parent;
      return finfo->num;
    }

  protected:
    std::mutex m_mutex;
    std::string m_log_dir;
    std::string m_log_name;
    LogFragmentQueue m_fragment_queue;
    int64_t m_latest_revision;
    std::set<int64_t> m_linked_log_hashes;
    bool m_range_reference_required;
  };

  typedef std::shared_ptr<CommitLogBase> CommitLogBasePtr;

}

#endif // Hypertable_Lib_CommitLogBase_h

