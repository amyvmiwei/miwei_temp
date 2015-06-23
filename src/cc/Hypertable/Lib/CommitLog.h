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

/// @file
/// Declarations for CommitLog.
/// This file contains declarations for CommitLog, a class for creating and
/// appending entries to an edit log.

#ifndef Hypertable_Lib_CommitLog_h
#define Hypertable_Lib_CommitLog_h

#include <Hypertable/Lib/BlockCompressionCodec.h>
#include <Hypertable/Lib/CommitLogBase.h>
#include <Hypertable/Lib/CommitLogBlockStream.h>

#include <Common/DynamicBuffer.h>
#include <Common/String.h>
#include <Common/Properties.h>
#include <Common/Filesystem.h>

#include <deque>
#include <map>
#include <memory>
#include <stack>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /**
   * Commit log for persisting range updates.  The commit log is a directory
   * that contains a growing number of files that contain compressed blocks of
   * "commits".  The files are named starting with '0' and will periodically
   * roll, which means that a trailer is written to the end of the file, the
   * file is closed, and then the numeric name is incremented by one and
   * opened.  Periodically when old parts of the log are no longer needed, they
   * get purged.  The size of each log fragment file is determined by the
   * following config file property:
   *<pre>
   * Hypertable.RangeServer.CommitLog.RollLimit
   *</pre>
   */

  class CommitLog : public CommitLogBase {

  public:

    struct CumulativeFragmentData {
      uint32_t distance;
      int64_t  size;
      int64_t  cumulative_size;
      uint32_t fragno;
    };

    typedef std::map<int64_t, CumulativeFragmentData> CumulativeSizeMap;

    /**
     * Constructs a CommitLog object using supplied properties.
     *
     * @param fs filesystem to write log into
     * @param log_dir directory of the commit log
     * @param props reference to properties map
     * @param init_log base log to pull fragments from
     * @param is_meta true for root, system and metadata logs
     */
    CommitLog(FilesystemPtr &fs, const std::string &log_dir,
              PropertiesPtr &props, CommitLogBase *init_log = 0,
              bool is_meta=true)
      : CommitLogBase(log_dir), m_fs(fs) {
      initialize(log_dir, props, init_log, is_meta);
    }

    /**
     * Constructs a CommitLog object using default properties.
     *
     * @param fs filesystem to write log into
     * @param log_dir directory of the commit log
     * @param is_meta true for root, system and metadata logs
     */
    CommitLog(FilesystemPtr &fs, const std::string &log_dir, bool is_meta=true);

    virtual ~CommitLog();

    /**
     * Atomically obtains a timestamp
     *
     * @return nanoseconds since the epoch
     */
    int64_t get_timestamp();

    /** Writes a block of updates to the commit log.
     *
     * @param cluster_id Originating cluster ID
     * @param buffer block of updates to commit
     * @param revision most recent revision in buffer
     * @param flags Flags to pass to underlying append operation
     * @return Error::OK on success or error code on failure
     */
    int write(uint64_t cluster_id, DynamicBuffer &buffer, int64_t revision, Filesystem::Flags flags);

    /** Flushes previous updates written to commit log.
     *
     * @return Error::OK on success or error code on failure
     */
    int flush();

    /** Sync previous updates written to commit log.
     *
     * @return Error::OK on success or error code on failure
     */
    int sync();

    /** Links an external log into this log.
     *
     * @param cluster_id Originating cluster ID
     * @param log_base pointer to commit log object to link in
     * @return Error::OK on success or error code on failure
     */
    int link_log(uint64_t cluster_id, CommitLogBase *log_base);

    /** Closes the log.  Writes the trailer and closes the file
     *
     * @return Error::OK on success or error code on failure
     */
    int close();

    /** Purges the log.  Removes all of the log fragments that have
     * a revision that is less than the given revision.
     *
     * @param revision real cutoff revision
     * @param remove_ok_logs Set of log pathnames that can be safely removed
     * @param removed_logs Set of logs that were removed by this call
     * @param trace Address of trace string to add trace info to if non-NULL
     */
    int purge(int64_t revision, StringSet &remove_ok_logs,
              StringSet &removed_logs, std::string *trace);

    /**
     * Fills up a map of cumulative fragment size data.  One entry per log
     * fragment is inserted into this map.  The key is the revision of the
     * fragment (e.g. the real revision of the most recent data in the fragment
     * file).  The value is a structure that contains information regarding how
     * expensive it is to keep this fragment around.
     *
     * @param cumulative_size_map reference to map of log fragment priority data
     */
    void load_cumulative_size_map(CumulativeSizeMap &cumulative_size_map);

    /**
     * Returns the maximum size of each log fragment file
     */
    int64_t get_max_fragment_size() { return m_max_fragment_size; }

    /**
     * Returns the stats on all commit log fragments
     *
     * @param prefix stat line prefix string
     * @param result reference to return stats string
     *
     */
    void get_stats(const std::string &prefix, std::string &result);

    /**
     * Returns total size of commit log
     */
    int64_t size() {
      std::lock_guard<std::mutex>lock(m_mutex);
      int64_t total = 0;
      for (LogFragmentQueue::iterator iter = m_fragment_queue.begin();
           iter != m_fragment_queue.end(); iter++)
        total += (*iter)->size;
      return total;
    }

    const std::string& get_current_fragment_file() {
      std::lock_guard<std::mutex>lock(m_mutex);
      return m_cur_fragment_fname;
    }

    static const char MAGIC_DATA[10];
    static const char MAGIC_LINK[10];

  private:
    void initialize(const std::string &log_dir,
                    PropertiesPtr &, CommitLogBase *init_log, bool is_meta);
    int roll(CommitLogFileInfo **clfip=0);
    int compress_and_write(DynamicBuffer &input, BlockHeader *header,
                           int64_t revision, Filesystem::Flags flags);
    void remove_file_info(CommitLogFileInfo *fi, StringSet &removed_logs);

    FilesystemPtr           m_fs;
    std::set<CommitLogFileInfo *> m_reap_set;
    std::unique_ptr<BlockCompressionCodec> m_compressor;
    std::string                  m_cur_fragment_fname;
    int64_t                 m_cur_fragment_length;
    int64_t                 m_max_fragment_size;
    uint32_t                m_cur_fragment_num;
    int32_t                 m_fd;
    int32_t                 m_replication;
    bool                    m_needs_roll;
  };

  /// Smart pointer to CommitLog
  typedef std::shared_ptr<CommitLog> CommitLogPtr;

  /// @}

}

#endif // Hypertable_Lib_CommitLog_h
