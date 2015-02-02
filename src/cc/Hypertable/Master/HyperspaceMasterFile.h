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

/// @file
/// Declarations for HyperspaceMasterFile.
/// This file contains declarations for HyperspaceMasterFile, a class that
/// facilitates operations on the <i>master</i> file in %Hyperspace.

#ifndef Hypertable_Master_HyperspaceMasterFile_h
#define Hypertable_Master_HyperspaceMasterFile_h

#include <Hyperspace/Session.h>

#include <Common/Properties.h>
#include <Common/ScopeGuard.h>

#include <condition_variable>
#include <mutex>
#include <string>

namespace Hypertable {
namespace Master {

  /// @addtogroup Master
  /// @{

  using namespace Hyperspace;

  /// Facilitates operations on the <i>master</i> file in %Hyperspace.
  class HyperspaceMasterFile {
  public:

    /// Constructor.
    /// @param props Smart pointer to properties object
    /// @param hyperspace Smart pointer to hyperspace session
    HyperspaceMasterFile(PropertiesPtr props, SessionPtr &hyperspace)
      : m_props(props), m_hyperspace(hyperspace) {}

    HyperspaceMasterFile(const HyperspaceMasterFile &) = delete;

    HyperspaceMasterFile &operator=(const HyperspaceMasterFile &) = delete;

    /// Destructor.
    ~HyperspaceMasterFile();

    /// Acquires lock on <code>toplevel_dir/master</code> file in hyperspace.
    /// Attempts to acquire an exclusive lock on the file
    /// <code>toplevel_dir/master</code> in Hyperspace.  The file is opened and the
    /// handle is stored in #m_handle.  If the lock is successfully acquired, the
    /// <i>next_server_id</i> attribute is created and initialized to "1" if it
    /// doesn't already exist.  It also creates the following directories in
    /// Hyperspace if they do not already exist:
    /// 
    ///   - <code>/hypertable/servers</code>
    ///   - <code>/hypertable/tables</code>
    ///   - <code>/hypertable/root</code>
    /// 
    /// If the the lock is not successfully acquired, it will go into a retry loop
    /// in which the function will sleep for 
    /// <code>Hypertable.Connection.Retry.Interval</code> milliseconds and then
    /// re-attempt to acquire the lock.
    /// @tparam Function Startup function type
    /// @param toplevel_dir Top-level directory containing master file in Hyperspace
    /// @param set_startup_status Function to be called to set startup status
    /// @return <i>true</i> if lock successfully acquired, <i>false</i> if
    /// returning due to shutdown
    template<typename Function>
    bool obtain_master_lock(const std::string &toplevel_dir,
                            Function set_startup_status) {
      try {
        uint64_t handle {};
        HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);

        // Create TOPLEVEL directory if not exist
        m_hyperspace->mkdirs(toplevel_dir);

        // Create /hypertable/master if not exist
        if (!m_hyperspace->exists( toplevel_dir + "/master" )) {
          handle = m_hyperspace->open( toplevel_dir + "/master",
                                       OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);
          m_hyperspace->close(handle);
          handle = 0;
        }

        {
          LockStatus lock_status = LOCK_STATUS_BUSY;
          uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
          LockSequencer sequencer;
          bool reported = false;
          uint32_t retry_interval = m_props->get_i32("Hypertable.Connection.Retry.Interval");

          m_handle = m_hyperspace->open(toplevel_dir + "/master", oflags);

          while (lock_status != LOCK_STATUS_GRANTED) {

            m_hyperspace->try_lock(m_handle, LOCK_MODE_EXCLUSIVE,
                                   &lock_status, &sequencer);

            if (lock_status != LOCK_STATUS_GRANTED) {
              if (!reported) {
                set_startup_status(false);
                HT_INFOF("Couldn't obtain lock on '%s/master' due to conflict, entering retry loop ...",
                         toplevel_dir.c_str());
                reported = true;
              }
              {
                unique_lock<mutex> lock(m_mutex);
                m_cond.wait_for(lock, std::chrono::milliseconds(retry_interval));
                if (m_shutdown)
                  return false;
              }
            }
          }

          set_startup_status(true);
          m_acquired = true;

          HT_INFOF("Obtained lock on '%s/master'", toplevel_dir.c_str());

          if (!m_hyperspace->attr_exists(m_handle, "next_server_id"))
            m_hyperspace->attr_set(m_handle, "next_server_id", "1", 2);
        }

        m_hyperspace->mkdirs(toplevel_dir + "/servers");
        m_hyperspace->mkdirs(toplevel_dir + "/tables");

        // Create /hypertable/root
        handle = m_hyperspace->open(toplevel_dir + "/root",
                                    OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);

      }
      catch (Exception &e) {
        HT_FATAL_OUT << e << HT_END;
      }
      return true;
    }

    /// Returns flag indicating if lock was acquired
    bool lock_acquired() { return m_acquired; }

    /// Writes Master's listen address to Hyperspace.    
    void write_master_address();

    /// Obtains next server ID.
    /// @return Next server ID
    uint64_t next_server_id();

    void shutdown();

  private:

    /// %Mutex for serializing member access
    std::mutex m_mutex;

    /// Condition variable signalling shutdown
    std::condition_variable m_cond;

    /// Properties
    PropertiesPtr m_props;

    /// Hyperspace session
    SessionPtr m_hyperspace;

    /// %Master file handle
    uint64_t m_handle {};

    /// Shutdown flag
    bool m_shutdown {};

    /// Lock acquired flag
    bool m_acquired {};
  };

  /// @}

}}

#endif // HyperspaceMasterFile_h
