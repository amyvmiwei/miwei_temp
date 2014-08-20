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

/// @file
/// Declarations for SshThreadsCallbacks.
/// This file contains type declarations for SshThreadsCallbacks, a class that
/// defines the libssh threads callbacks.

#ifndef Tools_cluster_SshThreadsCallbacks_h
#define Tools_cluster_SshThreadsCallbacks_h

#include <libssh/callbacks.h>

#include <mutex>
#include <thread>
#include <unordered_map>

namespace Hypertable {

  /// @addtogroup ssh
  /// @{

  /// %Thread callbacks for libssh
  class SshThreadsCallbacks {

  public:

    /// %Mutex initializaton callback.
    /// Allocates a mutex object and assigns to <code>*priv</code>
    /// @param priv Address of mutex pointer
    /// @return 0
    static int mutex_init (void **priv);

    /// %Mutex destroy callback.
    /// Deallocates mutex object pointed to by <code>*lock</code>
    /// @param lock Address of mutex pointer
    /// @return 0
    static int mutex_destroy (void **lock);

    /// %Mutex lock callback.
    /// Locks mutex object pointed to by <code>*lock</code>
    /// @param lock Address of mutex pointer
    /// @return 0
    static int mutex_lock (void **lock);

    /// %Mutex unlock callback.
    /// Unlocks mutex object pointed to by <code>*lock</code>
    /// @param lock Address of mutex pointer
    /// @return 0
    static int mutex_unlock (void **lock);

    /// Callback for obtaining current thread ID.
    static unsigned long thread_id ();

    /// Gets libssh threads callbacks structure.
    /// Allocates a <code>ssh_threads_callbacks_struct</code> structure with
    /// callback functions initialized to the ones defined in this class.  The
    /// <code>type</code> member is set to "threads_cpp".
    /// @return libssh threads callback structure.
    static struct ssh_threads_callbacks_struct *get();

  private:

    /// %Mutex for serializing access to static data members
    static std::mutex ms_thread_id_mutex;

    /// Next thread ID
    static unsigned long ms_thread_id_next;

    /// Mapping from std::thread::id to unique thread ID number
    static std::unordered_map<std::thread::id, int> ms_thread_id_map;

    /// libssh callbacks structure
    static struct ssh_threads_callbacks_struct ms_callbacks;

  };

  /// @}
}

#endif // Tools_cluster_SshThreadsCallbacks_h


