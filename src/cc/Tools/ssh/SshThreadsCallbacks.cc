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
/// Definitions for SshThreadsCallbacks.
/// This file contains type definitions for SshThreadsCallbacks, a class that
/// defines the libssh threads callbacks.

#include <Common/Compat.h>

#include "SshThreadsCallbacks.h"

using namespace Hypertable;
using namespace std;

unsigned long SshThreadsCallbacks::ms_thread_id_next {};

mutex SshThreadsCallbacks::ms_thread_id_mutex {};

unordered_map<thread::id, int> SshThreadsCallbacks::ms_thread_id_map {};

struct ssh_threads_callbacks_struct SshThreadsCallbacks::ms_callbacks = {
  "threads_cpp",
  mutex_init,
  mutex_destroy,
  mutex_lock,
  mutex_unlock,
  thread_id
};

int SshThreadsCallbacks::mutex_init (void **priv){
  *priv = new mutex();
  return 0;
}

int SshThreadsCallbacks::mutex_destroy (void **lock) {
  delete (mutex *)*lock;
  return 0;
}

int SshThreadsCallbacks::mutex_lock (void **lock) {
  ((mutex *)*lock)->lock();
  return 0;
}

int SshThreadsCallbacks::mutex_unlock (void **lock){
  ((mutex *)*lock)->unlock();
  return 0;
}


unsigned long SshThreadsCallbacks::thread_id () {
  lock_guard<mutex> lock(ms_thread_id_mutex);
  auto iter = ms_thread_id_map.find(this_thread::get_id());
  if (iter == ms_thread_id_map.end()) {
    unsigned long id = ++ms_thread_id_next;
    ms_thread_id_map[this_thread::get_id()] = id;
    return id;
  }
  return iter->second;
}

struct ssh_threads_callbacks_struct *SshThreadsCallbacks::get() {
  return &ms_callbacks;
}

