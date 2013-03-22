/*
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * Scoped lockers for recursive and non-recursive mutexes.
 * These helper classes use RAII to lock a mutex when they are constructed
 * and to unlock them when going out of scope.
 */

#ifndef HYPERTABLE_MUTEX_H
#define HYPERTABLE_MUTEX_H

#include <boost/version.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>

#include "Common/Logger.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/** A generic Locker class for objects with lock/unlock methods.
 *
 * boost::mutex::scoped_lock use lock_ops<mutex>::lock/unlock in pre 1.35
 * which makes it less convenient than the following which can be used
 * to guard any object (besides a typical mutex) with lock/unlock methods
 */
template <class MutexT>
class Locker : boost::noncopyable {
public:
  /** Constructor; acquires the lock, unless @a init_lock is false */
  explicit Locker(MutexT &mutex, bool init_lock = true)
    : m_mutex(mutex), m_locked(false) {
    if (init_lock)
      m_mutex.lock();
    m_locked = true;
  }

  /** Destructor; releases the lock */
  ~Locker() {
    if (m_locked) {
      m_mutex.unlock();
      m_locked = false;
    }
  }

private:
  /** Reference to the mutex object */
  MutexT &m_mutex;

  /** true if the mutex was locked, otherwise false */
  bool m_locked;
};

/** A ScopedLock is boost's version of the Locker class for a regular
 * (non-recursive) mutex.
 */
typedef boost::mutex::scoped_lock ScopedLock;

/** A ScopedRecLock is boost's version of the Locker class for a recursive
 * mutex.
 */
typedef boost::recursive_mutex::scoped_lock ScopedRecLock;

/** A (non-recursive) mutex
 *
 * This is identical to boost::mutex unless your boost version is < 1.35;
 * in this case additional lock(), unlock() methods are provided which are
 * more convenient to use than those of boost::mutex.
 */
class Mutex : public boost::mutex {
public:
#if BOOST_VERSION < 103500
  typedef boost::detail::thread::lock_ops<boost::mutex> Ops;

  void lock() { Ops::lock(*this); }

  void unlock() { Ops::unlock(*this); }
#endif
};

/** A recursive mutex
 *
 * This is identical to boost::recursive_mutex unless your boost version
 * is < 1.35; in this case additional lock(), unlock() methods are provided
 * which are more convenient to use than those of boost::recursive_mutex.
 */
class RecMutex : public boost::recursive_mutex {
public:
#if BOOST_VERSION < 103500
  typedef boost::detail::thread::lock_ops<boost::recursive_mutex> Ops;

  void lock() { Ops::lock(*this); }

  void unlock() { Ops::unlock(*this); }
#endif
};

/** @} */

} // namespace Hypertable

#endif // HYPERTABLE_MUTEX_H
