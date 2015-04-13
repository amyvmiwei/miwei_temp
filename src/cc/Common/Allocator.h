/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
 * Memory Allocator base class.
 * This file contains a base class for a memory allocator. It's derived in
 * PageArenaAllocator.h and others.
 */

#ifndef HYPERTABLE_ALLOCATOR_H
#define HYPERTABLE_ALLOCATOR_H

#include "Error.h"

#include <memory>
#include <utility>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * Specialized Allocator class using a Memory Arena which manages the allocated
 * memory; see PageArenaAllocator.h for an actual implementation
 */
template <typename T, class ArenaT>
class ArenaAllocatorBase : public std::allocator<T> {
  typedef std::allocator<T> Base;

  ArenaT *m_arenap;

  public:
  typedef typename Base::pointer pointer;
  typedef typename Base::size_type size_type;

  /** Default constructor; creates an empty object */
  ArenaAllocatorBase() : m_arenap(0) {}

  /** Constructor; takes ownership of a pointer to a memory arena */
  ArenaAllocatorBase(ArenaT &arena) { m_arenap = &arena; }

  /** Copy Constructor; copies the memory arena */
  template <typename U> inline
  ArenaAllocatorBase(const ArenaAllocatorBase<U, ArenaT> &copy)
    : m_arenap(copy.arena()) {}

  /** Assignment operator; copies the memory arena */
  template <typename U> inline
  ArenaAllocatorBase& operator =(const ArenaAllocatorBase<U, ArenaT> &other) {
    m_arenap = other.arena();
    return *this;
  }

  /**
   * Deletes allocated objects unless an Arena Allocator was used (in this
   * case the function is a nop)
   *
   * @param p Pointer to the object
   * @param sz Unused parameter, ignored
   */
  inline void deallocate(pointer p, size_type sz) {
    if (!m_arenap)
      Base::deallocate(p, sz);
  }

  /** operator == returns true if the arena pointers are equal
   *
   * @param x Constant reference to the other object
   */
  template <typename U>
  bool operator==(const ArenaAllocatorBase<U, ArenaT> &x) const {
    return m_arenap == x.arena();
  }

  /** operator != returns true if the arena pointers are not equal
   *
   * @param x Constant reference to the other object
   */
  template <typename U>
  bool operator!=(const ArenaAllocatorBase<U, ArenaT> &x) const {
    return m_arenap != x.arena();
  }

  /** Swaps the memory arena from another allocator with the arena
   * of 'this' allocator
   *
   * @param other The other ArenaAllocatorBase object
   */
  template <typename U>
  void swap(ArenaAllocatorBase<U, ArenaT> &other) {
    std::swap(m_arenap, other.arena());
  }

  /** Returns a pointer to the Arena
   *
   * @return Pointer to this allocator's arena; can be NULL
   */
  inline ArenaT *arena() const { return m_arenap; }
};

/** @}*/

} // namespace Hypertable

#endif /* HYPERTABLE_ALLOCATOR_H */
