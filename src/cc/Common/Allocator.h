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
 * Memory Allocator base class.
 * This file contains a base class for a memory allocator. It's derived in
 * PageArenaAllocator.h and others.
 */

#ifndef HYPERTABLE_ALLOCATOR_H
#define HYPERTABLE_ALLOCATOR_H

#include "Error.h"

#include <utility>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * helper functions which call the destructor of an object; destructor
 * is not called on POD types (using template specialization)
 *
 * There used to be std::destroy's, but they never made it into the final
 * standard.
 */
inline void destruct(char *) {}
inline void destruct(unsigned char *) {}
inline void destruct(short *) {}
inline void destruct(unsigned short *) {}
inline void destruct(int *) {}
inline void destruct(unsigned int *) {}
inline void destruct(long *) {}
inline void destruct(unsigned long *) {}
inline void destruct(long long *) {}
inline void destruct(unsigned long long *) {}
inline void destruct(float *) {}
inline void destruct(double *) {}
inline void destruct(long double *) {}

template <typename T>
inline void destruct(T *p) { p->~T(); }


/** Convenience function returning a size aligned to 8 or 4 bytes,
 * depending on the system's architecture */
inline size_t get_align_offset(void *p) {
  if (sizeof(void *) == 8)
    return 8 - (((uint64_t)p) & 0x7);

  return 4 - (((uint64_t)p) & 0x3);
}


/**
 * Base classes for all Allocator classes.
 * The template parameter T is the type of object that is allocated
 */
template <typename T>
struct AllocatorBase {
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef T& reference;
  typedef const T& const_reference;
  typedef T value_type;

  /** Returns a pointer to an object of type T
   *
   * @param x A reference to the object
   * @return The address of x (of type T*)
   */
  pointer address(reference x) const { return &x; }

  /** Returns a const pointer to an object of type T
   *
   * @param x A reference to the object
   * @return The address of x (of type const T*)
   */
  const_pointer address(const_reference x) const { return &x; }

  // implement the following 2 methods
  // pointer allocate(size_type sz);
  // void deallocate(pointer p, size_type n);

  /** Returns the maximum number of objects that this Allocator can allocate;
   * used to check for bad allocations
   */
  size_type max_size() const throw() {
    return size_t(-1) / sizeof(value_type);
  }

  /** Creates a new object instance using placement new and the copy
   * constructor
   *
   * @param p A pointer to the memory area where the new object will be
   *        constructed
   * @param val An object that is copied
   */
#if 0
  void construct(pointer p, const T& val) {
    new(static_cast<void*>(p)) T(val);
  }
#endif

  template< class U, class... Args >
    void construct( U* p, Args&&... args ) {
    ::new((void *)p) U(std::forward<Args>(args)...);
  }

  /** Creates a new object instance using placement new and the default
   * constructor
   *
   * @param p A pointer to the memory area where the new object will be
   *        constructed
   */
  void construct(pointer p) {
    new(static_cast<void*>(p)) T();
  }

  /** Calls the destructor of an object (unless it's a POD) */
  void destroy(pointer p) { destruct(p); }
};


/**
 * Disallow AllocatorBase for the type 'void' by generating a compiler error
 * if it's used
 */
template <>
struct AllocatorBase<void> {
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef void *pointer;
  typedef const void *const_pointer;
  // reference to void members are impossible.
  typedef void value_type;
};

/**
 * Specialized Allocator class using a Memory Arena which manages the allocated
 * memory; see PageArenaAllocator.h for an actual implementation
 */
template <typename T, class ArenaT>
class ArenaAllocatorBase : public AllocatorBase<T> {
  typedef AllocatorBase<T> Base;

  ArenaT *m_arenap;

 public:
  typedef typename Base::pointer pointer;
  typedef typename Base::size_type size_type;

  /** Default constructor; creates an empty object */
  ArenaAllocatorBase() : m_arenap(NULL) {}

  /** Constructor; takes ownership of a pointer to a memory arena */
  ArenaAllocatorBase(ArenaT &arena) { m_arenap = &arena; }

  /** Copy Constructor; copies the memory arena */
  template <typename U>
  ArenaAllocatorBase(const ArenaAllocatorBase<U, ArenaT> &copy)
    : m_arenap(copy.m_arenap) {}

  // arena allocators only needs to implement
  // pointer allocate(size_type sz);

  /** Sanity check the size of the requested buffer; if it's too large
   * then most likely this is a bug in the application
   *
   * @param sz Size of the requested buffer
   * @throws Error::BAD_MEMORY_ALLOCATION If the size is too large
   */
  inline void check_allocate_size(size_type sz) const {
    if (HT_UNLIKELY(sz > Base::max_size()))
      HT_THROW_(Error::BAD_MEMORY_ALLOCATION);
  }

  /**
   * Allocate an array of objects using the regular operator new
   *
   * @param sz Number of elements in the allocated array
   * @return A pointer to the allocated array
   */
  inline pointer default_allocate(size_type sz) {
    return (pointer)(::operator new(sz * sizeof(T)));
  }

  /**
   * Deletes allocated objects unless an Arena Allocator was used (in this
   * case the function is a nop)
   *
   * @param p Pointer to the object
   * @param sz Unused parameter, ignored
   */
  void deallocate(pointer p, size_type sz) { if (!m_arenap) delete p; }

  /** operator == returns true if the arena pointers are equal
   *
   * @param x Constant reference to the other object
   */
  template <typename U>
  bool operator==(const ArenaAllocatorBase<U, ArenaT> &x) const {
    return m_arenap == x.m_arenap;
  }

  /** operator != returns true if the arena pointers are not equal
   *
   * @param x Constant reference to the other object
   */
  template <typename U>
  bool operator!=(const ArenaAllocatorBase<U, ArenaT> &x) const {
    return m_arenap != x.m_arenap;
  }

  /** Swaps the memory arena from another allocator with the arena
   * of 'this' allocator
   *
   * @param other The other ArenaAllocatorBase object
   */
  template <typename U>
  void swap(ArenaAllocatorBase<U, ArenaT> &other) {
    ArenaT *tmp = m_arenap;
    m_arenap = other.m_arenap;
    other.m_arenap = tmp;
  }

  /** Returns a pointer to the Arena
   *
   * @return Pointer to this allocator's arena; can be NULL
   */
  ArenaT *arena() const { return m_arenap; }

  /** Sets the Arena pointer
   *
   * @param arena Pointer to the arena allocator
   */
  void set_arena(ArenaT *arena) { m_arenap = arena; }
};

/** @}*/

} // namespace Hypertable

#endif /* HYPERTABLE_ALLOCATOR_H */
