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
 * PageArena memory allocator for STL classes.
 * This class efficiently allocates memory chunks from large memory pages.
 * This reduces the number of heap allocations, thus causing less heap
 * fragmentation and higher performance. The PageArenaAllocator is used
 * in combination with STL container classes.
 */

#ifndef HYPERTABLE_PAGEARENA_ALLOCATOR_H
#define HYPERTABLE_PAGEARENA_ALLOCATOR_H

#include "PageArena.h"
#include "Allocator.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * The PageArenaAllocator is a STL allocator based on PageArena. Do not use
 * directly but when instantiating STL containers, i.e.
 *
 *     std::vector<int, PageArenaAllocator<int> > foo;
 */
template <typename T, class ArenaT = PageArena<> >
struct PageArenaAllocator : public ArenaAllocatorBase<T, ArenaT> {
  typedef ArenaAllocatorBase<T, ArenaT> Base;
  typedef typename Base::pointer pointer;
  typedef typename Base::size_type size_type;

  template <typename U> struct rebind {
    typedef PageArenaAllocator<U, ArenaT> other;
  };

  PageArenaAllocator() {}
  PageArenaAllocator(ArenaT &arena) : Base(arena) {}

  /** Copy constructor */
  template <typename U>
  PageArenaAllocator(const PageArenaAllocator<U, ArenaT> &copy) {
    Base::set_arena(copy.arena());
  }

  pointer allocate(size_type sz) {
    Base::check_allocate_size(sz);

    if (HT_LIKELY(Base::arena() != NULL))
      return (pointer)(Base::arena()->alloc(sz * sizeof(T)));

    return Base::default_allocate(sz);
  }
};

/** @} */

} // namespace Hypertable

#endif /* HYPERTABLE_PAGEARENA_ALLOCATOR_H */
