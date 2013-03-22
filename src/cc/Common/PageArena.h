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
 * PageArena memory allocator.
 * This class efficiently allocates memory chunks from large memory pages.
 * This reduces the number of heap allocations, thus causing less heap
 * fragmentation and higher performance. The PageArena memory allocator can
 * not be used for STL classes - use @ref PageArenaAllocator instead.
 */

#ifndef HYPERTABLE_PAGEARENA_H
#define HYPERTABLE_PAGEARENA_H

#include <cstdlib>
#include <iostream>
#include <cstring>
#include <cstddef>
#include <cassert>
#include <algorithm>
#include <set>

#include <boost/noncopyable.hpp>
#include <boost/static_assert.hpp>

#include "Common/Logger.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/** A memory allocator using std::malloc() and std::free(). */
struct DefaultPageAllocator {
  /** Allocates a new chunk of memory */
  void *allocate(size_t sz) { return std::malloc(sz); }

  /** Frees a previously allocated chunk of memory */
  void deallocate(void *p) { if (p) std::free(p); }

  /** Inform the allocator that a bulk of objects was freed by the caller */
  void freed(size_t sz) { }
};

/**
 * The PageArena allocator is simple and fast, avoiding individual
 * mallocs/frees. Good for usage patterns that just load, use and free the
 * entire container repeatedly.
 */
template <typename CharT = char, class PageAllocatorT = DefaultPageAllocator>
class PageArena : boost::noncopyable {
 private:
  /** The default page size is 8 kb */
  enum {
    DEFAULT_PAGE_SIZE = 8192
  };

  typedef PageArena<CharT, PageAllocatorT> Self;

  /** A structure to manage a single memory page which is used to
   * allocate smaller memory chunks
   */
  struct Page {
    Page *next_page;
    char *alloc_end;
    const char *page_end;
    char buf[1];

    Page(const char *end) : next_page(0), page_end(end) {
      alloc_end = buf;
    }

    size_t remain() const {
      return page_end - alloc_end;
    }

    CharT *alloc(size_t sz) {
      assert(sz <= remain());
      char *start = alloc_end;
      alloc_end += sz;
      return (CharT *)start;
    }
  };

 private: // data
  Page *m_cur_page;
  size_t m_used;        /** total number of bytes allocated by users */
  size_t m_page_limit;  /** capacity in bytes of an empty page */
  size_t m_page_size;   /** page size in number of bytes */
  size_t m_pages;       /** number of pages allocated */
  size_t m_total;       /** total number of bytes occupied by pages */
  PageAllocatorT m_page_allocator;

  /** predicate which sorts by size */
  struct LtPageRemain {
    bool operator()(const Page* p1, const Page*p2) const {
      return p1->remain() < p2->remain();
    }
  };

  /** a list of pages with gaps, sorted by gap size */
  typedef std::set<Page*, LtPageRemain> GappyPages;
  GappyPages m_gappy_pages;

  size_t m_gappy_limit;

  /** A small buffer (128 bytes) to satisfy very small memory allocations
   * without allocating a page from the heap
   */
  struct TinyBuffer {
    enum { SIZE = 128 };
    CharT base[SIZE];
    size_t fill;
    inline TinyBuffer() : fill(0) { }
    inline CharT *alloc(size_t sz) {
      CharT *p = 0;
      if (fill + sz <= SIZE) {
        p = base + fill;
        fill += sz;
      }
      return p;
    }
  } m_tinybuf;

 private:
  /** Allocates a page of size @a sz */
  Page *alloc_page(size_t sz, bool prepend = true) {
    Page *page = (Page *) m_page_allocator.allocate(sz);
    HT_EXPECT(page, Error::BAD_MEMORY_ALLOCATION);
    new (page) Page((char *)page + sz);

    if (HT_LIKELY(prepend))
      page->next_page = m_cur_page;
    else if (m_cur_page) {
      // insert after cur page, for big/full pages
      page->next_page = m_cur_page->next_page;
      m_cur_page->next_page = page;
    }
    else // don't leak it
      m_cur_page = page;

    ++m_pages;
    m_total += sz;

    return page;
  }

  inline void ensure_cur_page() {
    if (HT_UNLIKELY(!m_cur_page)) {
      m_cur_page = alloc_page(m_page_size);
      m_page_limit = m_cur_page->remain();
    }
  }

  inline bool is_normal_overflow(size_t sz) {
    return sz <= m_page_limit && m_cur_page->remain() < m_page_limit / 2;
  }

  CharT *alloc_big(size_t sz) {
    // big enough object to have their own page
    Page *p = alloc_page(sz + sizeof(Page), false);
    return p->alloc(sz);
  }

 public:
  /** Constructor; creates a new PageArena
   *
   * @param page_size Size (in bytes) of the allocated pages
   * @param alloc The allocator for the pages
   */
  PageArena(size_t page_size = DEFAULT_PAGE_SIZE,
          const PageAllocatorT &alloc = PageAllocatorT())
    : m_cur_page(0), m_used(0), m_page_limit(0), m_page_size(page_size),
      m_pages(0), m_total(0), m_page_allocator(alloc), m_gappy_limit(0) {
    BOOST_STATIC_ASSERT(sizeof(CharT) == 1);
    HT_ASSERT(page_size > sizeof(Page));
  }

  /** Destructor releases the whole memory and invalidates all allocated
   * objects
   */
  ~PageArena() {
    free();
  }

  /** Returns the page size */
  size_t page_size() {
    return m_page_size;
  }

  /** Sets the page size */
  void set_page_size(size_t sz) {
    HT_ASSERT(sz > sizeof(Page));
    m_page_size = sz;
  }

  /** Allocate sz bytes */
  CharT *alloc(size_t sz) {
    CharT *tiny;
    if ((tiny = m_tinybuf.alloc(sz)))
      return tiny;
    m_used += sz;
    ensure_cur_page();

    if (m_gappy_limit >= sz) {
      Page f((const char*)sz);
      f.alloc_end = 0;
      typename GappyPages::iterator it = m_gappy_pages.lower_bound(&f);
      Page* page = *it;
      CharT *p = page->alloc(sz);
      m_gappy_pages.erase(it);
      if (page->remain() >= TinyBuffer::SIZE) {
        m_gappy_pages.insert(page);
      }
      m_gappy_limit = m_gappy_pages.size()
          ? (*m_gappy_pages.rbegin())->remain()
          : 0;
      return p;
    }

    // common case
    if (HT_LIKELY(sz <= m_cur_page->remain()))
      return m_cur_page->alloc(sz);

    if (is_normal_overflow(sz)) {
      if (m_cur_page->remain() >= TinyBuffer::SIZE) {
        m_gappy_pages.insert(m_cur_page);
        m_gappy_limit = (*m_gappy_pages.rbegin())->remain();
      }
      m_cur_page = alloc_page(m_page_size);
      return m_cur_page->alloc(sz);
    }
    return alloc_big(sz);
  }

  /** Realloc for newsz bytes */
  CharT *realloc(void *p, size_t oldsz, size_t newsz) {
    HT_ASSERT(m_cur_page);
    // if we're the last one on the current page with enough space
    if ((char *)p + oldsz == m_cur_page->alloc_end
        && (char *)p + newsz  < m_cur_page->page_end) {
      m_cur_page->alloc_end = (char *)p + newsz;
      return (CharT *)p;
    }
    CharT *copy = alloc(newsz);
    memcpy(copy, p, newsz > oldsz ? oldsz : newsz);
    return copy;
  }

  /** Duplicate a null terminated string; memory is allocated from the pool.
   *
   * @param s The original string which is duplicated
   * @return The new string, or NULL if the original string also pointed to
   *        NULL
   */
  CharT *dup(const CharT *s) {
    if (HT_UNLIKELY(!s))
      return NULL;

    size_t len = std::strlen(s) + 1;
    CharT *copy = alloc(len);
    memcpy(copy, s, len);
    return copy;
  }

  /** Duplicate a buffer of size @a len; memory is allocated from the pool.
   *
   * @param s Pointer to the buffer
   * @param len Size of the buffer
   * @return A duplicate of the buffer, or NULL if @a s also pointed to NULL
   */
  CharT *dup(const void *s, size_t len) {
    if (HT_UNLIKELY(!s))
      return NULL;

    CharT *copy = alloc(len);
    memcpy(copy, s, len);
    return copy;
  }

  /** Free the whole arena. This releases the total memory allocated by the
   * arena and invalidates all pointers that were previously allocated.
   * This is implicitely called in the destructor.
   */
  void free() {
    Page *page;

    while (m_cur_page) {
      page = m_cur_page;
      m_cur_page = m_cur_page->next_page;
      m_page_allocator.deallocate(page);
    }
    m_page_allocator.freed(m_total);
    m_pages = m_total = m_used = 0;

    m_tinybuf = TinyBuffer();
    m_gappy_pages.clear();
    m_gappy_limit = 0;
  }

  /** Efficiently swaps the state with another allocator */
  void swap(Self &other) {
    std::swap(m_cur_page, other.m_cur_page);
    std::swap(m_page_limit, other.m_page_limit);
    std::swap(m_page_size, other.m_page_size);
    std::swap(m_pages, other.m_pages);
    std::swap(m_total, other.m_total);
    std::swap(m_used, other.m_used);
    std::swap(m_tinybuf, other.m_tinybuf);
    std::swap(m_gappy_pages, other.m_gappy_pages);
    std::swap(m_gappy_limit, other.m_gappy_limit);
  }

  /** Write allocator statistics to @ref out */
  std::ostream& dump_stat(std::ostream& out) const {
    out << "pages=" << m_pages << ", total=" << m_total << ", used=" << m_used
        << "(" << m_used * 100. / m_total << "%)";
    return out;
  }

  /** Statistic accessors - returns used bytes */
  size_t used() const { return m_used + m_tinybuf.fill; }

  /** Statistic accessors - returns number of allocated pages */
  size_t pages() const { return m_pages; }

  /** Statistic accessors - returns total allocated size */
  size_t total() const { return m_total + TinyBuffer::SIZE; }
};

typedef PageArena<> CharArena;
typedef PageArena<unsigned char> ByteArena;

template <typename CharT, class PageAlloc>
inline std::ostream&
operator<<(std::ostream& out, const PageArena<CharT, PageAlloc> &m) {
  return m.dump_stat(out);
}

/** @} */

} // namespace Hypertable

#endif // !HYPERTABLE_PAGEARENA_H
