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
/// Declarations for SshOutputCollector.
/// This file contains type declarations for SshOutputCollector, a class for
/// collecting buffers of output returned from the exection of a remote SSH
/// command.

#ifndef Tools_cluster_SshOutputCollector_h
#define Tools_cluster_SshOutputCollector_h

#include <Common/PageArena.h>
#include <Common/Logger.h>

#include <boost/iterator/iterator_facade.hpp>

#include <cstdlib>
#include <string>
#include <tuple>
#include <vector>

namespace Hypertable {

  /// @addtogroup ssh
  /// @{

  /// Collects buffers of output from execution of remote SSH command.
  class SshOutputCollector {

  public:

    /// Fixed-size buffer to hold a portion of output.
    class Buffer {
    public:
      /// Constructor.
      Buffer() { }
      /// Constructor with allocated page argument.
      /// @param buffer Pointer to allocated buffer memory
      /// @param sz Size of allocated buffer memory
      Buffer(char *buffer, size_t sz=0) : base(buffer), ptr(buffer), size(sz) { }
      /// Returns amount of buffer filled.
      /// @return Amount of buffer filled.
      size_t fill() const { return ptr-base; }
      /// Returns amount of unused space remaining in buffer
      /// @return Amount of unused space remaining in buffer
      size_t remain() const { return size-fill(); }
      /// Adds data to buffer.
      /// @param data Pointer to data to add
      /// @param len Length of data to add
      void add(const char *data, size_t len) {
        HT_ASSERT(len <= remain());
        memcpy(ptr, data, len);
        ptr += len;
      }
      /// Pointer to beginning of buffer memory
      char *base {};
      /// Pointer to next unused position in buffer memory
      char *ptr {};
      /// Size of allocated buffer
      size_t size {};
    };

    /// Iterator for traversing output line-by-line.
    class iterator : public boost::iterator_facade<iterator, const std::string, boost::forward_traversal_tag> {
    public:
      /// Constructor.
      /// @param buffers vector of buffers
      /// @param index Starting buffer index
      iterator(std::vector<Buffer> &buffers, size_t index=0) 
        : m_buffers(buffers), m_index(index), m_next_index(index) {
        increment();
      }

    private:
      friend class boost::iterator_core_access;

      /// Increment to next output line.
      /// This function fills #m_line with the next line of output.
      void increment();

      /// Equality comparison.
      /// Returns <i>true</i> if the #m_index and #m_offset members are equal.
      /// @param other Iterator with which to compare
      /// @return <i>true</i> if this iteratore is equal to <code>other</code>,
      /// <i>false</i> otherwise.
      bool equal(iterator const& other) const {
        return this->m_index == other.m_index &&
          this->m_offset == other.m_offset;
      }

      /// Returns next line.
      /// This method returns the line stored in #m_line bye the increment()
      /// function.
      /// @return Reference to next line
      const std::string & dereference() const { return m_line; }

      /// Output buffers
      std::vector<Buffer> &m_buffers;
      /// Index of buffer in #m_buffers holding start of current line of output
      size_t m_index {};
      /// Offset within current buffer of start of current line of output
      size_t m_offset {};
      /// Index of buffer in #m_buffers holding start of next line of output
      size_t m_next_index {};
      /// Offset within next buffer of start of next line of output
      size_t m_next_offset {};
      /// Current line of output
      std::string m_line;
    };

    /// Constructor.
    /// @param buffer_size Size of buffer pages to allocate
    SshOutputCollector(size_t buffer_size) : m_buffer_size(buffer_size) {}

    /// Returns buffer size.
    /// This function returns the size of buffers returned by allocate_buffer().
    /// @return %Buffer size
    size_t buffer_size() { return m_buffer_size; }

    /// Allocate a buffer.
    /// This function allocates a buffer of size #m_buffer_size from #m_arena
    /// @return Allocated buffer
    Buffer allocate_buffer() {
      return Buffer(m_arena.alloc(m_buffer_size), m_buffer_size);
    }

    /// Adds filled buffer to collector.
    /// @param buf %Buffer to add
    void add(Buffer buf) {
      if (buf.fill())
        m_buffers.push_back(buf);
    }

    /// Returns <i>true</i> if no output has been collected
    /// Returns <i>true</i> if there are no collected buffers or if none of the
    /// collected buffers contain data
    /// @return <i>true</i> if no output has been collected, <i>false</i>
    /// otherwise
    bool empty() const {
      if (m_buffers.empty())
        return true;
      for (auto & buffer : m_buffers) {
        if (buffer.fill())
          return false;
      }
      return true;
    }

    /// Returns <i>true</i> if last line collected is partial.
    /// If the last character collected is <b>not</b> a newline character, this
    /// function returns <i>true</i>, indicating that the collection has
    /// stopped in the middle of a line and there are more line characters to be
    /// read.
    /// @return <i>true</i> if last line collected is partial
    bool last_line_is_partial() {
      HT_ASSERT(m_buffers.empty() || m_buffers.back().fill() > 0);
      return !m_buffers.empty() && *(m_buffers.back().ptr-1) != '\n';
    }

    /// Returns iterator at beginning of output collector.
    /// @return Iterator at beginning of output collector
    iterator begin() { return iterator(m_buffers, 0); }

    /// Returns iterator at end of output collector.
    /// @return Iterator at end of output collector
    iterator end() { return iterator(m_buffers, m_buffers.size()); }

  private:
    /// %Buffer size
    size_t m_buffer_size {};
    /// Character arena from which buffers are allocated
    CharArena m_arena;
    /// Vector of buffers
    std::vector<Buffer> m_buffers;
  };

  /// @}

} // namespace Hypertable

#endif // Tools_cluster_SshOutputCollector_h
