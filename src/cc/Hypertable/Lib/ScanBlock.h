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
/// Declarations for ScanBlock.
/// This file contains type declarations for ScanBlock, a class for managing a
/// block of scan results.

#ifndef Hypertable_Lib_ScanBlock_h
#define Hypertable_Lib_ScanBlock_h

#include <Hypertable/Lib/RangeServer/Response/Parameters/CreateScanner.h>
#include <Hypertable/Lib/SerializedKey.h>

#include <AsyncComm/Event.h>

#include <Common/ByteString.h>

#include <memory>
#include <vector>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /** Encapsulates a block of scan results.  The CREATE_SCANNER and
   * FETCH_SCANBLOCK RangeServer methods return a block of scan results
   * and this class parses and provides easy access to the key/value
   * pairs in that result.
   */
  class ScanBlock {
  public:

    typedef std::vector< std::pair<SerializedKey, ByteString> > Vector;

    ScanBlock();

    /** Loads scanblock data returned from RangeServer.  Both the
     * CREATE_SCANNER and FETCH_SCANBLOCK methods return a block of key/value
     * pairs.
     * @param event smart pointer to response MESSAGE event
     * @return Error::OK on success or error code on failure
     */
    int load(EventPtr &event);

    /** Returns the number of key/value pairs in the scanblock.
     * @return number of key/value pairs in the scanblock
     */
    size_t size() { return m_vec.size(); }

    /** Resets iterator to first key/value pair in the scanblock. */
    void reset() { m_iter = m_vec.begin(); }

    /** Returns the next key/value pair in the scanblock.  <b>NOTE:</b>
     * invoking the #load method invalidates all pointers previously returned
     * from this method.
     * @param key reference to return key pointer
     * @param value reference to return value pointer
     * @return true if key/value returned, false if no more key/value pairs
     */
    bool next(SerializedKey &key, ByteString &value);

    /** Returns true if this is the final scanblock returned by the scanner.
     * @return true if this is the final scanblock, or false if more to come
     */
    bool eos() { return !m_response.more(); }

    /** Indicates whether or not there are more key/value pairs in block
     * @return ture if #next will return more key/value pairs, false otherwise
     */
    bool more() {
      if (m_iter == m_vec.end())
        return false;
      return true;
    }

    /**
     * Approximate estimate of memory used by scanblock (returns the size of the event payload)
     * which contains most of the data
     */
    size_t memory_used() const {
      if (m_event)
        return m_event->payload_len;
      return 0;
    }

    /** Returns scanner ID associated with this scanblock.
     * @return scanner ID
     */
    int get_scanner_id() { return m_response.id(); }

    /** Returns number of skipped rows because of an OFFSET predicate */
    int get_skipped_rows() { return m_response.skipped_rows(); }

    /** Returns number of skipped rows because of a CELL_OFFSET predicate */
    int get_skipped_cells() { return m_response.skipped_cells(); }

    /// Returns reference to profile data.
    /// @return Reference to profile data
    const ProfileDataScanner &profile_data() { return m_response.profile_data(); }

  private:
    int m_error {};
    Vector m_vec;
    Vector::iterator m_iter;
    EventPtr m_event;
    Lib::RangeServer::Response::Parameters::CreateScanner m_response;
  };

  /// Smart pointer to ScanBlock.
  typedef std::shared_ptr<ScanBlock> ScanBlockPtr;

  /// @}
}

#endif // Hypertable_Lib_ScanBlock_h
