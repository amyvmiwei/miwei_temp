/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_FRAGMENTDATA_H
#define HYPERTABLE_FRAGMENTDATA_H

#include <vector>

#include "Common/Mutex.h"
#include "Common/atomic.h"
#include "Common/ReferenceCount.h"
#include "Common/ByteString.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/Event.h"

#include "Hypertable/Lib/Key.h"

#include "ScanContext.h"
#include "Range.h"

namespace Hypertable {


  /**
   * Stores fragment data for a specific range
   */
  class FragmentData : public ReferenceCount {
  public:

    FragmentData(uint32_t id) : m_id(id), m_done(false) {}
    ~FragmentData() {}

    /**
     * Adds EventPtr to data for this fragment
     *
     * @param event event_ptr
     */
    void add(EventPtr &event);
    bool complete() const { return m_done; }
    void clear() {
      HT_ASSERT(!m_done);
      m_data.clear();
    }

    /**
     * write the contents of this fragment into the Range and the dynamic buffer
     */
    void merge(RangePtr &range, const char *split_point,
               DynamicBuffer &dbuf, int64_t *latest_revision);

  protected:
    uint32_t m_id;
    bool m_done;
    vector<EventPtr> m_data;

    typedef std::pair<uint8_t *, size_t> Fragment;
    typedef std::vector<Fragment> DeserializedFragments;
  };

  typedef boost::intrusive_ptr<FragmentData> FragmentDataPtr;
}

#endif // HYPERTABLE_FRAGMENTDATA_H
