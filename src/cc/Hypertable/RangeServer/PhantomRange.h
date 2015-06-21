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
/// Declarations for PhantomRange.
/// This file contains declarations for PhantomRange, a class representing a
/// "phantom" range (i.e. one that is being recovered by a RangeServer).

#ifndef Hypertable_RangeServer_PhantomRange_h
#define Hypertable_RangeServer_PhantomRange_h

#include <Hypertable/RangeServer/Range.h>
#include <Hypertable/RangeServer/TableInfo.h>
#include <Hypertable/RangeServer/FragmentData.h>

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeState.h>

#include <Common/String.h>
#include <Common/Filesystem.h>

#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace Hypertable {

  using namespace std;

  /// @addtogroup RangeServer
  /// @{

  /// Represents a "phantom" range.
  class PhantomRange {

  public:
    enum State {
      LOADED    = 0x00000001,
      REPLAYED  = 0x00000002,
      PREPARED  = 0x00000004,
      COMMITTED = 0x00000008,
      CANCELLED = 0x00000010
    };

    PhantomRange(const QualifiedRangeSpec &spec, const RangeState &state,
                 SchemaPtr &schema, const vector<int32_t> &fragments);
    ~PhantomRange() {}
    /**
     *
     * @param fragment fragment id
     * @param event contains data fort his fragment
     * @return true if the add succeded, false means the fragment is already complete
     */
    bool add(int32_t fragment, EventPtr &event);
    int get_state();
    const RangeState &get_range_state() { return m_range_state; }

    void purge_incomplete_fragments();
    void create_range(Lib::Master::ClientPtr &master_client, TableInfoPtr &table_info,
                      FilesystemPtr &log_dfs);
    RangePtr& get_range() {
      lock_guard<mutex> lock(m_mutex);
      return m_range;
    }

    void populate_range_and_log(FilesystemPtr &log_dfs, int64_t recovery_id,
                                bool *is_empty);
    CommitLogReaderPtr get_phantom_log();
    const String &get_phantom_logname();

    void set_replayed();
    bool replayed();

    void set_prepared();
    bool prepared();

    void set_committed();
    bool committed();

  private:

    String create_log(FilesystemPtr &log_dfs,
                      int64_t recovery_id,
                      MetaLogEntityRangePtr &range_entity);

    typedef std::map<int32_t, FragmentDataPtr> FragmentMap;

    std::mutex m_mutex;
    FragmentMap      m_fragments;
    QualifiedRangeSpec m_range_spec;
    RangeState       m_range_state;
    SchemaPtr        m_schema;
    size_t           m_outstanding;
    RangePtr         m_range;
    CommitLogReaderPtr m_phantom_log;
    String           m_phantom_logname;
    int              m_state;
  };

  /// Shared smart pointer to PhantomRange
  typedef std::shared_ptr<PhantomRange> PhantomRangePtr;

  /// @}

}

#endif // Hypertable_RangeServer_PhantomRange_h
