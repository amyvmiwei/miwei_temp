/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Declarations for GroupCommitInterface.
/// This file contains declarations for GroupCommitInterface, a class that
/// defines the interface to the group commit mechanism.

#ifndef HYPERSPACE_GROUPCOMMITINTERFACE_H
#define HYPERSPACE_GROUPCOMMITINTERFACE_H

#include <Hypertable/RangeServer/Range.h>
#include <Hypertable/RangeServer/TableInfo.h>

#include <Hypertable/Lib/CommitLog.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/Types.h>

#include <AsyncComm/Event.h>

#include <Common/ReferenceCount.h>
#include <Common/StaticBuffer.h>

#include <unordered_map>
#include <vector>

#if 0
/** GNU C++ extensions. */
namespace __gnu_cxx {
  template<> struct hash<Hypertable::Range *>  {
    size_t operator()(const Hypertable::Range *x) const {
      return (size_t)x;
    }
  };
}
#endif

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Describes portion of an update buffer rejected due to error.
  struct SendBackRec {
    /// Error code
    int error;
    /// Number of key/value pairs to which #error applies
    uint32_t count;
    /// Starting byte offset within update buffer of rejected key/value pairs
    uint32_t offset; 
    /// Length (in bytes) from #offset covering key/value pairs rejected
    uint32_t len;
  };

  /// Holds client update request and error state.
  class ClientUpdateRequest {
  public:
    /// Default constructor.
    ClientUpdateRequest() : count(0), error(0) { }
    /// Update buffer containing serialized key/value pairs
    StaticBuffer buffer;
    /// Count of serialized key/value pairs in #buffer
    uint32_t count;
    /// Event object of originating update requst
    EventPtr event;
    /// Vector of SendBacRec objects describing rejected key/value pairs
    std::vector<SendBackRec> send_back_vector;
    /// Error code that applies to entire buffer
    uint32_t error;
  };

  class RangeUpdate {
  public:
    DynamicBuffer *bufp;
    uint64_t offset;
    uint64_t len;
  };

  class RangeUpdateList {
  public:
    RangeUpdateList() : starting_update_count(0), last_request(0), transfer_buf_reset_offset(0),
                        latest_transfer_revision(TIMESTAMP_MIN), range_blocked(false) { }
    void reset_updates(ClientUpdateRequest *request) {
      if (request == last_request) {
        if (starting_update_count < updates.size())
          updates.resize(starting_update_count);
        transfer_buf.ptr = transfer_buf.base + transfer_buf_reset_offset;
      }
    }
    void add_update(ClientUpdateRequest *request, RangeUpdate &update) {
      if (request != last_request) {
        starting_update_count = updates.size();
        last_request = request;
        transfer_buf_reset_offset = transfer_buf.empty() ? 0 : transfer_buf.fill();
      }
      if (update.len)
        updates.push_back(update);
    }
    RangePtr range;
    std::vector<RangeUpdate> updates;
    size_t starting_update_count;
    ClientUpdateRequest *last_request;
    DynamicBuffer transfer_buf;
    uint32_t transfer_buf_reset_offset;
    int64_t latest_transfer_revision;
    CommitLogPtr transfer_log;
    bool range_blocked;
  };

  class TableUpdate {
  public:
    TableUpdate() : flags(0), commit_interval(0), total_count(0),
                    total_buffer_size(0), wait_for_metadata_recovery(false),
                    wait_for_system_recovery(false), sync(false),
                    transfer_count(0), total_added(0), error(0) {}
    ~TableUpdate() {
      foreach_ht (ClientUpdateRequest *r, requests)
        delete r;
      for (std::unordered_map<Range *, RangeUpdateList *>::iterator iter = range_map.begin(); iter != range_map.end(); ++iter)
        delete (*iter).second;
    }
    /// Cluster from which these updates originated
    uint64_t cluster_id;
    TableIdentifier id;
    std::vector<ClientUpdateRequest *> requests;
    uint32_t flags;
    uint32_t commit_interval;
    uint32_t commit_iteration;
    uint64_t total_count;
    uint64_t total_buffer_size;
    TableInfoPtr table_info;
    boost::xtime expire_time;
    std::unordered_map<Range *, RangeUpdateList *> range_map;
    DynamicBuffer go_buf;
    bool wait_for_metadata_recovery;
    bool wait_for_system_recovery;
    bool sync;
    uint32_t transfer_count;
    uint32_t total_added;
    int error;
    String error_msg;
  };

  /// Abstract base class for group commit implementation.
  /// <i>Group commit</i> is a feature whereby updates are queued over some
  /// period of time (usually measured in milliseconds) and then written and
  /// sync'ed to the commit log and processed in one group.  This improves
  /// efficiency for situations where there are many concurrent updates
  /// because otherwise the writing and sync'ing of the commit log can become
  /// a bottleneck.  This class acts as an interface class to the group
  /// commit implementation and provides a way for the group commit system
  /// and the RangeServer to reference one another.
  class GroupCommitInterface : public ReferenceCount {
  public:

    /// Adds a batch of updates to the group commit queue.
    virtual void add(EventPtr &event, uint64_t cluster_id, SchemaPtr &schema,
                     const TableIdentifier *table, uint32_t count,
                     StaticBuffer &buffer, uint32_t flags) = 0;

    /// Processes queued updates that are ready to be committed.
    virtual void trigger() = 0;
  };

  /// Smart pointer to GroupCommitInterface
  typedef boost::intrusive_ptr<GroupCommitInterface> GroupCommitInterfacePtr;

  /// @}
}

#endif // HYPERSPACE_GROUPCOMMITINTERFACE_H

