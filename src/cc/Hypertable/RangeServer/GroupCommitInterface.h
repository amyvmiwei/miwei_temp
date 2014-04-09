/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/Types.h>

#include <AsyncComm/Event.h>

#include <Common/StaticBuffer.h>

#include <memory>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Abstract base class for group commit implementation.
  /// <i>Group commit</i> is a feature whereby updates are queued over some
  /// period of time (usually measured in milliseconds) and then written and
  /// sync'ed to the commit log and processed in one group.  This improves
  /// efficiency for situations where there are many concurrent updates
  /// because otherwise the writing and sync'ing of the commit log can become
  /// a bottleneck.  This class acts as an interface class to the group
  /// commit implementation and provides a way for the group commit system
  /// and the RangeServer to reference one another.
  class GroupCommitInterface {
  public:

    /// Adds a batch of updates to the group commit queue.
    virtual void add(EventPtr &event, uint64_t cluster_id, SchemaPtr &schema,
                     const TableIdentifier *table, uint32_t count,
                     StaticBuffer &buffer, uint32_t flags) = 0;

    /// Processes queued updates that are ready to be committed.
    virtual void trigger() = 0;
  };

  /// Smart pointer to GroupCommitInterface
  typedef std::shared_ptr<GroupCommitInterface> GroupCommitInterfacePtr;

  /// @}
}

#endif // HYPERSPACE_GROUPCOMMITINTERFACE_H

