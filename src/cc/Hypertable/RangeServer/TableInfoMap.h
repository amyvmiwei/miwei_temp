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
/// Declarations for TableInfoMap.
/// This file contains the type declarations for TableInfoMap, a class used to
/// map table IDs to TableInfo objects and manage the set of "remove ok" logs.

#ifndef Hypertable_RangeServer_TableInfoMap_h
#define Hypertable_RangeServer_TableInfoMap_h

#include <Hypertable/RangeServer/HyperspaceTableCache.h>
#include <Hypertable/RangeServer/TableInfo.h>

#include <Common/StringExt.h>

#include <boost/intrusive_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include <map>
#include <string>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /** Manages live range map and set of log names that can be safely removed.
   * This class is used to maintain an active set of ranges, organized by table
   * ID.  It maps table IDs to TableInfo objects holding a set of active Range
   * objects.  It also manages a set of transfer log names that can be
   * safely removed from the system.  The maintenance scheduler is responsible
   * for removing commit log fragments (including linked transfer logs).  It
   * does this by obtaining the set of live ranges, computing some statistics
   * for each range and then removing the log fragments that contain no data
   * that hasn't been compacted.  However, loading a range is a two step
   * process that introduces a race condition: 1) the range's transfer
   * log is linked into the commit log and 2) the range is persisted to the
   * RSML.  If the maintenance scheduler does its commit log purging work
   * between steps #1 and #2, it' possible for a newly added transfer log to
   * get removed before the Range has been fully loaded.  To avoid this race
   * condition we've introduced MetaLogEntityRemoveOkLogs which contains the
   * set of transfer logs that have been linked in and which may be safely
   * removed.  The MetaLogEntityRemoveOkLogs entity is modified and persisted
   * atomically to the RSML with new Range entities by the promote_staged_range()
   * method.  The get_ranges() method returns a consistent snapshot of the
   * current set of live ranges and the set of transfer logs in
   * MetaLogEntityRemoveOkLogs.
   */
  class TableInfoMap : public ReferenceCount {
  public:

    /** Constructor. */
    TableInfoMap() { }

    /** Constructor with HyperspaceTableCache.
     * Objects constructed with this constructor will use the provided
     * schema cache to lookup schemas instead of reading them from
     * %Hyperspace.
     * @param schema_cache Table schema cache
     */
    TableInfoMap(HyperspaceTableCachePtr schema_cache) : m_schema_cache(schema_cache) { }

    /** Destructor. */
    virtual ~TableInfoMap();

    /** Returns the TableInfo object for a given table.
     * @param table_id %Table identifier string
     * @param info Output parameter to hold TableInfo object
     * @return <i>true</i> if mapping found for <code>table_id</code>,
     * <i>false</i> otherwise.
     */
    bool lookup(const String &table_id, TableInfoPtr &info);

    /** Gets the TableInfo object for a table, creating one if not found.
     * @param table_id %Table identifier string
     * @param info Output parameter to hold TableInfo object
     * @throws Exception with code set to Error::RANGESERVER_SCHEMA_PARSE_ERROR
     */
    void get(const String &table_id, TableInfoPtr &info);

    /** Adds a staged range.
     * When staging is complete for a range, the Range object is added
     * with a call to this method.  Since a range's transfer log cannot
     * be removed until the range has been added, this method adds the range
     * and adds the range's tranfer log to the the global
     * MetaLogEntityRemoveOkLogs object (Global::remove_ok_logs) in one atomic
     * transaction.  This method performs the following actions with #m_mutex
     * locked:
     *
     *   - Replays <code>transfer_log</code> with a call to
     *     Range::replay_transfer_log
     *   - Links <code>transfer_log</code> into the appropriate commit log
     *   - Adds <code>transfer_log</code> to Global::remove_ok_logs
     *   - Persists Global::remove_ok_logs and the range's metalog entity
     *     to the RSML
     *   - Calls TableInfo::promote_staged_range() to make the range object live
     *
     * @param table %Table identifier
     * @param range %Range object
     * @param transfer_log Transfer log for <code>range</code>
     * @see stage_range
     */
    void promote_staged_range(const TableIdentifier *table, RangePtr &range, const char *transfer_log);

    /** Removes a table from the map
     * @param table_id %Table identifier string
     * @param info Output parameter to hold removed TableInfo object
     * @return <i>true</i> if table exists in map and is removed, <i>false</i>
     * otherwise.
     */
    bool remove(const String &table_id, TableInfoPtr &info);

    /** Gets all TableInfo objects in map.
     * @param tv Output vector to hold TableInfo objects
     */
    void get_all(std::vector<TableInfoPtr> &tv);

    /** Gets set of live RangeData objects and corresponding transfer
     * logs that can be safely removed.
     * This method is used to fetch all of the live range objects in the
     * map.  The maintenance scheduler is responsible for removing commit
     * log fragments (including linked transfer logs).  It does this by
     * obtaining the set of live ranges, computing some statistics for each
     * range and then removing the log fragments that contain no data
     * that hasn't been compacted.  However, loading a range is a two step
     * process that introduces a race condition: 1) the range's transfer
     * log is linked into the commit log and 2) the range is persisted to the
     * RSML.  If the maintenance scheduler does its commit log purging work
     * between steps #1 and #2, it' possible for a newly added transfer log to
     * get removed before the Range has been fully loaded.  To avoid this race
     * condition we've introduced MetaLogEntityRemoveOkLogs which contains the
     * set of transfer logs that have been linked in and which may be safely
     * removed.  The MetaLogEntityRemoveOkLogs entity is modified and persisted
     * atomically to the RSML with new Range entities by the promote_staged_range()
     * method.  This method returns a consistent snapshot of the current set of
     * live ranges and the set of transfer logs in MetaLogEntityRemoveOkLogs.
     * @param ranges Output parameter to hold RangeData objects
     * @param remove_ok_logs Pointer to string set to hold logs that can be
     * removed
     * @see promote_staged_range
     */
    void get_ranges(Ranges &ranges, StringSet *remove_ok_logs=0);

    /** Clears the map. */
    void clear();

    /** Determines if map is empty.
     * @return <i>true</i> if map is empty, <i>false</i> otherwise
     */
    bool empty();

    /** Merges in another map.
     * This method locks #m_mutex and calls merge_unlocked().  It should only
     * be called for ranges that have already been persisted into the RSML
     * from a previous call to promote_staged_range() such as during the loading
     * of ranges from the RSML at system startup.
     * @param other Map to merge in
     */
    void merge(TableInfoMap *other);

    /** Merges in another map and updates RMSL with merged entities.
     * This method atomically adds <code>transfer_logs</code> to
     * Global::remove_ok_logs, persists <code>entities</code> and
     * Global::remove_ok_logs to the RSML, and merges in another map.
     * <code>entities</code> and <code>transfer_logs</code> should correspond
     * to the ranges in <code>other</code> that are to be merged in.
     * This method performs these steps atomically (with #m_mutex locked)
     * to avoid the race condition described in get_ranges()
     * @param other Map to merge in
     * @param entities Vector of range entities
     * @param transfer_logs Set of transfer logs
     * @see get_ranges
     */
    void merge(TableInfoMap *other, vector<MetaLog::Entity *> &entities,
               StringSet &transfer_logs);

  private:

    /** Merges in another map (without locking mutex).
     * For each TableInfo object in <code>other</code>, if there is no
     * corresponding TableInfo object in this object's map, the TableInfo object
     * is directly inserted into this object's map (#m_map).  If there is
     * already a TableInfo object in this object's map, then all of the
     * RangeData objects are fetched from the other TableInfo object and
     * inserted into this object's TableInfo object. At the end of the method
     * <code>other</code> is cleared.
     * @param other Map to merge in
     */
    void merge_unlocked(TableInfoMap *other);

    /// table_id-to-TableInfoPtr map type
    typedef std::map<String, TableInfoPtr> InfoMap;

    /// %Mutex for serializing access
    Mutex m_mutex;
    
    /// %Hyperspace table cache
    HyperspaceTableCachePtr m_schema_cache;

    /// table_id-to-TableInfoPtr map
    InfoMap m_map;
  };

  /// Smart pointer to TableInfoMap
  typedef boost::intrusive_ptr<TableInfoMap> TableInfoMapPtr;

  /// @}
}

#endif // Hypertable_RangeServer_TableInfoMap_h
