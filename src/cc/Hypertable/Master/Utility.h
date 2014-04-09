/*
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
/// Declarations for general-purpose utility functions.
/// This file contains declarations for a set of general-purpose utility
/// functions used by the %Master.

#ifndef HYPERTABLE_UTILITY_H
#define HYPERTABLE_UTILITY_H

#include "Common/StringExt.h"

#include "Hypertable/Lib/Types.h"

#include "Context.h"

namespace Hypertable {

  /// General-purpose utilities.
  namespace Utility {

    /// @addtogroup Master
    /// @{

    /** Gets set of servers holding ranges for a given table.
     * Scans the METADATA table to determine which %RangeServers
     * hold ranges for the table with identifier <code>id</code>.
     * @param context %Master context
     * @param id %Table identifier
     * @param row Get servers containing this row ("" implies all)
     * @param servers Reference to string set to hold %RangeServer names
     */
    extern void get_table_server_set(ContextPtr &context, const String &id,
                                     const String &row, StringSet &servers);

    /** Checks if table exists and returns table ID.
     * Checks to see if table <code>name</code> exists and returns the
     * corresponding table ID in the <code>id</code> parameter.  It considers
     * the table to exist if it has an ID mapping in %Hyperspace and the
     * <i>x</i> attribute exists.  The creation of the <i>x</i> atribute
     * on a table's ID file in %Hyperspace is the last thing that happens
     * in a create table operation.
     * @param context %Master context
     * @param name Name of table to check for existance
     * @param id Return parameter to hold table ID
     * @return <i>true</i> if table exists, <i>false</i> otherwise
     */
    extern bool table_exists(ContextPtr &context, const String &name,
                             String &id);

    /** Checks if table ID exists.
     * Checks to see if table specified by <code>id</code> exists.  It considers
     * the table to exist if the ID file exists in %Hyperspace and the the
     * <i>x</i> attribute exists.  The creation of the <i>x</i> atribute on a
     * table's ID file in %Hyperspace is the last thing that happens in a create
     * table operation.     
     * @param context %Master context
     * @param id ID of table to check for existance
     * @return <i>true</i> if table exists, <i>false</i> otherwise
     */
    extern bool table_exists(ContextPtr &context, const String &id);

    /** Checks if table name is available.
     * Checks to see if table <code>name</code> is available for
     * use in a create table operation.  It considers the table name to be
     * available if it does not have an ID mapping, or the <i>x</i> attribute
     * does not exist.  The creation of the <i>x</i> atribute on a table's ID
     * file in %Hyperspace is the last thing that happens in a create table
     * operation.  If this function retuns without throwing an exception, then
     * <code>name</code> can be considered available.  If <code>name</code>
     * has an ID mapping, but no <i>x</i> attribute, then the ID is returned
     * in <code>id</code>.
     * @param context %Master context
     * @param name %Table name to check for availability
     * @param id %Table ID, if already assigned
     * @throws Error::NAME_ALREADY_IN_USE if name exists and is a namespace,
     * or Error::MASTER_TABLE_EXISTS if name exists and is a table.
     */
    extern void verify_table_name_availability(ContextPtr &context,
                                               const String &name, String &id);

    /** Creates a table in %Hyperspace.
     * Adds a table ID mapping for <code>name</code>, creates a schema object
     * from <code>schema_str</code> and assigns column family IDs and writes
     * <i>schema</i> attribute of ID file in %Hyperspace, and sets the table ID
     * and generation attributes of <code>table</code>.  This method also
     * creates access group directories 
     * (<code>/hypertable/tables/$TABLE_ID/$ACCESS_GROUP</code>) for the table
     * in the DFS.
     * @param context %Master context
     * @param name %Table name to create
     * @param schema_str Schema XML string
     * @param table Pointer to table identifier
     */
    extern void 
    create_table_in_hyperspace(ContextPtr &context, const String &name,
                               const String &schema_str,
                               TableIdentifierManaged *table);
    
    /** Prepares index schema and table name.
     * Creates a schema for an index table associated with primary table
     * <code>name</code>.  The group commit interval is obtained from the
     * primary table schema and copied to the index table schema.  The
     * index name is formualted by prepending "^" to the primary table
     * name for a value index and "^^" for a qualifier index.
     * @param context %Master context
     * @param name Name of table for which to prepare index
     * @param schema_str XML schema string for primary table
     * @param qualifier Indicates if preparation is for qualifier index
     * @param index_name Return string to hold index table name
     * @param index_schema Return string to hold index table XML schema
     */
    extern void prepare_index(ContextPtr &context, const String &name,
                              const String &schema_str, bool qualifier,
                              String &index_name, String &index_schema);

    /** Creates initial METADATA table entry for <code>table</code>.
     * Creates the initial METADATA table entry for <code>table</code> by
     * writing a new <i>StartRow</i> column with row key formulated as
     * <i>table_id</i> + ":" + Key::END_ROW_MARKER.  The value is set to NULL
     * unless table is METADATA in which case it is set to Key::END_ROOT_ROW.
     * @param context %Master context
     * @param table %Table identifier for which to create initial METADATA entry
     */
    extern void
    create_table_write_metadata(ContextPtr &context, TableIdentifier *table);

    /** Gets name of next available server.
     * Queries RangeServerConnectionManager for next available server to hold
     * a new range and sets <code>location</code> to the server name if found.
     * @param context %Master context
     * @param context Reference to string to hold next available server name
     * @param urgent Use servers that exceed disk threshold, if necessary
     * @return <i>true</i> if next available server found, <i>false</i>
     * otherwise
     */
    extern bool next_available_server(ContextPtr &context, String &location,
                                      bool urgent=false);

    /** Loads a table's initial range.
     * Load's a table's initial range in the server specified by
     * <code>location</code>.  A RangeState object is initialized with the
     * split size and passed into the RangeServer::load_range call.  The split
     * size is set as follows:
     *
     *  - If table is metadata, split size is set to the value of the
     *    <code>Hypertable.RangeServer.Range.MetadataSplitSize</code> property
     *    if it is set, otherwise it is set to the value of the
     *    <code>Hypertable.RangeServer.Range.SplitSize</code> property.
     *  - If table is not metadata, split size is set to the value of the
     *    <code>Hypertable.RangeServer.Range.SplitSize</code> property if
     *    <code>Hypertable.Master.Split.SoftLimitEnabled</code> is <i>false</i>.
     *    Otherwise, if <code>Hypertable.Master.Split.SoftLimitEnabled</code> is
     *    <i>true</i>, the split size is set to
     *    <code>Hypertable.RangeServer.Range.SplitSize / min(64, server_count*2)</code>
     *
     * @param context %Master context
     * @param location %RangeServer in which to load initial range
     * @param table %Table identifier
     * @param range %Range spec
     * @param needs_compaction Needs compaction flag
     */
    extern void
    create_table_load_range(ContextPtr &context, const String &location,
                            TableIdentifier &table, RangeSpec &range,
                            bool needs_compaction);

    /** Calls RangeServer::acknowledge_load for a range.
     * Calls RangeServer::acknowledge_load for the range specified by
     * <code>table</code> and <code>range</code>.
     * @param context %Master context
     * @param location %RangeServer at which to call acknowledge_load
     * @param table %Table identifier
     * @param range %Range spec
     * @throws Exception holding error code if not Error::OK,
     * Error::TABLE_NOT_FOUND, or Error::RANGESERVER_RANGE_NOT_FOUND
     */
    extern void
    create_table_acknowledge_range(ContextPtr &context, const String &location,
                                   TableIdentifier &table, RangeSpec &range);

    /** Returns a hash code for a range with an optional qualifer string.
     * This method computes a hash code by XOR'ing the md5_hash values for the
     * following components:
     *
     *   - <code>qualifier</code> (if not empty)
     *   - <code>table.id</code>
     *   - <code>range.start_row</code>
     *   - <code>range.end_row</code>
     *
     * @param table %Table identifier
     * @param range %Range spec
     * @param qualifier Qualifier string
     * @return Range hash code
     */
    extern int64_t
    range_hash_code(const TableIdentifier &table, const RangeSpec &range,
                    const String &qualifier);

    /** Returns string representation of hash code for a range with an optional qualifer string.
     * Calls Utility::range_hash_code and converts it to a string.
     * @param table %Table identifier
     * @param range %Range spec
     * @param qualifier Qualifier string
     * @return Range hash string
     */
    extern String
    range_hash_string(const TableIdentifier &table, const RangeSpec &range,
                      const String &qualifier);

    /** Returns location of root METADATA range.
     * Reads location of root METADATA range from <i>Location</i> attribute of
     * <code>/hypertable/root</code> file in %Hyperspace and returns it.
     * @note The top-level directory <code>/hypertable</code> may be different
     * depending on the <code>Hypertable.Directory</code> property.
     * @param context %Master context
     * @return Location of root METADATA range.
     */
    extern String root_range_location(ContextPtr &context);

    /// Canonicalizes pathname.
    /// This member function canonicalizes <code>pathname</code> by stripping
    /// any leading and trailing whitespace or '/' characters and then
    /// prepending a '/' character.
    /// @param pathname Pathname to canonicalize (modified in place)
    extern void canonicalize_pathname(std::string &pathname);

    /// @}

  } // namespace Utility

} // namespace Hypertable

#endif // HYPERTABLE_UTILITY_H
