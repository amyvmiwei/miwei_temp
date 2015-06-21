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
/// Declarations for Schema.
/// This file contains type declarations for Schema, a class representing a
/// table schema specification.

#ifndef Hypertable_Lib_Schema_h
#define Hypertable_Lib_Schema_h

#include <Common/Properties.h>
#include "Common/PageArenaAllocator.h"
#include <Common/StringExt.h>

#include <Hypertable/Lib/AccessGroupSpec.h>
#include <Hypertable/Lib/ColumnFamilySpec.h>
#include <Hypertable/Lib/TableParts.h>

#include <expat.h>

#include <bitset>
#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// %Schema specification.
  class Schema {
  public:

    /// Default constructor.
    Schema() : m_arena(1024), m_counter_mask(256) {
      m_access_group_map = CstrAccessGroupMap(LtCstr(), CstrAlloc(m_arena));
      m_column_family_map = CstrColumnFamilyMap(LtCstr(), CstrAlloc(m_arena));
    }

    /// Copy constructor.
    /// Copies contents of <code>other</code> schema into this one and then
    /// calls validate().
    /// @param other Other schema from which to copy
    Schema(const Schema &other);

    /// Destructor.
    /// Deletes all access group specifications in #m_access_groups
    ~Schema();

    /// Clears generation values.
    /// Sets #m_generation to 0 and sets the generation value of all column
    /// families within all access groups to 0.
    void clear_generation();

    /// Clears generation if different than <code>original</code>.
    /// Compares this object with <code>original</code> and if then differ, sets
    /// #m_generation to 0 and returns <i>true</i>.  Comparison of each member
    /// access group is compared with
    /// AccessGroupSpec::clear_generation_if_changed() causing the access
    /// group spec's generation to be set to 0 if they differ.
    /// @param original Original schema with which to compare
    /// @return <i>true</i> if this object differs from <code>original</code>,
    /// <i>false</i> otherwise.
    bool clear_generation_if_changed(Schema &original);

    /// Updates generation and assigns column family IDs.
    /// For each column family specification that has a generation value of
    /// zero, its generation is set to <code>generation</code>, and if its ID is
    /// also zero, it is assigned an ID that is one larger than the maximum
    /// column family ID of all of the column families.  If any column family
    /// was assigned the new generation value, <code>generation</code>, then
    /// #m_generation is also set to <code>generation</code>.  Lastly,
    /// validate() is called.
    /// @param generation New generation
    void update_generation(int64_t generation);

    /** Renders schema in XML format.
     * The name of the toplevel element of the output is <i>%Schema</i> and
     * contains the following child elements:
     * <table>
     * <tr>
     * <th>Element</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>Generation</td>
     * <td>Generation number</td>
     * </tr>
     * <tr>
     * <td>GroupCommitInterval</td>
     * <td>Group commit interval</td>
     * </tr>
     * <tr>
     * <td>AccessGroupDefaults</td>
     * <td>Default access group options</td>
     * </tr>
     * <tr>
     * <td>ColumnFamilyDefaults</td>
     * <td>Default column family options</td>
     * </tr>
     * <tr>
     * <td>%AccessGroup</td>
     * <td>Access group specification (see AccessGroupSpec::render_xml())</td>
     * </tr>
     * </table>
     * The following is example output produced by this member function.
     * @verbatim
     <Schema>
       <Generation>42</Generation>
       <GroupCommitInterval>100</GroupCommitInterval>
       <AccessGroupDefaults>
         <BlockSize>65000</BlockSize>
       </AccessGroupDefaults>
       <ColumnFamilyDefaults>
       </ColumnFamilyDefaults>
       <AccessGroup name="default">
         <Options>
           <BlockSize>65000</BlockSize>
         </Options>
         <ColumnFamilyDefaults>
         </ColumnFamilyDefaults>
         <ColumnFamily id="1">
           <Generation>42</Generation>
           <Name>actions</Name>
           <AccessGroup>default</AccessGroup>
           <Deleted>false</Deleted>
           <Options>
             <MaxVersions>3</MaxVersions>
             <TTL>2592000</TTL>
           </Options>
         </ColumnFamily>
       </AccessGroup>
       <AccessGroup name="meta">
         <Options>
           <BlockSize>65000</BlockSize>
         </Options>
         <ColumnFamilyDefaults>
           <MaxVersions>2</MaxVersions>
         </ColumnFamilyDefaults>
         <ColumnFamily id="2">
           <Generation>42</Generation>
           <Name>language</Name>
           <AccessGroup>meta</AccessGroup>
           <Deleted>false</Deleted>
           <Options>
             <MaxVersions>2</MaxVersions>
           </Options>
         </ColumnFamily>
         <ColumnFamily id="3">
           <Generation>42</Generation>
           <Name>checksum</Name>
           <AccessGroup>meta</AccessGroup>
           <Deleted>false</Deleted>
           <Options>
             <MaxVersions>1</MaxVersions>
           </Options>
         </ColumnFamily>
       </AccessGroup>
     </Schema>
     @endverbatim
     * @param with_ids Include generation and column IDs in output
     * @return %String representation of schema in XML format
     */
    const std::string render_xml(bool with_ids=false);

    /// Renders schema as HQL <code>CREATE TABLE</code> statement.
    /// The following is example output produced by this member function with
    /// "MyTable" passed in as <code>table_name</code>.
    /// <pre>
    /// CREATE TABLE MyTable (
    ///   actions MAX_VERSIONS 3 TTL 2592000,
    ///   language MAX_VERSIONS 2,
    ///   checksum MAX_VERSIONS 1,
    ///   ACCESS GROUP default (actions) BLOCKSIZE 65000,
    ///   ACCESS GROUP meta (language, checksum) BLOCKSIZE 65000 MAX_VERSIONS 2
    /// ) GROUP_COMMIT_INTERVAL 200 BLOCKSIZE 65000
    /// </pre>
    /// @param table_name Name of table
    /// @return %String representation of schema as HQL
    /// <code>CREATE TABLE</code> statement.
    const std::string render_hql(const std::string &table_name);

    /// Gets generation.
    /// Returns the value of #m_generation.
    /// @returns Generation
    int64_t get_generation() const { return m_generation; }

    /// Sets generation.
    /// Sets #m_generation to <code>generation</code>
    /// @param generation New generation value
    void set_generation(int64_t generation) { m_generation = generation; }

    /// Gets version number.
    /// Returns the value of #m_version.
    /// @returns Version Number
    int32_t get_version() const { return m_version; }

    /// Sets version number.
    /// Sets #m_version to <code>version</code>
    /// @param version New version number
    void set_version(int32_t version) { m_version = version; }

    /// Gets the maximum column family ID.
    /// @return Maximum column family ID.
    int32_t get_max_column_family_id();

    /// Gets table parts.
    /// @return TableParts object describing table parts defined by this schema.
    TableParts get_table_parts();

    /// Returns reference to access group vector.
    /// @return Reference to access group vector.
    AccessGroupSpecs &get_access_groups() { return m_access_groups; }

    /// Gets an access group specification given its name.
    /// Searches #m_access_group_map for the access group named
    /// <code>name</code>, returning it if it exists or nullptr if it does not.
    /// @param name Name of access group to return
    /// @return Access group specification corresponding to <code>name</code>,
    /// or nullptr if it does not exist.
    AccessGroupSpec *get_access_group(const std::string &name) {
      return get_access_group(name.c_str());
    }
    AccessGroupSpec *get_access_group(const char* name) {
      auto iter = m_access_group_map.find(name);
      if (iter != m_access_group_map.end())
        return iter->second;
      return nullptr;
    }

    /// Returns reference to column family vector.
    /// @return Reference to column family vector.
    ColumnFamilySpecs &get_column_families() { return m_column_families; }

    /// Gets a column family specification given its name.
    /// Searches #m_column_family_map for the column family named
    /// <code>name</code>, returning it if it exists or nullptr if it does not.
    /// @param name Name of column family to return
    /// @return %Column family specification corresponding to <code>name</code>,
    /// or nullptr if it does not exist.
    ColumnFamilySpec *get_column_family(const std::string &name) {
      return get_column_family(name.c_str());
    }
    ColumnFamilySpec *get_column_family(const char* name) {
      auto iter = m_column_family_map.find(name);
      if (iter != m_column_family_map.end())
        return iter->second;
      return nullptr;
    }

    /// Removes column family.
    /// Removes column family named <code>name</code> from #m_column_family_map.
    /// If column family ID is non-zero, then the column family is removed from
    /// #m_column_family_id_map and the corresponding entry in #m_counter_mask
    /// is set to false.  If column family exists in #m_column_family_map, then
    /// it is returned, otherwise, nullptr is returned.
    /// @param name Name of column family to remove
    /// @return Removed column family
    ColumnFamilySpec *remove_column_family(const std::string &name);

    /// Gets a column family specification given its ID.
    /// Searches #m_column_family_id_map for the column family with ID
    /// <code>id</code>, returning it if it exists and is not deleted or
    /// <code>get_deleted</code> is <i>true</i>.  Otherwise, nullptr is
    /// returned.
    /// @param id %Column family ID of specification to fetch
    /// @param get_deleted Return specification even if it is deleted
    /// @return %Column family specification with ID <code>id</code>
    /// if it exists and it is not deleted or <code>get_deleted</code> is
    /// <i>true</i>.  Otherwise, nullptr is returned.
    ColumnFamilySpec *get_column_family(int32_t id, bool get_deleted=false) {
      auto iter = m_column_family_id_map.find(id);
      if (iter != m_column_family_id_map.end()) {
        if (get_deleted || iter->second->get_deleted() == false)
          return (iter->second);
      }
      return nullptr;
    }

    /// Adds access group specification.
    /// Merges access group defaults (#m_ag_defaults) into <code>ag</code> and
    /// merges column family defaults (#m_cf_defaults) into each of its column
    /// family specs.  Pushes <code>ag</code> onto the end of #m_access_groups
    /// and inserts it into #m_access_group_map.  
    /// @param ag Access group specification
    /// @throws Exception with code set to Error::TOO_MANY_COLUMNS if added
    /// access group pushes the number of column families past the maximum, or
    /// Error::BAD_SCHEMA if access group or any of its colums already exist.
    void add_access_group(AccessGroupSpec *ag);

    /// Replaces access group specification.
    /// Removes access group specification with the same name as the name of
    /// <code>new_ag</code> from #m_access_groups and then adds
    /// <code>new_ag</code> with a call to add_access_group().
    /// @param new_ag Replacement access group specification
    /// @return Old access group specification
    AccessGroupSpec *replace_access_group(AccessGroupSpec *new_ag);

    /// Checks if access group exists.
    /// Looks up <code>name</code> in #m_access_group_map and returns
    /// <i>true</i> if it is found.
    /// @param name Access group name
    /// @return <i>true</i> if access group exists, <i>false</i> otherwise
    bool access_group_exists(const std::string &name) const;

    /// Checks if column family exists.
    /// Checks if the column family with ID, <code>id</code>, exists by
    /// searching for it in #m_column_family_id_map.  Returns <i>true</i> if
    /// column family exists and either a) is not deleted, or b) is deleted and
    /// the value passed in for <code>get_deleted</code> is <i>true</i>.
    /// @param id ID of column family to search for
    /// @param get_deleted Return 
    /// @return <i>true</i> if column family exists, <i>false</i> otherwise.
    bool column_family_exists(int32_t id, bool get_deleted = false) const;

    /// Drops column family.
    /// Locates access group for column family <code>name</code> and marks it as
    /// deleted with a call to AccessGroupSpec::drop_column(), then re-inserts
    /// the column family back into #m_column_family_map with its new new
    /// ("!<id>").
    /// @param name Name of column family to drop
    void drop_column_family(const String& name);

    /// Renames a column family.
    /// Locates column family <code>old_name</code> in #m_column_family_map,
    /// renames it to <code>new_name</code> and sets its generation to 0.
    /// @param old_name Name of existing column family to be renamed
    /// @param new_name New name for column family
    void rename_column_family(const String& old_name, const String& new_name);

    /// Checks if column is a counter column.
    /// Returns the value of the <code>id</code><sup>th</sup> bit of
    /// #m_counter_mask.
    /// @param id %Column family ID
    /// @return <i>true</i> if column is a counter, <i>false</i> otherwise.
    bool column_is_counter(int id) {
      return m_counter_mask[id];
    }

    /// Sets group commit interval.
    /// Sets #m_group_commit_interval to <code>interval</code>.
    /// @param interval New value for group commit interval
    void set_group_commit_interval(int32_t interval) {
      m_group_commit_interval = interval;
    }

    /// Gets group commit interval.
    /// @return Group commit interval
    int32_t get_group_commit_interval() { return m_group_commit_interval; }

    /// Sets default access group options.
    /// Sets #m_ag_defaults to <code>defaults</code>
    /// @param defaults Access group options to use as table defaults
    void set_access_group_defaults(const AccessGroupOptions &defaults) {
      m_ag_defaults = defaults;
    }

    /// Returns reference to default access group options.
    /// @return Reference to default access group options.
    AccessGroupOptions &access_group_defaults() { return m_ag_defaults; }

    /// Sets default column family options.
    /// Sets #m_cf_defaults to <code>defaults</code>
    /// @param defaults Column family options to use as table defaults
    void set_column_family_defaults(const ColumnFamilyOptions &defaults) {
      m_cf_defaults = defaults;
    }

    /// Returns reference to default column family options.
    /// @return Reference to default column family options.
    ColumnFamilyOptions &column_family_defaults() { return m_cf_defaults; }

    /// Validates schema and reconstructs data structures.
    /// Reconstructs #m_column_families, #m_column_family_map,
    /// #m_column_family_id_map, and #m_counter_mask by recursing through
    /// #m_access_groups.  During the data structure reconstruction, validation
    /// checks are performed to make sure there aren't too many columns or there
    /// exists a column that is assigned to two different access groups.
    /// @throws Exception with code set to Error::TOO_MANY_COLUMNS if too many
    /// columns are defined, or Error::BAD_SCHEMA if the same column is assigned
    /// to two different access groups.
    void validate();

    /// Creates schema object from XML schema string.
    /// Constructs a new schema from the XML specification held in
    /// <code>buf</code> and them calls validate().
    /// @param buf Buffer holding XML schema string.
    /// @return Pointer to newly allocated Schema object.
    static Schema *new_instance(const std::string &buf);

  private:

    /// Merges default column family options into a column family spec.
    /// @param cf_spec Column family spec into which table defaults are to be
    /// merged
    void merge_table_defaults(ColumnFamilySpec *cf_spec);

    /// Merges default access group options into access group spec.
    /// @param ag_spec Access group spec into which table defaults are to be
    /// merged
    void merge_table_defaults(AccessGroupSpec *ag_spec);

    //  Page arena allocator
    typedef PageArenaAllocator<const char*> CstrAlloc;
    CharArena m_arena;

    /// Generation
    int64_t m_generation {};

    /// Version number
    int32_t m_version {};

    /// Group commit interval
    int32_t m_group_commit_interval {};

    /// Default access group options
    AccessGroupOptions m_ag_defaults;

    /// Default column family options
    ColumnFamilyOptions m_cf_defaults;

    /// Access group specifications
    AccessGroupSpecs m_access_groups;

    /// Map of access group specifications
    typedef std::map<const char*, AccessGroupSpec*, LtCstr, CstrAlloc> CstrAccessGroupMap;
    CstrAccessGroupMap m_access_group_map;

    /// &Column family specifications
    ColumnFamilySpecs m_column_families;

    /// Map of column family specifications (key == name)
    typedef std::map<const char*, ColumnFamilySpec*, LtCstr, CstrAlloc> CstrColumnFamilyMap;
    CstrColumnFamilyMap m_column_family_map;

    /// Map of column family specifications (key == ID)
    std::map<int32_t, ColumnFamilySpec *> m_column_family_id_map;

    /// Bitmask describing which column families are counters
    std::vector<bool> m_counter_mask;
  };

  /// Smart pointer to Schema
  typedef std::shared_ptr<Schema> SchemaPtr;

  /// @}

}

#endif // Hypertable_Lib_Schema_h
