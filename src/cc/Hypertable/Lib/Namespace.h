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

#ifndef Hypertable_Lib_Namespace_h
#define Hypertable_Lib_Namespace_h

#include <Hypertable/Lib/ClientObject.h>
#include <Hypertable/Lib/Master/Client.h>
#include <Hypertable/Lib/NameIdMapper.h>
#include <Hypertable/Lib/NamespaceListing.h>
#include <Hypertable/Lib/Table.h>
#include <Hypertable/Lib/TableCache.h>
#include <Hypertable/Lib/TableMutator.h>
#include <Hypertable/Lib/TableParts.h>
#include <Hypertable/Lib/TableScanner.h>
#include <Hypertable/Lib/TableScannerAsync.h>
#include <Hypertable/Lib/TableSplit.h>

#include <Hyperspace/Session.h>

#include <AsyncComm/ApplicationQueueInterface.h>
#include <AsyncComm/ConnectionManager.h>

#include <Common/String.h>

#include <boost/tokenizer.hpp>

#include <memory>

namespace Hypertable {

  class Comm;
  class HqlInterpreter;
  class Client;

  /// @addtogroup libHypertable
  /// @{

  class Namespace : public ClientObject {
  public:

    /**
     * Constructs the object
     *
     */
    Namespace(const std::string &name, const std::string &id, PropertiesPtr &props,
              ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
              ApplicationQueueInterfacePtr &app_queue, NameIdMapperPtr &namemap,
              Lib::Master::ClientPtr &master_client, RangeLocatorPtr &range_locator,
              TableCachePtr &table_cache, uint32_t timeout, Client *client);

    virtual ~Namespace() {}

    /**
     * Get canonical format of name/id string. This means no leading or trailing '/' and
     * cleanup '//'
     */
    static void canonicalize(String *original);

    const std::string& get_name() const {
      return m_name;
    }

    const std::string& get_id() const {
      return m_id;
    }

    /** Performs a manual compaction.
     * @param name Name of table to compact
     * @param row Optional row identifying range withing table to compact
     * @param flags Compaction flags
     *        (see Lib::RangeServer::Protocol::CompactionFlags)
     */
    void compact(const std::string &name, const std::string &row, uint32_t flags);

    /** Creates a table.
     *
     * The <code>schema_str</code> parameter is a string that contains an
     * XML-style schema specification.  The best way to learn the syntax
     * of this specification format is to create tables with HQL
     * in the command interpreter and then run DESCRIBE TABLE to
     * see what the XML specification looks like.  For example,
     * the following HQL:
     *
     * <pre>
     * CREATE TABLE COMPRESSOR="lzo" foo (
     *   a MAX_VERSIONS=1 TTL=30 DAYS,
     *   b TTL=1 WEEKS,
     *   c,
     *   ACCESS GROUP primary IN_MEMORY BLOCKSIZE=1024( a ),
     *   ACCESS GROUP secondary COMPRESSOR="zlib" BLOOMFILTER="none" ( b, c)
     * );
     *</pre>
     *
     * will create a table with the follwing XML schema:
     *
     * @verbatim
     * <Schema compressor="lzo">
     *   <AccessGroup name="primary" inMemory="true" blksz="1024">
     *     <ColumnFamily>
     *       <Name>a</Name>
     *       <MaxVersions>1</MaxVersions>
     *       <ttl>2592000</ttl>
     *     </ColumnFamily>
     *   </AccessGroup>
     *   <AccessGroup name="secondary" compressor="zlib" bloomFilter="none">
     *     <ColumnFamily>
     *       <Name>b</Name>
     *       <ttl>604800</ttl>
     *     </ColumnFamily>
     *     <ColumnFamily>
     *       <Name>c</Name>
     *     </ColumnFamily>
     *   </AccessGroup>
     * </Schema>
     * @endverbatim
     *
     * @param name name of the table
     * @param schema_str schema definition for the table
     */
    void create_table(const std::string &name, const std::string &schema_str);

    void create_table(const std::string &name, SchemaPtr &schema);

    /** Alter table schema.
     * @param table_name Name of table to alter
     * @param schema Schema object holding alterations
     * @param force Force table alteration even if generation mismatch
     */
    void alter_table(const std::string &table_name, SchemaPtr &schema, bool force);

#if 0
    /**
     * Alters column families within a table.  The schema parameter
     * contains an XML-style schema difference and supports a
     * 'deleted' element within the 'ColumnFamily' element which
     * indicates that the column family should be deleted.  For example,
     * the following XML diff:
     *
     * @verbatim
     * <Schema>
     *   <AccessGroup name="secondary">
     *     <ColumnFamily>
     *       <Name>b</Name>
     *       <deleted>true</deleted>
     *     </ColumnFamily>
     *   </AccessGroup>
     *   <AccessGroup name="tertiary" blksz="2048">
     *     <ColumnFamily>
     *       <Name>d</Name>
     *       <MaxVersions>5</MaxVersions>
     *     </ColumnFamily>
     *   </AccessGroup>
     * </Schema>
     * @endverbatim
     *
     * when applied to the 'foo' table, described in the create_table
     * example, generates a table that is equivalent to one created
     * with the following HQL:
     *
     * <pre>
     * CREATE TABLE COMPRESSOR="lzo" foo (
     *   a MAX_VERSIONS=1 TTL=2592000,
     *   c,
     *   d MAX_VERSIONS=5,
     *   ACCESS GROUP primary IN_MEMORY BLOCKSIZE=1024 (a),
     *   ACCESS GROUP secondary COMPRESSOR="zlib" BLOOMFILTER="none" (b, c),
     *   ACCESS GROUP tertiary BLOCKSIZE=2048 (d)
     * );
     * </pre>
     *
     * @param table_name Name of the table to alter
     * @param schema_str desired alterations represented as schema
     * @param force Force table alteration even if generation mismatch
     */
#endif
    void alter_table(const std::string &table_name, const std::string &schema_str, bool force);

    /**
     * Opens a table
     *
     * @param name name of the table
     * @param flags open flags
     * @return pointer to Table object
     */
    TablePtr open_table(const std::string &name, int32_t flags = 0);

    /**
     * Refreshes the cached table entry
     *
     * @param name name of the table
     */
    void refresh_table(const std::string &name);

    /**
     * Checks if the table exists
     *
     * @param name name of table
     * @return true of table exists false ow
     */
    bool exists_table(const std::string &name);

    /**
     * Returns the table identifier for a table
     *
     * @param name name of table
     * @return identifier string for the table
     */
    std::string get_table_id(const std::string &name);

    /**
     * Returns a smart ptr to a schema object for a table
     *
     * @param name table name
     * @return schema object of table
     */
    SchemaPtr get_schema(const std::string &name);

    /**
     * Returns the schema for a table
     *
     * @param name table name
     * @param with_ids include generation and column family ID attributes
     * @return XML schema of table
     */
    std::string get_schema_str(const std::string &name, bool with_ids=false);

    /**
     * Returns a list of existing tables &  namesspaces
     *
     * @param include_sub_entries include or not include all sub entries
     * @param listing reference to vector of table names
     */
    void get_listing(bool include_sub_entries, std::vector<NamespaceListing> &listing);

    /**
     * Renames a table.
     *
     * @param old_name old table name
     * @param new_name new table name
     */
    void rename_table(const std::string &old_name, const std::string &new_name);

    /**
     * Removes a table.  This command instructs the Master to
     * remove a table from the system, including all of its
     * ranges.
     *
     * @param name table name
     * @param if_exists don't throw an exception if table does not exist
     */
    void drop_table(const std::string &name, bool if_exists);

    /// Rebuild a table's indices.
    /// Rebuilds the indices for table <code>table_name</code> by carrying out a
    /// <i>recreate index tables</i> master operation an then re-populating the
    /// index tables by scanning the primary table with the
    /// <i>rebuild_indices</i> member of ScanSpec set.
    /// @param table_name Name of table for which indices are to be rebuilt
    /// @param table_parts Controls which indices to rebuild
    void rebuild_indices(const std::string &table_name, TableParts table_parts);

    /**
     * Returns a list of existing table names
     *
     * @param name table name
     * @param splits reference to TableSplitsContainer object
     */
    void get_table_splits(const std::string &name, TableSplitsContainer &splits);

    /**
     * Returns a pointer to the client object which created this Namespace
     */
    Client *get_client() {
      return m_client;
    }

  private:
    std::string get_index_table_name(const std::string &table_name) {
      std::string s="^";
      return (s+table_name);
    }

    std::string get_qualifier_index_table_name(const std::string &table_name) {
      std::string s="^^";
      return (s+table_name);
    }

    void create_index_table(const std::string &primary_table_name);

    typedef boost::tokenizer<boost::char_separator<char> > Tokenizer;
    std::string get_full_name(const std::string &sub_name);

    void initialize();
    TablePtr _open_table(const std::string &full_name, int32_t flags = 0);

    std::string                  m_name;
    std::string                  m_id;
    PropertiesPtr           m_props;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    Hyperspace::SessionPtr  m_hyperspace;
    ApplicationQueueInterfacePtr     m_app_queue;
    NameIdMapperPtr         m_namemap;
    Lib::Master::ClientPtr m_master_client;
    RangeLocatorPtr         m_range_locator;
    std::string                  m_toplevel_dir;
    bool                    m_hyperspace_reconnect;
    TableCachePtr           m_table_cache;
    uint32_t                m_timeout_ms;
    Client                 *m_client;
  };

  /// Shared smart pointer to Namespace
  typedef std::shared_ptr<Namespace> NamespacePtr;

  /// @}

}

#endif // Hypertable_Lib_Namespace_h
