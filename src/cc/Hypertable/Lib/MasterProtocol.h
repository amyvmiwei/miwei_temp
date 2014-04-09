/*
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

/** @file
 * Declarations for MasterProtocol.
 * This file contains declarations for MasterProtocol, a class for generating
 * Master protocol messages.
 */

#ifndef MASTER_PROTOCOL_H
#define MASTER_PROTOCOL_H

#include <map>
#include "Common/StatsSystem.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/Protocol.h"

#include "BalancePlan.h"
#include "SystemVariable.h"
#include "TableParts.h"
#include "Types.h"


namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /** Generates Master protocol request messages.
   */
  class MasterProtocol : public Protocol {

  public:

    static const uint64_t COMMAND_CREATE_TABLE                = 0;
    static const uint64_t COMMAND_GET_SCHEMA                  = 1;
    static const uint64_t COMMAND_STATUS                      = 2;
    static const uint64_t COMMAND_REGISTER_SERVER             = 3;
    static const uint64_t COMMAND_MOVE_RANGE                  = 4;
    static const uint64_t COMMAND_DROP_TABLE                  = 5;
    static const uint64_t COMMAND_ALTER_TABLE                 = 6;
    static const uint64_t COMMAND_SHUTDOWN                    = 7;
    static const uint64_t COMMAND_CLOSE                       = 8;
    static const uint64_t COMMAND_CREATE_NAMESPACE            = 9;
    static const uint64_t COMMAND_DROP_NAMESPACE              = 10;
    static const uint64_t COMMAND_RENAME_TABLE                = 11;
    static const uint64_t COMMAND_RELINQUISH_ACKNOWLEDGE      = 12;
    static const uint64_t COMMAND_FETCH_RESULT                = 13;
    static const uint64_t COMMAND_BALANCE                     = 14;
    static const uint64_t COMMAND_REPLAY_COMPLETE             = 15;
    static const uint64_t COMMAND_PHANTOM_PREPARE_COMPLETE    = 16;
    static const uint64_t COMMAND_PHANTOM_COMMIT_COMPLETE     = 17;
    static const uint64_t COMMAND_STOP                        = 18;
    static const uint64_t COMMAND_REPLAY_STATUS               = 19;
    static const uint64_t COMMAND_COMPACT                     = 20;
    static const uint64_t COMMAND_SET                         = 21;
    static const uint64_t COMMAND_RECREATE_INDEX_TABLES       = 22;
    static const uint64_t COMMAND_MAX                         = 23;

    static const char *m_command_strings[];

    static CommBuf *
    create_create_namespace_request(const String &name, int flags);
    static CommBuf *
    create_drop_namespace_request(const String &name, bool if_exists);
    static CommBuf *
    create_create_table_request(const String &tablename, const String &schemastr);
    static CommBuf *
    create_alter_table_request(const String &tablename, const String &schemastr);
    static CommBuf *
    create_compact_request(const String &tablename, const String &row,
                           uint32_t range_types);

    /** Creates protocol message for SetState operation.
     * @param specs Vector of variable specifications
     * @return Pointer to protocol message
     */
    static CommBuf *
    create_set_request(const std::vector<SystemVariable::Spec> &specs);

    static CommBuf *create_get_schema_request(const String &tablename);

    static CommBuf *create_status_request();

    static CommBuf *create_register_server_request(const String &location,
                                                   uint16_t listen_port,
                                                   bool lock_held,
                                                   StatsSystem &system_stats);

    static CommBuf *
    create_move_range_request(const String &source, const TableIdentifier *,
                              const RangeSpec &, const String &transfer_log_dir,
                              uint64_t soft_limit, bool split);
    static CommBuf *
    create_relinquish_acknowledge_request(const String &, const TableIdentifier *,
					  const RangeSpec &);
    static CommBuf *create_rename_table_request(const String &old_name, const String &new_name);
    static CommBuf *create_drop_table_request(const String &table_name,
                                              bool if_exists);

    static CommBuf *create_fetch_result_request(int64_t id);

    static CommBuf *create_shutdown_request();

    static CommBuf *create_balance_request(BalancePlan &plan);

    static CommBuf *create_stop_request(const String &rsname, bool recover);

    static CommBuf *create_replay_status_request(int64_t op_id,
                                    const String &location, int plan_generation);

    static CommBuf *create_replay_complete_request(int64_t op_id,
                                    const String &location, int plan_generation,
                                    int32_t error, const String &message);

    static CommBuf *create_phantom_prepare_complete_request(int64_t op_id,
                                    const String &location, int plan_generation,
                                    int32_t error, const String &message);

    static CommBuf *create_phantom_commit_complete_request(int64_t op_id,
                                    const String &location, int plan_generation,
                                    int32_t error, const String &message);

    /// Creates <i>recreate index tables</i> %Master operation request message.
    /// Formats a <i>recreate index tables</i> %Master operation request message
    /// as follows:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>Name of table for which to recreate index tables</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>Serialized object specifying which index tables to recreate
    ///   </td>
    ///   </tr>
    /// </table>
    /// @param table_name Name of table for which to recreate index tables
    /// @param table_parts Specifies which index tables to recreate
    /// @return %Master operation request message
    static CommBuf *create_recreate_index_tables_request(const std::string &table_name,
                                                         TableParts table_parts);

    virtual const char *command_text(uint64_t command);

  };
  /// @}
}

#endif // MASTER_PROTOCOL_H
