/*
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

#include <Common/Compat.h>

#include "MetaLogDefinitionMaster.h"

#include "OperationAlterTable.h"
#include "OperationBalance.h"
#include "OperationCompact.h"
#include "OperationCreateNamespace.h"
#include "OperationCreateTable.h"
#include "OperationDropTable.h"
#include "OperationDropNamespace.h"
#include "OperationInitialize.h"
#include "OperationMoveRange.h"
#include "OperationRenameTable.h"
#include "OperationRecover.h"
#include "OperationRecoverRanges.h"
#include "OperationRecoveryBlocker.h"
#include "OperationRecreateIndexTables.h"
#include "OperationSetState.h"
#include "OperationToggleTableMaintenance.h"
#include "RangeServerConnection.h"
#include "BalancePlanAuthority.h"
#include "RecoveredServers.h"
#include "SystemState.h"

#include <memory>

using namespace Hypertable;
using namespace Hypertable::MetaLog;
using namespace std;

uint16_t DefinitionMaster::version() {
  return 4;
}

const char *DefinitionMaster::name() {
  return "mml";
}

EntityPtr DefinitionMaster::create(const EntityHeader &header) {
  OperationPtr operation;

  if (header.type == EntityType::RANGE_SERVER_CONNECTION)
    return make_shared<RangeServerConnection>(header);

  if ((header.type & 0xF0000L) == 0x20000L) {

    /*
     * If old operation, then record the original type and convert
     */

    if (header.type == EntityType::OLD_OPERATION_INITIALIZE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_INITIALIZE;
      operation = make_shared<OperationInitialize>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_INITIALIZE);
    }
    else if (header.type == EntityType::OLD_OPERATION_ALTER_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_ALTER_TABLE;
      operation = make_shared<OperationAlterTable>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_ALTER_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_CREATE_NAMESPACE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_CREATE_NAMESPACE;
      operation = make_shared<OperationCreateNamespace>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_CREATE_NAMESPACE);
    }
    else if (header.type == EntityType::OLD_OPERATION_DROP_NAMESPACE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_DROP_NAMESPACE;
      operation = make_shared<OperationDropNamespace>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_DROP_NAMESPACE);
    }
    else if (header.type == EntityType::OLD_OPERATION_CREATE_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_CREATE_TABLE;
      operation = make_shared<OperationCreateTable>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_CREATE_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_DROP_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_DROP_TABLE;
      operation = make_shared<OperationDropTable>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_DROP_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_RENAME_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_RENAME_TABLE;
      operation = make_shared<OperationRenameTable>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_RENAME_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_MOVE_RANGE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_MOVE_RANGE;
      operation = make_shared<OperationMoveRange>(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_MOVE_RANGE);
    }
    else if (header.type == EntityType::OLD_OPERATION_BALANCE) {
      return 0;
    }
  }
  else if (header.type == EntityType::BALANCE_PLAN_AUTHORITY) {
    MetaLog::WriterPtr mml_writer = m_context ? m_context->mml_writer : 0;
    return make_shared<BalancePlanAuthority>(m_context, mml_writer, header);
  }
  else {
    if (header.type == EntityType::OPERATION_INITIALIZE)
      operation = make_shared<OperationInitialize>(m_context, header);
    else if (header.type == EntityType::OPERATION_ALTER_TABLE)
      operation = make_shared<OperationAlterTable>(m_context, header);
    else if (header.type == EntityType::OPERATION_CREATE_NAMESPACE)
      operation = make_shared<OperationCreateNamespace>(m_context, header);
    else if (header.type == EntityType::OPERATION_DROP_NAMESPACE)
      operation = make_shared<OperationDropNamespace>(m_context, header);
    else if (header.type == EntityType::OPERATION_CREATE_TABLE)
      operation = make_shared<OperationCreateTable>(m_context, header);
    else if (header.type == EntityType::OPERATION_DROP_TABLE)
      operation = make_shared<OperationDropTable>(m_context, header);
    else if (header.type == EntityType::OPERATION_RENAME_TABLE)
      operation = make_shared<OperationRenameTable>(m_context, header);
    else if (header.type == EntityType::OPERATION_MOVE_RANGE)
      operation = make_shared<OperationMoveRange>(m_context, header);
    else if (header.type == EntityType::OPERATION_BALANCE_RETIRED)
      return 0;
    else if (header.type == EntityType::OPERATION_RECOVER_SERVER)
      operation = make_shared<OperationRecover>(m_context, header);
    else if (header.type == EntityType::OPERATION_RECOVER_SERVER_RANGES)
      operation = make_shared<OperationRecoverRanges>(m_context, header);
    else if (header.type == EntityType::OPERATION_BALANCE)
      operation = make_shared<OperationBalance>(m_context, header);
    else if (header.type == EntityType::OPERATION_COMPACT)
      operation = make_shared<OperationCompact>(m_context, header);
    else if (header.type == EntityType::OPERATION_SET)
      operation = make_shared<OperationSetState>(m_context, header);
    else if (header.type == EntityType::OPERATION_TOGGLE_TABLE_MAINTENANCE)
      operation = make_shared<OperationToggleTableMaintenance>(m_context, header);
    else if (header.type == EntityType::OPERATION_RECREATE_INDEX_TABLES)
      operation = make_shared<OperationRecreateIndexTables>(m_context, header);
    else if (header.type == EntityType::SYSTEM_STATE)
      return make_shared<SystemState>(header);
    else if (header.type == EntityType::RECOVERED_SERVERS)
      return make_shared<RecoveredServers>(header);
  }

  if (operation)
    return operation;

  HT_THROWF(Error::METALOG_ENTRY_BAD_TYPE,
            "Unrecognized type (%d) encountered in mml",
            (int)header.type);
}

