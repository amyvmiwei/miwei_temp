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

/** @file
 * Declarations for MetaLog::Definition.
 * This file contains declarations for MetaLog::Definition, an abstract
 * base class from which are derived concrete classes that define the set of
 * valid %MetaLog entities for a server.
 */

#ifndef Hypertable_Lib_MetaLogDefinition_h
#define Hypertable_Lib_MetaLogDefinition_h

#include "MetaLogEntity.h"

#include <Common/String.h>

#include <memory>

namespace Hypertable {

  namespace MetaLog {

    /** @addtogroup libHypertable
     * @{
     */

    /** Defines the set of valid %MetaLog entities for a server.
     * The %MetaLog framework can be used to persist the state transitions for
     * servers in %Hypertable.  From this abstract base class are derived
     * concrete classes that define the set of valid %MetaLog entities for a
     * particular server.  It consists of a name (e.g. "mml" or "rsml") and a
     * create() method that is used by the %MetaLog framework for constructing
     * entities from serialized records in a %MetaLog file.
     */
    class Definition {
    public:

      /** Constructor.
       * @param backup_label Backup label of %MetaLog
       * @see backup_label()
       */
      Definition(const char* backup_label) : m_backup_label(backup_label) { }

      /** Returns version number of definition.
       * @return Version number of definition
       */
      virtual uint16_t version() = 0;

      /** Returns %MetaLog definition name.
       * This method returns the name of the %MetaLog definition (e.g. "mml" or
       * "rsml"). This name is used as the last path component of the directory
       * name of the %MetaLog.  For example, the RSML for server "rs1" is stored
       * in a directory path that looks something like
       * <code>/hypertable/servers/rs1/log/rsml</code>.
       * @return %MetaLog definition name
       */
      virtual const char *name() = 0;

      /** Returns backup label of %MetaLog.
       * The %MetaLog framework stores a backup copy of the %MetaLog files in
       * the local filesystem for the purposes of disaster recovery.  To allow
       * tests to run multiple instances of a server on the same machine without
       * their local backups clobbering one another, the definition is given
       * a backup label which is used to construct the pathname of the backup
       * directory within the local filesystem.  For example, the RangeServer
       * uses the proxy name of the server as the backup label, so the local
       * backup directories for servers "rs1" and "rs2" are formulated as:
       * <pre>
       * /opt/hypertable/current/run/log_backup/rsml/rs1
       * /opt/hypertable/current/run/log_backup/rsml/rs2
       * </pre>
       * @return Backup label of %MetaLog
       */
      virtual const char *backup_label() { return m_backup_label.c_str(); }
      
      /** Constructs a %MetaLog entity from an entity header.
       * When the %MetaLog framework reads serialized entities from a %MetaLog
       * file, it first reads the entity header and passes that header into
       * this method which will construct an empty entity.  It then reconstructs
       * the entity state by passing a pointer to the serialized entity to the
       * Entity::decode() method.
       * @param header %Entity header
       * @return Pointer to newly constructed entity, or 0 if entity described
       * by header is no longer used
       */
      virtual EntityPtr create(const EntityHeader &header) = 0;

    private:

      /// Backup label of %MetaLog
      std::string m_backup_label;
    };

    /// Smart pointer to Definition
    typedef std::shared_ptr<Definition> DefinitionPtr;

    /** @}*/
  }

}

#endif // Hypertable_Lib_MetaLogDefinition_h
