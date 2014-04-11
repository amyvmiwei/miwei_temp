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

/** @file
 * Declarations for MetaLog::Writer.
 * This file contains declarations for MetaLog::Writer, a class for writing
 * a %MetaLog.
 */

#ifndef HYPERTABLE_METALOGWRITER_H
#define HYPERTABLE_METALOGWRITER_H

#include "Common/Filesystem.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include <vector>

#include "MetaLogDefinition.h"
#include "MetaLogEntity.h"

namespace Hypertable {

  namespace MetaLog {

    /** @addtogroup libHypertable
     * @{
     */

    /** Writes a %MetaLog.
     * This class is used to persist application entities to a %MetaLog.  It
     * is constructed with an initial set of entities to persist (obtained from
     * a prior read of the log) and is kept open during the duration of the
     * server process and is used to persist state changes by appending updated
     * Entity objects.  It is typically constructed as follows:
     * <pre>
     * MetaLog::Reader reader = new MetaLog::Reader(log_fs, definition, log_dir);
     * vector<MetaLog::EntityPtr> entities;
     * reader->get_entities(entities);
     * MetaLog::Writer writer = new MetaLog::Writer(log_fs, definition, log_dir, entities);
     * </pre>
     */
    class Writer : public ReferenceCount {
    public:

      /** Constructor.
       * Initializes the writer by setting #m_path to <code>path</code> and
       * creating it if it doesn't exist.  It then initializes #m_backup_path
       * (creating it in the local filesystem if it does not exist) as follows:
       * <pre>
       * $data_dir     = Config::properties->get_str("Hypertable.DataDirectory");
       * $name         = m_definition->name();
       * $backup_label = m_definition->backup_label()
       * m_backup_path = $data_dir/run/log_backup/$name/$backup_label
       * </pre>
       * It then scans the log directory with a call to scan_log_directory() to
       * obtain the current set of numeric file names in the log directory
       * (<code>path</code>) and the next unused numeric file name which is
       * stored in local variable, <i>next_id</i>.  Old log files are purged
       * with a call to purge_old_log_files() with a "keep count" of 30.
       * It then sets #m_filename equal
       * to  <code>path/next_id</code> and opens it for writing, setting
       * the #m_fd to the opened file descriptor.  The replication factor is
       * set to the <code>Hypertable.Metadata.Replication</code> property.  It
       * then sets #m_backup_filename to #m_backup_path<code>/next_id</code> and
       * opens it for writing (in the local filesystem), setting #m_backup_fd to
       * the opened file descriptor.  It then writes the file header with a call
       * to write_header() and persists <code>initial_entities</code>.  Finally,
       * it writes a RecoverEntity.
       * @param fs Smart pointer to Filesystem object
       * @param definition Smart pointer to Definition object
       * @param path %Path to %MetaLog directory
       * @param initial_entites Initial set of entities to write into log
       */
      Writer(FilesystemPtr &fs, DefinitionPtr &definition, const String &path,
             std::vector<EntityPtr> &initial_entities);

      /** Destructor.
       * Calls close().
       */
      ~Writer();

      /** Closes open file descriptors.
       * This method closes both #m_fd and #m_backup_fd and sets them to -1.
       */
      void close();

      /** Persists an Entity to the log.
       * @anchor single_entity_record_state
       * If the Entity::marked_for_removal() method of <code>entity</code>
       * returns <i>true</i>, just the entity header is persisted, otherwise
       * the entity header and state are persisted with a call to the
       * Entity::encode_entry() method.  First the entity is appended
       * to the local backup file and then to the log file in the FS.
       * #m_offset is incremented by the length of the serialized entity.
       * @param entity Pointer to entity to persist
       */
      void record_state(Entity *entity);

      /** Persists a vector of entities to the log.
       * This method is logically equivalent to calling
       * @ref single_entity_record_state for each entity in
       * <code>entities</code>.
       * @param entities Reference to vector of entities to persist
       */
      void record_state(std::vector<Entity *> &entities);

      /** Records the removal of an entity.
       * @anchor single_entity_record_removal
       * This method records the removal of <code>entity</code> by
       * setting the EntityHeader::FLAG_REMOVE bit in the <i>flags</i> field
       * of the header, setting the <i>length</i> and <i>checksum</i>
       * header fields to 0, and then writing the entity header. First the
       * entity header is appended to the local backup file and then to the log
       * file in the FS.
       * #m_offset is incremented by the length of the serialized entity.
       * @param entity Pointer to entity to remove
       */
      void record_removal(Entity *entity);

      /** Records the removal of a vector of entities.
       * This method is logically equivalent to calling
       * @ref single_entity_record_removal for each entity in
       * <code>entities</code>.
       * @param entities Reference to vector of entities to remove
       */
      void record_removal(std::vector<Entity *> &entities);

      /// Global flag to force writer to skip writing EntityRecover (testing)
      static bool skip_recover_entry;

    private:

      /** Writes %MetaLog file header.
       * This method writes a %MetaLog file header to the open %MetaLog file
       * by initializing a MetaLog::Header object with the name and version
       * obtained by calling the methods Definition::name() and
       * Definition::version() of #m_definition.  First the file header is
       * appended to the local backup file and then to the log file in the FS.
       * #m_offset is incremented by the length of the serialized file header
       * (Header::LENGTH).
       */
      void write_header();

      /** Purges old %MetaLog files.
       * This method removes the %MetaLog files with the numerically smallest
       * names until the number of remaining files is equal to
       * <code>keep_count</code>.  The files are removed from the FS as well
       * as the backup directory.  The <code>file_ids</code> parameter is
       * adjusted to only include the numeric file names that remain after
       * purging.
       * @param file_ids Numeric file names in the %MetaLog directory
       * @param keep_count Number of %MetaLog files to keep
       */
      void purge_old_log_files(std::vector<int32_t> &file_ids, size_t keep_count);

      /// %Mutex for serializing access to members
      Mutex m_mutex;
      
      /// Smart pointer to Filesystem object
      FilesystemPtr m_fs;

      /// Smart pointer to %MetaLog Definition
      DefinitionPtr m_definition;

      /// Path name of %MetaLog directory
      String  m_path;

      /// Full pathname of %MetaLog file open for writing
      String  m_filename;

      /// File descriptor of %MetaLog file in FS
      int m_fd;

      /// Pathname of local log backup directory
      String  m_backup_path;

      /// Pathname of local log backup file
      String  m_backup_filename;

      /// File descriptor of backup %MetaLog file in local filesystem
      int m_backup_fd;

      /// Current write offset of %MetaLog file
      int m_offset;
    };

    /// Smart pointer to Writer
    typedef intrusive_ptr<Writer> WriterPtr;
    
    /** @}*/
  }

}

#endif // HYPERTABLE_METALOGWRITER_H
