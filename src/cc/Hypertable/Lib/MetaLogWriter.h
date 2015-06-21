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
 * Declarations for MetaLog::Writer.
 * This file contains declarations for MetaLog::Writer, a class for writing
 * a %MetaLog.
 */

#ifndef Hypertable_Lib_MetaLogWriter_h
#define Hypertable_Lib_MetaLogWriter_h

#include "MetaLogDefinition.h"
#include "MetaLogEntity.h"

#include <Common/Filesystem.h>

#include <AsyncComm/Comm.h>
#include <AsyncComm/DispatchHandler.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace Hypertable {

  namespace MetaLog {

    /// @addtogroup libHypertable
    /// @{

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
    class Writer {
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
       * then sets #m_backup_filename to #m_backup_path + <code>\/next_id</code>
       * and opens it for writing (in the local filesystem), setting
       * #m_backup_fd to the opened file descriptor.  It then writes the file
       * header with a call to write_header() and persists
       * <code>initial_entities</code>.  Finally, it writes a RecoverEntity.
       * @param fs Smart pointer to Filesystem object
       * @param definition Smart pointer to Definition object
       * @param path %Path to %MetaLog directory
       * @param initial_entities Initial set of entities to write into log
       */
      Writer(FilesystemPtr &fs, DefinitionPtr &definition, const std::string &path,
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
      void record_state(EntityPtr entity);

      /** Persists a vector of entities to the log.
       * This method is logically equivalent to calling
       * @ref single_entity_record_state for each entity in
       * <code>entities</code>.
       * @param entities Reference to vector of entities to persist
       */
      void record_state(std::vector<EntityPtr> &entities);

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
      void record_removal(EntityPtr entity);

      /** Records the removal of a vector of entities.
       * This method is logically equivalent to calling
       * @ref single_entity_record_removal for each entity in
       * <code>entities</code>.
       * @param entities Reference to vector of entities to remove
       */
      void record_removal(std::vector<EntityPtr> &entities);

      void signal_write_ready();

      /// Global flag to force writer to skip writing EntityRecover (testing)
      static bool skip_recover_entry;

    private:

      /// Periodically flushes deferred writes to disk
      class WriteScheduler : public DispatchHandler {
      public:

        /// Constructor.
        WriteScheduler(Writer *writer);

        virtual ~WriteScheduler();

        void schedule();

        void handle(EventPtr &event) override;

      private:
        /// %Mutex for serializing access to members
        std::mutex m_mutex;
        /// Condition variable to signal when timer has stopped
        std::condition_variable m_cond;
        /// Pointer to MetaLogWriter
        Writer *m_writer {};
        /// Pointer to Comm layer
        Comm *m_comm {};
        /// Timer interval
        int32_t m_interval {};
        /// Flag indicating that write has been scheduled
        bool m_scheduled {};
      };

      typedef std::shared_ptr<WriteScheduler> WriteSchedulerPtr;

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
       * names until the number of remaining files is equal to #m_history_size.
       * The files are removed from the FS as well as the backup directory.  The
       * #m_file_ids member is assumed to be populated on entry with the file
       * name IDs in the log directory and is adjusted to only include the file
       * name IDs that remain after purging.
       */
      void purge_old_log_files();

      void roll();

      void service_write_queue();

      /// %Mutex for serializing access to members
      std::mutex m_mutex;

      /// Condition variable to signal completion of deferred writes
      condition_variable m_cond;
      
      /// Smart pointer to Filesystem object
      FilesystemPtr m_fs;

      /// Smart pointer to %MetaLog Definition
      DefinitionPtr m_definition;

      /// Path name of %MetaLog directory
      std::string  m_path;

      /// Full pathname of %MetaLog file open for writing
      std::string  m_filename;

      /// File descriptor of %MetaLog file in FS
      int m_fd {-1};

      /// Pathname of local log backup directory
      std::string  m_backup_path;

      /// Pathname of local log backup file
      std::string  m_backup_filename;

      /// File descriptor of backup %MetaLog file in local filesystem
      int m_backup_fd;

      /// Current write offset of %MetaLog file
      int32_t m_offset {};

      /// Deque of existing file name IDs
      std::deque<int32_t> m_file_ids;

      /// Maximum file size
      int64_t m_max_file_size {};

      /// Number of old MetaLog files to retain for historical purposes
      size_t m_history_size {};

      /// Replication factor
      int32_t m_replication {};

      /// Log flush method (FLUSH or SYNC)
      Filesystem::Flags m_flush_method {};

      // Serialized entity (length and smart pointer to buffer)
      typedef std::pair<size_t, std::shared_ptr<uint8_t>> SerializedEntityT;

      /// Map of current serialized entity data
      std::map<int64_t, SerializedEntityT> m_entity_map;

      /// Flag indicating that 
      bool m_write_ready {};

      /// Vector of pending writes
      std::vector<StaticBufferPtr> m_write_queue;

      /// Write scheduler
      WriteSchedulerPtr m_write_scheduler;

    };

    /// Smart pointer to Writer
    typedef std::shared_ptr<Writer> WriterPtr;
    
    /// @}
  }

}

#endif // Hypertable_Lib_MetaLogWriter_h
