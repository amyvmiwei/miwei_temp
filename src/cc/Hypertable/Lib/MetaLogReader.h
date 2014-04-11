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
 * Declarations for MetaLog::Reader.
 * This file contains declarations for MetaLog::Reader, a class for reading
 * a %MetaLog.
 */

#ifndef HYPERTABLE_METALOGREADER_H
#define HYPERTABLE_METALOGREADER_H

#include "Common/Filesystem.h"
#include "Common/ReferenceCount.h"

#include <map>
#include <vector>

#include "MetaLogDefinition.h"

namespace Hypertable {

  namespace MetaLog {

  /** @addtogroup libHypertable
   * @{
   */

  /** Reads a %MetaLog.  This class is part of a group of generic meta log
   * manipulation classes.  A meta log is a server state log and is currently
   * used by both the RangeServer and the Master servers.  The set of valid
   * MetaLog::Entity classes are defined by a MetaLog::Definition class that
   * is defined for each server.  This class reads a meta log and provides access
   * to the latest versions of live MetaLog entities that have been persisted in
   * the log.
   */
    class Reader : public ReferenceCount {

    public:

      /** Constructor.
       * Constructs and empty object.  This constructor is used when opening a
       * specific %MetaLog fragment file and is typically followed by a call to
       * load_file().
       * @param fs Smart pointer to Filesystem object
       * @param definition Smart pointer to Definition object
       */
      Reader(FilesystemPtr &fs, DefinitionPtr &definition);

      /** Constructor.
       * @anchor primary_metalog_reader_constructor
       * This constructor initializes the Reader and loads the %MetaLog specified
       * by <code>path</code>.  It initializes #m_backup_path as follows:
       * <pre>
       * $data_dir     = Config::properties->get_str("Hypertable.DataDirectory");
       * $name         = m_definition->name();
       * $backup_label = m_definition->backup_label()
       * m_backup_path = $data_dir/run/log_backup/$name/$backup_label
       * </pre>
       * It then calls reload() to load the %MetaLog.
       * @param fs Smart pointer to Filesystem object
       * @param definition Smart pointer to Definition object
       * @param path %Path to %MetaLog directory
       */
      Reader(FilesystemPtr &fs, DefinitionPtr &definition, const String &path);

      /** Returns latest version of all entities.
       * @param entities Reference to vector to hold returned entities
       */
      void get_entities(std::vector<EntityPtr> &entities);

      /** Returns all versions of all entities.
       * @param entities Reference to vector to hold returned entities
       */
      void get_all_entities(std::vector<EntityPtr> &entities);

      /** Loads %MetaLog.
       * This method scans the %MetaLog directory with a call to
       * scan_log_directory() and then loads the largest numerically named file
       * in the directory with a call to load_file().  The #m_file_nums
       * vector is populated with the numeric file names found in log directory
       * and the #m_next_filenum is set to the next largest unused numeric
       * file name.  This method propagates all
       * exceptions of type Error::METALOG_ERROR and converts all other
       * exceptions to Error::METALOG_READ_ERROR and rethrows.
       * @throws Exception with one of the Error::METALOG_ERROR codes.
       */
      void reload();

      /** Returns next unused numeric file name.
       * This method returns #m_next_filenum which is set during the log
       * directory scan by taking the largest numerically named file and adding
       * one.
       * @return Next unused numeric file name.
       */
      int32_t next_file_number() { return m_next_filenum; }

      /** Loads %MetaLog file.
       * This method opens <code>fname</code> and reads the header with a call
       * to read_header().  It then reads each Entity from the file by first
       * reading and decoding the EntityHeader.  If the <i>flags</i> field of
       * the header has the EntityHeader::FLAG_REMOVE bit set, it is purged
       * from #m_entity_map and skipped.
       * Otherwise, the entity is constructed with a call to
       * the Definition::create() of #m_definition, afterwhich the entity's
       * state is decoded with a call to the entity's Entity::decode() method
       * and it is inserted into both #m_entity_map and #m_entities.
       * If a read comes up short, this method will throw an Exception with
       * error code Error::METALOG_ENTRY_TRUNCATED.  If there is a checksum
       * mis-match, it will throw an Exception with error code
       * Error::METALOG_CHECKSUM_MISMATCH.  If after the complete file is
       * read, if an EntityRecover was not encountered, an Exception is
       * thrown with error code Error::METALOG_MISSING_RECOVER_ENTITY.
       * @param fname Full pathname of %MetaLog file to load
       * @throws %Exception with code Error::METALOG_ENTRY_TRUNCATED, or
       * Error::METALOG_CHECKSUM_MISMATCH, or
       * Error::METALOG_MISSING_RECOVER_ENTITY, or one of the exceptions
       * returned by Client::open_buffered(), or Client::read().
       */
      void load_file(const String &fname);

      /** Returns %MetaLog Definition version read from log file header.
       * @return %MetaLog Definition version read from log file header.
       */
      uint16_t version() { return m_version; }

    private:

      /** Sanity check local backup file.
       * This method throws an Exception with error code
       * Error::METALOG_BACKUP_FILE_MISMATCH if the backup file with
       * numeric name <code>file_num</code> exists and has size less
       * than the corresponding file in the FS.  Otherwise it returns
       * successfuly.
       * @param file_num Numeric file name of file for which to verify
       * backup
       */
      void verify_backup(int32_t file_num);

      /** Reads the log file header.
       * Reads and decodes the log file header (MetaLog::Header).  If the
       * read comes up short or the name in the header does not match what is
       * returned by the Definition::name() method of #m_definition, an
       * Exception is thrown with error code Error::METALOG_BAD_HEADER. If
       * the version read from the header is greater than what is returned by
       * Definition::version() method of #m_definition, an exception is thrown
       * with error code Error::METALOG_VERSION_MISMATCH.  Otherwise, #m_version
       * is set to the version read from the header, and <code>*offsetp</code>
       * is incremented by the length of the header.
       * @param fd Open file descriptor for %MetaLog file
       * @param offsetp Pointer to current offset within the file
       * @throws Exception with code Error::METALOG_BAD_HEADER or
       * Error::METALOG_VERSION_MISMATCH, or one of the exceptions thrown by
       * Client::read()
       */
      void read_header(int fd, int64_t *offsetp);

      /// Smart pointer to Filesystem object
      FilesystemPtr m_fs;

      /// Smart pointer to %MetaLog Definition
      MetaLog::DefinitionPtr m_definition;

      /// Path name of %MetaLog directory
      String m_path;

      /// Next unused numeric filename
      int32_t m_next_filenum;

      /// Vector of numeric file names found in log directory
      std::vector<int32_t> m_file_nums;

      /// Map containing latest version of each Entity read from %MetaLog
      std::map<EntityHeader, EntityPtr> m_entity_map;

      /// Vector containing all versions of each Entity read from %MetaLog
      std::vector<EntityPtr> m_entities;

      /// Local backup path initialized in @ref primary_metalog_reader_constructor
      String m_backup_path;

      /// %MetaLog Definition version read from log file header.
      uint16_t m_version;
    };

    /// Smart pointer to Reader
    typedef intrusive_ptr<Reader> ReaderPtr;

    /** @}*/
  }
}

#endif // HYPERTABLE_METALOGREADER_H
