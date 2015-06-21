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
 * Declarations for MetaLog.
 * This file contains declarations that are part of the MetaLog system.
 */

#ifndef Hypertable_Lib_MetaLog_h
#define Hypertable_Lib_MetaLog_h

#include <Common/Filesystem.h>

#include <deque>
#include <iostream>

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /** %MetaLog framework.
   * Contains a set of classes that define the MetaLog subsystem.
   */
  namespace MetaLog {

    /** %Metalog file header.
     * This class is used to facilitate encoding and decoding the header of a
     * %MetaLog file.  The %MetaLog is a server state log that is implemented as
     * a directory containing a set of log files.  Each log file holds the
     * complete state and state transitions for the execution of a server.  When
     * the server starts, the last state log file is read in its entirety and a
     * new file is created to hold the last known server state plus all
     * subsequent state transitions the occur during the execution of the
     * server. Each state log file begins with a header that is encoded and
     * decoded by this class.
     */
    class Header {

    public:
      
      /** Encodes %MetaLog file header.
       * The header is encoded as follows:
       * <table>
       * <tr><th>Encoding</th><th>Description</th></tr>
       * <tr><td>i16</td><td>%MetaLog definition version</td></tr>
       * <tr><td>char14</td><td>%MetaLog definition name (e.g. "mml" or "rsml")</td></tr>
       * </table>
       */
      void encode(uint8_t **bufp) const;

      /** Decodes serialized header.
       * @param bufp Address of destination buffer pointer (advanced by call).
       * @param remainp Address of integer holding amount of remaining buffer
       * (decremented by call).
       */
      void decode(const uint8_t **bufp, size_t *remainp);

      /** Enumeration containing static header length
       */
      enum {
        LENGTH = 16 ///< Static header length
      };

      /// %MetaLog definition version number
      uint16_t version;

      /// %MetaLog definition name (e.g. "mml" or "rsml")
      char name[14];
    };

    /** Scans %MetaLog directory for numeric file names.
     * A %MetaLog consists of a directory containing a set of numerically named
     * files, each file containing the state and state transitions of a
     * start-to-finish run of a server.  This function returns the set of
     * numeric file names found within the %MetaLog directory as well as the
     * file number that can be used to hold the state of the next run of the
     * server.
     * @param fs Smart pointer to Filesystem object
     * @param path Pathname of %MetaLog directory
     * @param file_ids Deque to hold discovered numeric file names
     */
    void scan_log_directory(FilesystemPtr &fs, const std::string &path,
                            std::deque<int32_t> &file_ids);

   }

   /** @}*/
}

#endif // Hypertable_Lib_MetaLog_h
