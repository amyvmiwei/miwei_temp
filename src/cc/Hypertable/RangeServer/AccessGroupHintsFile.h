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
 * Declarations for AccessGroupHintsFile.
 * This file contains the type declarations for AccessGroupHintsFile, a class
 * used to read and write the hints file used for access group state
 * boostrapping.
 */

#ifndef HYPERTABLE_ACCESSGROUPHINTSFILE_H
#define HYPERTABLE_ACCESSGROUPHINTSFILE_H

#include <vector>

#include "AccessGroup.h"

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

  /** Reads and writes access group "hints" file.
   * There are some state variables in each access group object that are
   * required immediately after the object is constructed.  These variables
   * improve the performance of the system, but are expensive to compute because
   * it requires scanning the METADATA table entry for the parent range, and
   * opening and loading the block indexes for all CellStores that make up the
   * access group.  To allow this state to be reconstructed cheaply on restart,
   * these state variables are persisted to a <i>hints</i> file in the filesystem
   * that is read before the access group objects are constructed.  There is one
   * hints file for each range that contains an entry for each access group that
   * makes up the range.  For each range, the hints file has the following path:
   * <pre>
   * /hypertable/tables/<table-id>/default/<md5-end-row>/hints
   * </pre>
   * This file is in YAML format and contains one mapping for each access group
   * with the following format:
   * <pre>
   * ag_name: {
   *   LatestStoredRevision: <revision>,
   *   DiskUsage: $bytes,
   *   Files: $file_list
   * }
   * </pre>
   * The fields in the mapping for each access group are described below.
   *
   *   - <b>LatestStoredRevision</b> - This is the revision number of the latest
   *     (or newest) cell persisted in the access group's cell stores.  When
   *     replaying the commit log after a restart, the access group can safely
   *     drop any cell whose revision number is less than or equal to this value.
   *   - <b>DiskUsage</b> - This is the amount of disk space taken up by the
   *     access group's cell stores.  This information is used to determine
   *     whether or not a range needs to be split.
   *   - <b>Files:</b> - This is a semicolon separated list of cell store files
   *     that are part of the access group.  This is information is not required
   *     by the access group immediately after construction, but is persisted
   *     here to assist in disaster recovery if the METADATA table becomes
   *     corrupt or %Hyperspace is lost.
   *
   * The reason that the hints for all of the access groups are persisted to a
   * single hints file for each range is to improve restart performance.  Upon
   * restart, a single file is read to obtain the hints for all access groups,
   * regardless of the number of access groups.
   */
  class AccessGroupHintsFile {

  public:

    /** Constructor.
     * @param table %Table ID string
     * @param end_row End row of range
     */
    AccessGroupHintsFile(const String &table, const String &end_row);

    /** Changes the end row.
     * This method is called after a range split to change the end row
     * that is used to compute the path name to the hints file.
     * @param end_row End row of range
     */
    void change_end_row(const String &end_row);

    /** Write hints file.
     * @param hints Vector of hints to be written, one for each access group
     */
    void write(const std::vector<AccessGroup::Hints> &hints);

    /** Reads hints file.
     * @param hints Vector of hints records to be populated, one for each access
     * group
     */
    void read(std::vector<AccessGroup::Hints> &hints);

  private:

    /// %Table ID string
    String m_table_id;

    /// %Range subdirectory (md5 of end row)
    String m_range_dir;
  };

  /* @} */

} // namespace Hypertable

#endif // HYPERTABLE_ACCESSGROUPHINTSFILE_H
