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
 * Declarations for OperationRegisterServerBlocker.
 * This file contains declarations for OperationRegisterServerBlocker, an
 * Operation class for blocking server registration until a lock
 * release notificaiton for the server's file has been delivered from
 * Hyperspace.
 */

#ifndef HYPERTABLE_OPERATIONRECOVERYBLOCKER_H
#define HYPERTABLE_OPERATIONRECOVERYBLOCKER_H

#include "OperationEphemeral.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Blocks range server registration.
   * An object of this class is created and added to the OperationProcessor when
   * a RangeServer acquires the lock on its lock file in Hyperspace.  It exists
   * soley to prevent server registration from occurring until after the
   * <i>lock released</i> event for the server has been delivered from
   * Hyperspace.
   */
  class OperationRegisterServerBlocker : public OperationEphemeral {
  public:

    /** Constructor.
     * Initializes object by passing parameters to parent constructor, setting
     * #m_location to <code>location</code>, and creating a single obstruction
     * "RegisterServerBlocker <location>".
     * @param context %Master context
     * @param location Server proxy name
     */
    OperationRegisterServerBlocker(ContextPtr &context, const String &location);

    /** Destructor. */
    virtual ~OperationRegisterServerBlocker() { }

    /** Carries out a range server recovery operation.
     * This method carries out the operation via the following states:
     * <table>
     * <tr><th>State</th><th>Description</th></tr>
     * <tr>
     * <td>INITIAL</td>
     * <td><ul>
     * <li>Transitions to STARTED state</li>
     * <li>Calls block() and returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>STARTED</td>
     * <td><ul>
     * <li>Calls complete_ok() and returns</li>
     * </ul></td>
     * </tr>
     * </table>
     */
    virtual void execute();

    /** Returns name of operation ("OperationRegisterServerBlocker")
     * @return %Operation name
     */
    virtual const String name();

    /** Returns descriptive label for operation
     * @return Descriptive label for operation
     */
    virtual const String label();

    virtual void display_state(std::ostream &os) { }

  private:
    /// Server proxy name
    String m_location;
  };

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRECOVERYBLOCKER_H
