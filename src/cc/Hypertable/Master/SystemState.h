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
 * Declarations for SystemState.
 * This file contains declarations for SystemState, a MetaLog entity
 * for persisting global system state.
 */

#ifndef HYPERTABLE_SYSTEMSTATE_H
#define HYPERTABLE_SYSTEMSTATE_H

#include <vector>

#include "Hypertable/Lib/MetaLogEntity.h"
#include "Hypertable/Lib/SystemVariable.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Holds persistent global system state. */
  class SystemState : public MetaLog::Entity {
  public:

    /** Constructor.
     * This constructor intializes the #m_admin_specified and
     * #m_auto_specified arrays with system variables and their
     * default values.
     */
    SystemState();

    /** Constructor from MetaLog entity.
     * @param header_ MetaLog entity header
     */
    SystemState(const MetaLog::EntityHeader &header_);

    /** Destructor. */
    virtual ~SystemState() { }

    /** Set a vector of variables by administrator.
     * @param specs Vector of variable specifications
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool admin_set(std::vector<SystemVariable::Spec> &specs);

    /** Set a variable by administrator.
     * @param code Variable code to set
     * @param value Value of variable to set
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool admin_set(int code, bool value);

    /** Set a vector of variables by automated condition.
     * @param specs Vector of variable specifications
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool auto_set(std::vector<SystemVariable::Spec> &specs);

    /** Set a variable by administrator.
     * @param code Variable code to set
     * @param value Value of variable to set
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool auto_set(int code, bool value);

    /** Get system variables and generation number.
     * This method returns a vector of system variables and their current values
     * as well as the current system state generation number.  Each variable's
     * value is set to the default value if both the administratively and
     * automatically set variables are set to the default value, otherwise the
     * value is set to boolean NOT of the default value.
     * @param specs Reference to vector of variable specifications
     * @param generation Address of variable to hold generation number
     */
    void get(std::vector<SystemVariable::Spec> &specs, uint64_t *generation);

    /** Get system state variables that are not set to their default value.
     * This method returns a vector of system variable specs for the variables
     * that are not set to their default value.
     * @param specs Reference to vector of variable specifications
     * @param generation Address of variable to hold generation number
     */
    void get_non_default(std::vector<SystemVariable::Spec> &specs,
                         uint64_t *generation=0);

    /** Return name of entity.
     * @return Name of entity.
     */
    virtual const String name() { return "SystemState"; }

    /** Return textual representation of entity state.
     * @param os Output stream on which to write state string
     */
    virtual void display(std::ostream &os);

    /** Returns length of encoded state. */
    virtual size_t encoded_length() const;

    /** Encodes system state.
     * This method encodes the system state formatted as follows:
     * <pre>
     * Format version number
     * System state generation number
     * Number of administratively set variable specs
     * foreach administratively set variable spec {
     *   variable code
     *   variable value
     * }
     * Number of automatically set variable specs
     * foreach automatically set variable spec {
     *   variable code
     *   variable value
     * }
     * </pre>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    virtual void encode(uint8_t **bufp) const;

    /** Decodes system state.
     * See encode_state() for format description.
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp);

  private:

    /// Generation number incremented with each state change
    uint64_t m_generation;

    /// Administratively set state variables
    std::vector<SystemVariable::Spec> m_admin_specified;

    /// Automatically set state variables
    std::vector<SystemVariable::Spec> m_auto_specified;
  };

  /// Smart pointer to SystemState
  typedef intrusive_ptr<SystemState> SystemStatePtr;

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_SYSTEMSTATE_H
