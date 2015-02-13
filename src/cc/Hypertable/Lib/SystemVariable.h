/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
 * Declarations for SystemVariable.
 * This file contains the namespace SystemVariable which includes an enum and
 * conversion functions for representing system variables.
 */

#ifndef Hypertable_Lib_SystemvVriable_h
#define Hypertable_Lib_SystemvVriable_h

#include <Common/Serializable.h>
#include <Common/String.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup libHypertable
  ///  @{

  namespace SystemVariable {

    /** Enumeration for variable codes.
     */
    enum Code {
      READONLY = 0,   /**< Read-only */
      COUNT    = 1    /**< Valid code count */
    };

    /// Holds a variable code and boolean value.
    struct Spec : public Serializable {

      /// Variable code
      int32_t code;

      /// Variable value
      bool value;
      
      /// Returns encoding version.
      /// @return Encoding version
      uint8_t encoding_version() const override;

      /// Returns internal serialized length.
      /// @return Internal serialized length
      /// @see encode_internal() for encoding format
      size_t encoded_length_internal() const override;

      /// Writes serialized representation of object to a buffer.
      /// @param bufp Address of destination buffer pointer (advanced by call)
      void encode_internal(uint8_t **bufp) const override;

      /// Reads serialized representation of object from a buffer.
      /// @param version Encoding version
      /// @param bufp Address of destination buffer pointer (advanced by call)
      /// @param remainp Address of integer holding amount of serialized object
      /// remaining
      /// @see encode_internal() for encoding format
      void decode_internal(uint8_t version, const uint8_t **bufp,
                           size_t *remainp) override;

    };

    /** Converts variable code to variable string.
     * @param var_code The variable code
     * @return Variable string corresponding to variable code
     */
    const char *code_to_string(int var_code);

    /** Converts variable string to variable code.
     * @param var_string std::string representation of variable
     * @return Variable string corresponding to variable code or 0 if
     * <code>var_string</code> is invalid
     */
    int string_to_code(const std::string &var_string);

    /** Returns default value for given variable.
     * @param var_code The variable code
     * @return Default value for <code>var_code</code>
     */
    bool default_value(int var_code);

    /** Returns a textual representation of variable specifications.
     * @param specs Vector of variable specifications
     * @return Textual representation of variable specifications
     */
    std::string specs_to_string(const std::vector<Spec> &specs);

    /** Returns encoded length of variable specs vector.
     * @param specs Vector of variable specs
     * @return Encoded length of <code>specs</code>
     */
    size_t encoded_length_specs(const std::vector<Spec> &specs);

    /** Encodes a vector of variable specs.
     * This method encodes a vector of variable specs formatted as follows:
     * <pre>
     *   Number of variable specs
     *   foreach variable spec {
     *     variable code
     *     variable value
     *   }
     * </pre>
     * @param specs Vector of variable specs
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    void encode_specs(const std::vector<Spec> &specs, uint8_t **bufp);

    /** Decodes a vector of variable specs.
     * See encode_state() for format description.
     * @param specs Vector of variable specs
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @param remainp Address of integer holding amount of remaining buffer
     */
    void decode_specs(std::vector<Spec> &specs,
                      const uint8_t **bufp, size_t *remainp);

  }

  /// @}

}

#endif // Hypertable_Lib_SystemvVriable_h
