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

/// @file
/// Declarations for ColumnFamilySpec.
/// This file contains type declarations for ColumnFamilySpec, a class representing
/// a column family specification.

#ifndef Hypertable_Lib_ColumnFamilySpec_h
#define Hypertable_Lib_ColumnFamilySpec_h

#include <bitset>
#include <unordered_map>
#include <vector>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Specification for column family options.
  class ColumnFamilyOptions {
  public:

    /// Enumeration for <i>isset</i> bits
    enum {
      /// <i>max versions</i> bit
      MAX_VERSIONS,
      /// <i>ttl</i> bit
      TTL,
      /// <i>time order desc</i> bit
      TIME_ORDER_DESC,
      /// <i>counter</i> bit
      COUNTER,
      /// Total bit count
      MAX
    };

    /// Sets <i>max versions</i> option.
    /// Sets the MAX_VERSIONS bit of #m_isset and sets #m_max_versions to
    /// <code>max_versions</code>.
    /// @param max_versions New value for <i>max versions</i> option
    /// @return <i>true</i> if <i>max versions</i> option was set, otherwise
    /// <i>false</i> if <i>max versions</i> option was not set because the
    /// supplied value <code>max_versions</code> was equal to the existing
    /// value.
    /// @throws Exception if <code>max_versions</code> is negative or if the
    /// <i>counter</i> option is set, which is incompatible.
    bool set_max_versions(int32_t max_versions);

    /// Gets <i>max versions</i> option.
    /// @return <i>max versions</i> option.
    int32_t get_max_versions() const { return m_max_versions; }

    /// Checks if <i>max versions</i> option is set.
    /// This method returns the value of the MAX_VERSIONS bit of #m_isset.
    /// @return <i>true</i> if <i>max versions</i> option is set, <i>false</i>
    /// otherwise.
    bool is_set_max_versions() const;

    /// Sets <i>ttl</i> option.
    /// Sets the TTL bit of #m_isset and sets #m_ttl to <code>ttl</code>.
    /// @param ttl New value for <i>ttl</i> option
    /// @return <i>true</i> if <i>ttl</i> option was set, otherwise
    /// <i>false</i> if <i>ttl</i> option was not set because the supplied
    /// value <code>ttl</code> was equal to the existing value.
    /// @throws Exception if supplied <code>ttl</code> value is negative.
    bool set_ttl(time_t ttl);

    /// Gets <i>ttl</i> option.
    /// @return <i>ttl</i> option.
    time_t get_ttl() const { return m_ttl; }

    /// Checks if <i>ttl</i> option is set.
    /// This method returns the value of the TTL bit of #m_isset.
    /// @return <i>true</i> if <i>ttl</i> option is set, <i>false</i> otherwise.
    bool is_set_ttl() const;

    /// Sets <i>time order desc</i> option.
    /// Sets the TIME_ORDER_DESC bit of #m_isset and sets #m_time_order_desc to
    /// <code>value</code>.
    /// @param value New value for <i>time order desc</i> option
    /// @return <i>true</i> if <i>time order desc</i> option was set, otherwise
    /// <i>false</i> if <i>time order desc</i> option was not set because the
    /// supplied <code>value</code> was equal to the existing value.
    /// @throws Exception if supplied <code>value</code> is <i>true</i> and
    /// the <i>counter</i> option is set, which is incompatible with the
    /// <i>time order desc</i> option.
    bool set_time_order_desc(bool value);

    /// Gets <i>time order desc</i> option.
    /// @return <i>time order desc</i> option.
    bool get_time_order_desc() const { return m_time_order_desc; }

    /// Checks if <i>time_order_desc</i> option is set.
    /// This method returns the value of the TIME_ORDER_DESC bit of #m_isset.
    /// @return <i>true</i> if <i>time order desc</i> option is set,
    /// <i>false</i> otherwise.
    bool is_set_time_order_desc() const;

    /// Sets <i>counter</i> option.
    /// Sets the COUNTER bit of #m_isset and sets #m_counter to
    /// <code>value</code>.
    /// @param value New value for <i>counter</i> option.
    /// @return <i>true</i> if <i>counter</i> option was set, otherwise
    /// <i>false</i> if <i>counter</i> option was not set because the supplied
    /// <code>value</code> was equal to the existing value.
    /// @throws Exception if supplied <code>value</code> is <i>true</i> and
    /// either the <i>max versions</i> or <i>time order desc</i> options are
    /// set, which are incompatible with the <i>counter</i> option.
    bool set_counter(bool value);

    /// Gets the <i>counter</i> option.
    /// @return <i>counter</i> option.
    bool get_counter() const { return m_counter; }

    /// Checks if <i>counter</i> option is set.
    /// This method returns the value of the COUNTER bit of #m_isset.
    /// @return <i>true</i> if <i>counter</i> option is set, <i>false</i>
    /// otherwise.
    bool is_set_counter() const;

    /// Merges options from another ColumnFamilyOptions object.
    /// For each option that is not set, if the corresponding option in the
    /// <code>other</code> parameter is set, then the option is set to
    /// <code>other</code>'s value.
    /// @param other ColumnFamilyOptions object with which to merge
    void merge(const ColumnFamilyOptions &other);

    /** Parses XML options document.
     * This method parses an XML document which holds column family options,
     * populates the corresponding member variables.  The parser accepts
     * any element name for the outtermost element so that it can be used
     * to parse options from any section within a Schema XML document (e.g. 
     * ColumnFamilyDefaults and Options).  The following example illustrates a
     * valid XML opitons document.
     * @verbatim
     * <Options>
     *   <MaxVersions>3</MaxVersions>
     *   <TTL>1398615345</TTL>
     *   <TimeOrder>desc</TimeOrder>
     *   <Counter>false</Counter>
     * </Options>
     * @endverbatim
     * @param base Pointer to character buffer holding XML document
     * @param len Length of XML document
     */
    void parse_xml(const char *base, int len);

    /** Renders options in XML format.
     * %Column family options can be specified in an XML schema document within
     * the <code>ColumnFamily -> Options</code> element or the
     * <code>%AccessGroup -> ColumnFamilyDefaults</code> element.  This member
     * function generates the XML option specification elements that can be
     * used in both places.  The <code>line_prefix</code> parameter can be used
     * to get nice indenting if the object is being rendered as part of a larger
     * object and is typically some number of space characters.  The following
     * is example output produced by this member function.
     * @verbatim
     *   <MaxVersions>3</MaxVersions>
     *   <TTL>1398615345</TTL>
     *   <TimeOrder>desc</TimeOrder>
     *   <Counter>false</Counter>
     * @endverbatim
     * @param line_prefix String to prepend to each line of output
     * @return String representing options in XML format
     */
    const std::string render_xml(const std::string &line_prefix) const;

    /// Renders options in HQL format.
    /// %Column family options can be specified in an HQL
    /// <code>CREATE TABLE</code> command for columns, access group defaults,
    /// and table defaults.  In all three situations, the format of the options
    /// specification is the same.  The following shows an example of the HQL
    /// output that could be produced by this member function.
    /// <pre>
    /// MAX_VERSIONS 3 TTL 1398615345 TIME_ORDER desc
    /// </pre>
    /// @return String representing options in HQL format
    const std::string render_hql() const;

    /// Equality operator.
    /// @param other Other object to which comparison is to be made
    /// @return <i>true</i> if this object is equal to <code>other</code>,
    /// <i>false</i> otherwise.
    bool operator==(const ColumnFamilyOptions &other) const;

  private:

    /// Max version
    int32_t m_max_versions {};

    /// TTL
    time_t m_ttl {};

    /// Time order "desc" flag
    bool m_time_order_desc {};

    /// Counter
    bool m_counter {};

    /// Bit mask describing which options are set
    std::bitset<MAX> m_isset;
  };

  /// %Column family specification.
  class ColumnFamilySpec {
  public:

    /// Default constructor.
    ColumnFamilySpec() { }

    /// Constructor with column family name initializer.
    /// Initializes #m_name with <code>name</code>
    /// @param name %Column family name
    ColumnFamilySpec(const std::string &name) : m_name(name) { }

    /// Sets column family name.
    /// Sets #m_name to <code>name</code>
    /// @param name %Column family name
    void set_name(const std::string &name);

    /// Gets column family name.
    /// @return %Column family name.
    const std::string &get_name() const { return m_name; }

    /// Sets access group.
    /// Sets #m_ag to <code>ag</code>
    /// @param ag Access group name to which this column belons
    void set_access_group(const std::string &ag);

    /// Gets access group name.
    /// @return Access group name.
    const std::string &get_access_group() const { return m_ag; }

    /// Sets generation.
    /// @param generation Generation value
    void set_generation(int64_t generation);

    /// Gets generation.
    /// @return Generation value
    int64_t get_generation() const { return m_generation; }

    /// Sets column ID.
    /// @param id %Column ID
    void set_id(int32_t id);

    /// Gets column ID.
    /// @return %Column ID.
    int32_t get_id() const { return m_id; }

    /// Sets value index flag.
    /// Sets #m_value_index flag to <code>value</code> indicating whether or not
    /// a value index is specified for this column family.
    /// @param value Value index flag.
    void set_value_index(bool value);

    /// Gets value index flag.
    /// @return Value index flag.
    bool get_value_index() const { return m_value_index; }

    /// Sets qualifier index flag.
    /// Sets #m_qualifier_index flag to <code>value</code> indicating whether or not
    /// a qualifier index is specified for this column family.
    /// @param value %Qualifier index flag.
    void set_qualifier_index(bool value);

    /// Gets qualifier index flag.
    /// @return %Qualifier index flag.
    bool get_qualifier_index() const { return m_qualifier_index; }

    /// Sets deleted flag.
    /// Sets #m_deleted flag to <code>value</code> indicating whether or not the
    /// column family is deleted.
    /// @param value Deleted flag.
    void set_deleted(bool value);

    /// Gets deleted flag.
    /// @return Deleted flag.
    bool get_deleted() const { return m_deleted; }

    /// Sets <i>max versions</i> option.
    /// Sets the <i>max versions</i> option of the #m_options member to
    /// <code>max_versions</code>
    /// @param max_versions New value for <i>max versions</i> option
    /// @throws Exception if <code>max_versions</code> is negative or if the
    /// <i>counter</i> option is set, which is incompatible.
    void set_option_max_versions(int32_t max_versions);

    /// Gets <i>max versions</i> option.
    /// @return <i>max versions</i> option.
    int32_t get_option_max_versions() const {
      return m_options.get_max_versions();
    }

    /// Sets <i>ttl</i> option.
    /// Sets the <i>ttl</i> option of the #m_options member to <code>ttl</code>.
    /// @param ttl New value for <i>ttl</i> option
    /// @throws Exception if supplied <code>ttl</code> value is negative.
    void set_option_ttl(time_t ttl);

    /// Gets <i>ttl</i> option.
    /// @return <i>ttl</i> option.
    time_t get_option_ttl() const { return m_options.get_ttl(); }

    /// Sets <i>time order desc</i> option.
    /// Sets the <i>time order desc</i> option of the #m_options member to <code>value</code>.
    /// @param value New value for <i>time order desc</i> option
    /// @throws Exception if supplied <code>value</code> is <i>true</i> and
    /// the <i>counter</i> option is set, which is incompatible with the
    /// <i>time order desc</i> option.
    void set_option_time_order_desc(bool value);

    /// Gets <i>time order desc</i> option.
    /// @return <i>time order desc</i> option.
    bool get_option_time_order_desc() const {
      return m_options.get_time_order_desc();
    }

    /// Sets <i>counter</i> option.
    /// Sets the <i>counter</i> option of the #m_options member to
    /// <code>value</code>.
    /// @param value New value for <i>counter</i> option.
    /// @throws Exception if supplied <code>value</code> is <i>true</i> and
    /// either the <i>max versions</i> or <i>time order desc</i> options are
    /// set, which are incompatible with the <i>counter</i> option.
    void set_option_counter(bool value);

    /// Gets the <i>counter</i> option.
    /// @return <i>counter</i> option.
    bool get_option_counter() const { return m_options.get_counter(); }

    /// Merges options from another ColumnFamilyOptions object.
    /// Merges options from <code>other</code> int #m_options.
    /// @param other ColumnFamilyOptions object with which to merge
    void merge_options(const ColumnFamilyOptions &other);

    /** Renders spec in XML format.
     * Renders the spec as an XML document.  The <code>line_prefix</code>
     * parameter can be used to get nice indenting if the object is being
     * rendered as part of a larger object and is typically some number of space
     * characters.  The <code>with_ids</code> parameter causes the generated
     * XML to include the ID attribute and Generation elements.  The following
     * is example output produced by this member function with
     * <code>with_ids</code> set to <i>true</i>.
     * @verbatim
     <ColumnFamily id="42">
       <Generation>123</Generation>
       <Name>foo</Name>
       <AccessGroup>default</AccessGroup>
       <Deleted>false</Deleted>
       <QualifierIndex>true</QualifierIndex>
       <Options>
         <MaxVersions>3</MaxVersions>
         <TTL>1398615345</TTL>
         <TimeOrder>desc</TimeOrder>
       </Options>
     </ColumnFamily>
     @endverbatim
     * @param line_prefix String to prepend to each line of output
     * @param with_ids Include ID and Generation in output
     * @return String representing spec in XML format
     */
    const std::string render_xml(const std::string &line_prefix,
                                 bool with_ids=false) const;

    /// Equality operator.
    /// @param other Other object to which comparison is to be made
    /// @return <i>true</i> if this object is equal to <code>other</code>,
    /// <i>false</i> otherwise.
    bool operator==(const ColumnFamilySpec &other) const;

    /// Parses XML options document.
    /// This method parses an XML document representing a column family
    /// specification, populating the corresponding member variables.
    /// @param base Pointer to character buffer holding XML document
    /// @param len Length of XML document
    /// @see render_xml() for an example XML column family spec document
    void parse_xml(const char *base, int len);

    /// Renders spec in HQL format.
    /// Renders the column family specification in a format that can be used as
    /// the column specification in an HQL <code>CREATE TABLE</code> command.
    /// The following shows an example of the HQL produced by this member
    /// function.
    /// <pre>
    ///   foo MAX_VERSIONS 3 TTL 1398615345 TIME_ORDER desc, QUALIFIER INDEX foo
    /// </pre>
    /// @return String representing column specification in HQL format
    const std::string render_hql() const;

    /// Returns reference to options structure.
    /// @return Reference to options structure.
    const ColumnFamilyOptions &options() const { return m_options; }

  private:

    /// %Column family name
    std::string m_name;
    
    /// Access group name
    std::string m_ag;

    /// Generation
    int64_t m_generation {};

    /// ID
    int32_t m_id {};

    /// Options
    ColumnFamilyOptions m_options;

    /// Value index flag
    bool m_value_index {};

    /// %Qualifier index flag
    bool m_qualifier_index {};

    /// Deleted flag
    bool m_deleted {};
  };

  /// Vector of ColumnFamilySpec pointers
  typedef std::vector<ColumnFamilySpec *> ColumnFamilySpecs;

  /// @}

} // namespace Hypertable

#endif // Hypertable_Lib_ColumnFamily_h
