INSERT
------
#### EBNF

    INSERT INTO table_name VALUES value_list

    value_list:
        value_spec [',' value_spec ...]

    value_spec:
        '(' row ',' column ',' value ')'
        '(' timestamp ',' row ',' column ',' value ')'

    column:
        family [ ':' qualifier ]

    timestamp:
        YYYY-MM-DD HH:MM:SS[.nanoseconds]

#### Description
<p>
The `INSERT` command inserts data (cells) into a table.  The data is supplied
as a list of comma separated tuples.  Each tuple represents a cell and can take
one of two forms:

  * `(row, column, value)`
  * `(timestamp, row, column, value)`

The first form just supplies the row key, column key, and value as strings and
the cell timestamp is auto-assigned.  The second form supplies the timestamp in
addition to the row key, column key, and value.  For example, the following
`INSERT` statement:

    INSERT INTO fruit VALUES ("cantelope", "tag:good", "Had with breakfast"),
    ("2009-08-02 08:30:00", "cantelope", "description", "A cultivated variety
    of muskmelon with orange flesh"),("banana", "tag:great", "Had with lunch");

might yield the following output from the `SELECT` command:

    SELECT * FROM fruit DISPLAY_TIMESTAMPS;
    2009-08-11 05:06:17.246062001  banana     tag:great    Had with lunch
    2009-08-11 05:06:17.246062002  cantelope  tag:good     Had with breakfast
    2009-08-02 08:30:00.000000000  cantelope  description  A cultivated variety
    of muskmelon with orange flesh
<p>
If timestamps are not supplied, then they will be automatically assigned by
the RangeServer for non-indexed column families, and will be automatically
assigned by the client library for indexed column families.  It is therefore 
important that the system clock of the application servers be synchronized 
with the clocks of the Hypertable RangeServer machines.
<p>
Hypertable supports the GUID() function call for row key and value. It will 
generate a globally unique ID:

    INSERT INTO test VALUES (GUID(), "username", "bloefeld");
    INSERT INTO test VALUES ("harddisk0", "device", GUID());

might yield the following output from the SELECT command:

    3a983b8e-b7c7-49ae-b3e4-e221610f33ec    username    bloefeld
    harddisk0   device  6d38d110-8790-4a40-8653-701742343d1e
