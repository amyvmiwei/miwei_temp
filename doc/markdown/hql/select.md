SELECT
------
#### EBNF

    SELECT [CELLS] ('*' | (column_predicate [',' column_predicate]*))
      FROM table_name
      [where_clause]
      [options_spec]

    where_clause:
        WHERE where_predicate [AND where_predicate ...]

    where_predicate:
      cell_predicate
      | row_predicate
      | column_value_predicate
      | timestamp_predicate

    relop: '=' | '<' | '<=' | '>' | '>=' | '=^'
    
    column_predicate
      = column_family
      | column_family ':' column_qualifer
      | column_family ':' '/' column_qualifier_regexp '/'
      | column_family ':' '^' column_qualifier_prefix

    cell_spec: row ',' column

    cell_predicate:
      [cell_spec relop] CELL relop cell_spec
      | '(' [cell_spec relop] CELL relop cell_spec
            (OR [cell_spec relop] CELL relop cell_spec)* ')'

    row_predicate:
      [row_key relop] ROW relop row_key
      | '(' [row_key relop] ROW relop row_key
              (OR [row_key relop] ROW relop row_key)* ')'
      | ROW REGEXP '"'row_regexp'"'

    column_value_predicate:
      column_family '=' value
      | column_family '=' '^' value

    timestamp_predicate:
      [timestamp relop] TIMESTAMP relop timestamp

    options_spec:
      (MAX_REVISIONS revision_count
      | OFFSET row_offset
      | LIMIT row_count
      | CELL_OFFSET cell_offset
      | CELL_LIMIT max_cells
      | CELL_LIMIT_PER_FAMILY max_cells_per_cf
      | OFFSET row_offset
      | CELL_OFFSET cell_offset
      | INTO FILE [file_location]filename[.gz]
      | DISPLAY_TIMESTAMPS
      | KEYS_ONLY
      | NO_ESCAPE
      | RETURN_DELETES
      | SCAN_AND_FILTER_ROWS)*

    timestamp:
      'YYYY-MM-DD HH:MM:SS[.nanoseconds]'
    
    file_location:
      "dfs://" | "file://"
 
#### Description
<p>
SELECT is used to retrieve cells from a table. The retrieved cells are filtered
with predicates for row keys, timestamps or cell values.
The parser only accepts a single timestamp predicate.  The '=^' operator is the
"starts with" operator.  It will return all rows that have the same prefix as
the operand. Use of the value_predicate without the "CELLS" modifier to the
"SELECT" command is deprecated.
<p>
If your query selects several independent ranges by specifying multiple row
predicates  (i.e. WHERE ROW < 'a' OR ROW > 'c') then the LIMIT, CELL_LIMIT,
OFFSET, CELL_OFFSET predicates are applied to each range independently.
<p>
When specifying a column value predicate, the column family must be identical 
to the column family used in the SELECT clause, and exactly one column family
must be selected.  The following examples are valid: 
<pre><code>
    SELECT col FROM test WHERE col = "foo";
    SELECT col FROM test WHERE col =^ "prefix";
</code></pre>
<p>
The following examples are NOT valid because they select more than one 
column family or because the column family in the select clause is different 
from the one in the predicate (these limitations will be removed in future 
versions of Hypertable):
<pre><code>
    SELECT * FROM test WHERE col = "foo";
    SELECT col, col2 FROM test WHERE col =^ "prefix";
    SELECT foo FROM test WHERE bar = "value";
</code></pre>

#### Options
<p>
#### `MAX_REVISIONS revision_count`
<p>
Each cell in a Hypertable table can have multiple timestamped revisions.  By
default all revisions of a cell are returned by the `SELECT` statement.  The
`MAX_REVISIONS` option allows control over the number of cell revisions
returned.  The cell revisions are stored in reverse-chronological order, so
`MAX_REVISIONS 1` will return the most recent version of the cell.

#### `OFFSET row_offset`
<p>
Skips the first `row_offset` rows returned by the `SELECT` statement.  This
option cannot be combined with `CELL_OFFSET` and currently applies
independently to each row (or cell) interval supplied in the `WHERE` clause.

#### `LIMIT row_count`
<p>
Limits the number of rows returned by the `SELECT` statement to `row_count`.
The limit applies independently to each row (or cell) interval specified
in the `WHERE` clause.

#### `CELL_OFFSET cell_offset`
<p>
Skips the first cell_offset cells returned by the `SELECT` statement.
This option cannot be combined with `OFFSET` and currently applies
independently to each row (or cell) interval supplied in the WHERE clause.

#### `CELL_LIMIT max_cells`
<p>
Limits the total number of cells returned by the query to `max_cells`
(applied after `CELL_LIMIT_PER_FAMILY`).  The limit applies independently to each row
(or cell) interval specified in the `WHERE` clause.

#### `CELL_LIMIT_PER_FAMILY max_cells_per_cf`
<p>
Limits the number of cells returned per row per column family by the `SELECT` 
statement to `max_cells_per_cf`.

#### `OFFSET row_offset`
<p>
Skips the first `row_offset` rows returned by the SELECT statement.
Not allowed in combination with CELL_OFFSET.

#### `CELL_OFFSET cell_offset`
<p>
Skips the first `cell_offset` cells returned by the SELECT statement.
Not allowed in combination with OFFSET.

#### `INTO FILE [file://|dfs://]filename[.gz]`
<p>
The result of a `SELECT` command is displayed to standard output by default.
The `INTO FILE` option allows the output to get redirected to a file.  
If the file name starts with the location specifier `dfs://` then the output file is 
assumed to reside in the DFS. If it starts with `file://` then output is 
sent to a local file. This is also the default location in the absence of any 
location specifier.
If the file name specified ends in a `.gz` extension, then the output is compressed
with gzip before it is written to the file.  The first line of the output,
when using the `INTO FILE` option, is a header line, which will take one of
the two following formats.  The second format will be output if the
`DISPLAY_TIMESTAMPS` option is supplied.

     #row '\t' column '\t' value

     #timestamp '\t' row '\t' column '\t' value

<p>
#### `DISPLAY_TIMESTAMPS`
<p>
The `SELECT` command displays one cell per line of output.  Each line contains
three tab delimited fields, row, column, and value.  The `DISPLAY_TIMESTAMPS`
option causes the cell timestamp to be included in the output as well.  When
this option is used, each output line will contain four tab delimited fields
in the following order:

     timestamp, row, column, value
<p>
#### `KEYS_ONLY`
<p>
The `KEYS_ONLY` option suppresses the output of the value.  It is somewhat
efficient because the option is processed by the RangeServers and not by
the client.  The value data is not transferred back to the client, only
the key data.

#### `NO_ESCAPE`
<p>
The output format of a `SELECT` command comprises tab delimited lines, one
cell per line, which is suitable for input to the `LOAD DATA INFILE`
command.  However, if the value portion of the cell contains either newline
or tab characters, then it will confuse the `LOAD DATA INFILE` input parser.
To prevent this from happening, newline, tab, and backslash characters are
converted into two character escape sequences, described in the following table.

<table border="1">
<tr>
<td>&nbsp;<b>Character</b>&nbsp;</td>
<td>&nbsp;<b>Escape Sequence</b>&nbsp;</td>
</tr>
<tr>
<td>&nbsp;backslash \</td>
<td><pre> '\' '\' </pre></td>
</tr>
<tr>
<td>&nbsp;newline \n&nbsp;</td>
<td><pre> '\' 'n' </pre></td>
</tr>
<tr>
<td>&nbsp;tab \t</td>
<td><pre> '\' 't' </pre></td>
</tr>
<tr>
<td>&nbsp;NUL \0</td>
<td><pre> '\' '0' </pre></td>
</tr>
</table>
<p>
The `NO_ESCAPE` option turns off this escaping mechanism.
<p>
#### `RETURN_DELETES`
<p>
The `RETURN_DELETES` option is used internally for debugging.  When data is
deleted from a table, the data is not actually deleted right away.  A delete
key will get inserted into the database and the delete will get processed
and applied during subsequent scans.  The `RETURN_DELETES` option will return
the delete keys in addition to the normal cell keys and values.  This option
can be useful when used in conjuction with the `DISPLAY_TIMESTAMPS` option to
understand how the delete mechanism works.

<p>
#### `SCAN_AND_FILTER_ROWS`
<p>
The `SCAN_AND_FILTER_ROWS` option can be used to improve query performance
for queries that select a very large number of individual rows.  The default
algorithm for fetching a set of rows is to fetch each row individually, which
involves a network roundtrip to a range server for each row.  Supplying the
`SCAN_AND_FILTER_ROWS` option tells the system to scan over the data and
filter the requested rows at the range server, which will reduce the number of
network roundtrips required when the number of rows requested is very large.

<p>
#### Examples

    SELECT * FROM test WHERE ('a' <= ROW <= 'e') and
                             '2008-07-28 00:00:02' < TIMESTAMP < '2008-07-28 00:00:07';
    SELECT * FROM test WHERE ROW =^ 'b';
    SELECT * FROM test WHERE (ROW = 'a' or ROW = 'c' or ROW = 'g');
    SELECT * FROM test WHERE ('a' < ROW <= 'c' or ROW = 'g' or ROW = 'c');
    SELECT * FROM test WHERE (ROW < 'c' or ROW > 'd');
    SELECT * FROM test WHERE (ROW < 'b' or ROW =^ 'b');
    SELECT * FROM test WHERE "farm","tag:abaca" < CELL <= "had","tag:abacinate";
    SELECT * FROM test WHERE "farm","tag:abaca" <= CELL <= "had","tag:abacinate";
    SELECT * FROM test WHERE CELL = "foo","tag:adactylism";
    SELECT * FROM test WHERE CELL =^ "foo","tag:ac";
    SELECT * FROM test WHERE CELL =^ "foo","tag:a";
    SELECT * FROM test WHERE CELL > "old","tag:abacate";
    SELECT * FROM test WHERE CELL >= "old","tag:abacate";
    SELECT * FROM test WHERE "old","tag:foo" < CELL >= "old","tag:abacate";
    SELECT * FROM test WHERE (CELL = "maui","tag:abaisance" OR
                              CELL = "foo","tag:adage" OR
                              CELL = "cow","tag:Ab" OR
                              CELL =^ "foo","tag:acya");
    SELECT * FROM test INTO FILE "dfs:///tmp/foo";
    SELECT col2:"bird" FROM RegexpTest WHERE ROW REGEXP "http://.*"; 
    SELECT col1:/^w[^a-zA-Z]*$/ FROM RegexpTest WHERE ROW REGEXP "m.*\s\S";
    SELECT CELLS col1:/^w[^a-zA-Z]*$/ FROM RegexpTest WHERE VALUE REGEXP \"l.*e\";
    SELECT CELLS col1:/^w[^a-zA-Z]*$/ FROM RegexpTest WHERE ROW REGEXP \"^\\D+\" 
        AND VALUE REGEXP \"l.*e\";",
    SELECT col FROM test WHERE col = "foo";
    SELECT col FROM test WHERE col =^ "prefix";
    SELECT tags:^prefix FROM test;
