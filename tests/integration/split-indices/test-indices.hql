use '/';

DESCRIBE TABLE IndexTest;
DESCRIBE TABLE "^IndexTest";

SELECT Field1 FROM IndexTest WHERE ROW <= "00001" AND Field1 =^ "1q";

SELECT Field1 FROM IndexTest WHERE ROW < "00100" AND Field1 =^ "a";

SELECT Field1 FROM IndexTest WHERE ROW < "00100" AND Field1 =^ "";

SELECT Field1 FROM IndexTest WHERE "03800" < ROW < "03900" AND Field1 =^ "6";

SELECT Field1 FROM IndexTest WHERE ROW > "04990" AND Field1 =^ "2";

SELECT Field1 FROM IndexTest WHERE ROW > "05900" AND Field1 =^ "";

SELECT Field1 FROM IndexTest WHERE ROW REGEXP "00\d\d$" AND Field1 =^ "A";

SELECT Field1 FROM IndexTest WHERE Field1 =^ "zfGV";

SELECT Field1 FROM IndexTest WHERE (CELL < "00020","Field1" OR CELL > "04980","Field1") AND Field1 =^ "2";

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 0 LIMIT 3;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 10 LIMIT 3;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 100 LIMIT 3;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 990 LIMIT 30;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 890 LIMIT 300;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 5000 LIMIT 3;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 1500 LIMIT 3;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 4995 LIMIT 5;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 4998 LIMIT 5;

SELECT Field1 FROM IndexTest WHERE Field1 =^ "" OFFSET 4500;


# now repeat with Field2
SELECT Field2 FROM IndexTest WHERE ROW <= "00001" AND Field2 =^ "txB5R";

SELECT Field2 FROM IndexTest WHERE ROW < "00100" AND Field2 =^ "a";

SELECT Field2 FROM IndexTest WHERE ROW < "00100" AND Field2 =^ "";

SELECT Field2 FROM IndexTest WHERE "03800" < ROW < "03900" AND Field2 =^ "6";

SELECT Field2 FROM IndexTest WHERE ROW > "04990" AND Field2 =^ "2";

SELECT Field2 FROM IndexTest WHERE ROW > "05900" AND Field2 =^ "";

SELECT Field2 FROM IndexTest WHERE ROW REGEXP "00\d\d$" AND Field2 =^ "A";

SELECT Field2 FROM IndexTest WHERE Field2 =^ "zfGV";

SELECT Field2 FROM IndexTest WHERE (CELL < "00020","Field1" OR CELL > "04980","Field1") AND Field2 =^ "1";

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 0 LIMIT 3;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 10 LIMIT 3;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 100 LIMIT 3;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 990 LIMIT 30;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 890 LIMIT 300;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 5000 LIMIT 3;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 1500 LIMIT 3;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 4995 LIMIT 5;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 4998 LIMIT 5;

SELECT Field2 FROM IndexTest WHERE Field2 =^ "" OFFSET 4500;


# now repeat with both fields
# this is currently disabled because it's not yet supported by the index.
# will be supported later, though
# SELECT * FROM IndexTest WHERE ROW <= "00001" AND Field2 =^ "1q";

# SELECT * FROM IndexTest WHERE ROW < "00100" AND Field2 =^ "a";

# SELECT * FROM IndexTest WHERE ROW < "00100" AND Field2 =^ "";

# SELECT * FROM IndexTest WHERE "03800" < ROW < "03900" AND Field2 =^ "6";

# SELECT * FROM IndexTest WHERE ROW > "04990" AND Field2 =^ "2";

# SELECT * FROM IndexTest WHERE ROW > "05900" AND Field2 =^ "";

# SELECT * FROM IndexTest WHERE ROW REGEXP "00\d\d$" AND Field2 =^ "A";

# SELECT * FROM IndexTest WHERE Field2 =^ "zfGV";

# SELECT * FROM IndexTest WHERE (CELL < "00020","Field1" OR CELL > "04980","Field1") AND Field2 =^ "2";

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 0 LIMIT 3;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 10 LIMIT 3;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 100 LIMIT 3;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 990 LIMIT 30;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 890 LIMIT 300;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 5000 LIMIT 3;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 1500 LIMIT 3;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 4995 LIMIT 5;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 4998 LIMIT 5;

# SELECT * FROM IndexTest WHERE Field2 =^ "" OFFSET 4500;
