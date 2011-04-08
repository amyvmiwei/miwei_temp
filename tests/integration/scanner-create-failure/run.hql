use '/';
drop table if exists ScannerFailTest;
create table ScannerFailTest (
  Field
) COMPRESSOR="none";
insert into ScannerFailTest values ("r1", "Field", "v1");
insert into ScannerFailTest values ("r2", "Field", "v2");
select * from ScannerFailTest where ('r1'<= row <= 'r2' or row = 'r1' or row > 'r2');
select * from ScannerFailTest where ('r1'<= row <= 'r2' or row = 'r1' or row > 'r2');
select * from ScannerFailTest where ('r1'<= row <= 'r2' or row = 'r1' or row > 'r2');
select * from ScannerFailTest where ('r1'<= row <= 'r2' or row = 'r1' or row > 'r2');
select * from ScannerFailTest where ('r1'<= row <= 'r2' or row = 'r1' or row > 'r2');
quit;
