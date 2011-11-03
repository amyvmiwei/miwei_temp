use '/';
drop table if exists IndexTest;
create table IndexTest (
    Field1, 
    INDEX Field1, 
    Field2, 
    INDEX Field2
) COMPRESSOR="none";
