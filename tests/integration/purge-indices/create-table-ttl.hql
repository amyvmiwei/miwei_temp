use '/';
drop table if exists IndexTest;
create table IndexTest (
    Field1 TTL=1, 
    INDEX Field1, 
    Field2 TTL=1, 
    QUALIFIER INDEX Field2
) COMPRESSOR="none";
