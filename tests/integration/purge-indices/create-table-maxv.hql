use '/';
drop table if exists IndexTest;
create table IndexTest (
    Field1, 
    INDEX Field1, 
    Field2, 
    QUALIFIER INDEX Field2
) MAX_VERSIONS=1 COMPRESSOR="none";
