UPDATE 'test/Test4' "Test4-data.txt";
pause 1;
CREATE SCANNER ON 'test/Test4'[..??];
DESTROY SCANNER;
CREATE SCANNER ON 'test/Test4'[..??] RETURN_DELETES;
DESTROY SCANNER;
quit;
