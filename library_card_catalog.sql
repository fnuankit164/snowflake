-- use role sysadmin;

create database library_card_catalog comment='DW Lesson 9';

use database library_card_catalog;

create or replace table author(
    AUTHOR_UID number
    ,FIRST_NAME varchar(50)
    ,MIDDLE_NAME varchar(50)
    ,LAST_NAME varchar(50)
);

INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
(1, 'Fiona', '','Macdonald')
,(2, 'Gian','Paulo','Faleschini');


SELECT * 
FROM AUTHOR;

select seq_author_uid.nextval;

show sequences;

create or replace sequence "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_AUTHOR_UID"
    START=3
    INCREMENT=1
    COMMENT='Use this to fill in the Author_UID every time you add a row';

INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
(SEQ_AUTHOR_UID.nextval, 'Laura', 'K','Egendorf')
,(SEQ_AUTHOR_UID.nextval, 'Jan', '','Grover')
,(SEQ_AUTHOR_UID.nextval, 'Jennifer', '','Clapp')
,(SEQ_AUTHOR_UID.nextval, 'Kathleen', '','Petelinsek');

use database library_card_catalog;

create or replace sequence "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_BOOK_UID"
    start 1
    increment 1
    comment = 'something';

    create or replace table book(
    book_uid number default seq_book_uid.nextval
    ,title varchar(50)
    ,year_published number(4,0)
    );
    
INSERT INTO BOOK(TITLE,YEAR_PUBLISHED)
VALUES
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

CREATE TABLE BOOK_TO_AUTHOR
(  BOOK_UID NUMBER
  ,AUTHOR_UID NUMBER
);

//Insert rows of the known relationships
INSERT INTO BOOK_TO_AUTHOR(BOOK_UID,AUTHOR_UID)
VALUES
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek

select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 

-- DO NOT EDIT THE CODE 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
     SELECT 'DWW15' as step 
     ,( select count(*) 
      from LIBRARY_CARD_CATALOG.PUBLIC.Book_to_Author ba 
      join LIBRARY_CARD_CATALOG.PUBLIC.author a 
      on ba.author_uid = a.author_uid 
      join LIBRARY_CARD_CATALOG.PUBLIC.book b 
      on b.book_uid=ba.book_uid) as actual 
     , 6 as expected 
     , '3NF DB was Created.' as description  
); 

use library_card_catalog;

create table library_card_catalog.public.author_ingest_json(
raw_author variant
);

create file format library_card_catalog.public.json_file_format
    type='JSON'
    compression = 'AUTO'
    enable_octal =False
    allow_duplicate=False
    strip_outer_array=True
    strip_null_values=False
    ignore_utf8_errors=False;

list @UTIL_DB.PUBLIC.LIKE_A_WINDOW_INTO_AN_S3_BUCKET;

copy into LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON
from @UTIL_DB.PUBLIC.LIKE_A_WINDOW_INTO_AN_S3_BUCKET/author_with_header.json
file_format=(format_name=json_file_format);

select * from author_ingest_json;

//returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

//returns the data in a way that makes it look like a normalized table
SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM author_ingest_json;

-- Set your worksheet drop lists. DO NOT EDIT THE DORA CODE.
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW16' as step
  ,( select row_count 
    from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
    where table_name = 'AUTHOR_INGEST_JSON') as actual
  ,6 as expected
  ,'Check number of rows' as description
 ); 


 create or replace table library_card_catalog.public.nested_ingest_json(
 "RAW_NESTED_BOOK" variant
 );


select raw_nested_book
from nested_ingest_json;

select raw_nested_book:year_published
from nested_ingest_json;

select raw_nested_book:authors 
from nested_ingest_json;

select value:first_name
from nested_ingest_json
,lateral flatten(input=>raw_nested_book:authors);

select value:first_name 
from nested_ingest_json
,table(flatten(raw_nested_book:authors));

select value:first_name::VARCHAR, value:last_name::VARCHAR
from nested_ingest_json
,lateral flatten(input=>raw_nested_book:authors);


select value:first_name::VARCHAR as first_nm, value:last_name::VARCHAR as last_nm
from nested_ingest_json
,lateral flatten(input=>raw_nested_book:authors);

-- Set your worksheet drop lists. DO NOT EDIT THE DORA CODE.

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (   
     SELECT 'DWW17' as step 
      ,( select row_count 
        from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
        where table_name = 'NESTED_INGEST_JSON') as actual 
      , 5 as expected 
      ,'Check number of rows' as description  
); 


















 
