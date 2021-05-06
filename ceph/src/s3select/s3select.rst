===============
 Ceph s3 select 
===============

.. contents::

Overview
--------

    | The purpose of **s3 select** engine is to create an efficient pipe between user client to storage node (the engine should be close as possible to storage).
    | It enables the user to define the exact portion of data should be received by his side.
    | It also enables for higher level analytic-applications (such as SPARK-SQL) , using that feature to improve their latency and throughput.

    | For example, a s3-object of several GB (CSV file), a user needs to extract a single column which filtered by another column.
    | As the following query:
    | ``select customer-id from s3Object where age>30 and age<65;``

    | Currently the whole s3-object must retrieve from OSD via RGW before filtering and extracting data.
    | By "pushing down" the query into OSD , it's possible to save a lot of network and CPU(serialization / deserialization).

    | **The bigger the object, and the more accurate the query, the better the performance**.
 
Basic workflow
--------------
    
    | S3-select query is sent to RGW via `AWS-CLI <https://docs.aws.amazon.com/cli/latest/reference/s3api/select-object-content.html>`_

    | It passes the authentication and permission process as an incoming message (POST).
    | **RGWSelectObj_ObjStore_S3::send_response_data** is the “entry point”, it handles each fetched chunk according to input object-key.
    | **send_response_data** is first handling the input query, it extracts the query and other CLI parameters.
   
    | Per each new fetched chunk (~4m), it runs the s3-select query on that chunk.    
    | The current implementation supports CSV objects and since chunks are randomly “cutting” the CSV rows in the middle, those broken-lines (first or last per chunk) are skipped while processing the query.   
    | Those “broken” lines are stored and later merged with the next broken-line (belong to the next chunk), and finally processed.
   
    | Per each processed chunk an output message is formatted according to AWS specification and sent back to the client.    
    | For aggregation queries the last chunk should be identified as the end of input, following that the s3-select-engine initiates end-of-process and produces an aggregate result.  

Design Concepts
---------------

AST- Abstract Syntax Tree
~~~~~~~~~~~~~~~~~~~~~~~~~
    | The s3-select main flow is initiated with parsing of input-string (i.e user query), and follows 
    | with building an AST (abstract-syntax-tree) as a result.  
    | The execution phase is built upon the AST.
    
    | ``Base_statement`` is the base for the all object-nodes participating in the execution phase, it consists of the ``eval()`` method which returns the <value> object.
    
    | ``value`` object is handling the known basic-types such as int,string,float,time-stamp
    | It is able to operate comparison and basic arithmetic operations on mentioned types.
    
    | The execution-flow is actually calling the ``eval()`` method on the root-node (per each projection), it goes all the way down, and returns the actual result (``value`` object) from bottom node to root node(all the way up) .

    | **Alias** programming-construct is an essential part of s3-select language, it enables much better programming especially with objects containing many columns or in the case of complex queries.
    
    | Upon parsing the statement containing alias construct, it replaces alias with reference to the correct AST-node, on runtime the node is simply evaluated as any other node.

    | There is a risk that self(or cyclic) reference may occur causing stack-overflow(endless-loop), for that concern upon evaluating an alias, it is validated for cyclic reference.
    
    | Alias also maintains result-cache, meaning upon using the same alias more than once, it’s not evaluating the same node again(it will return the same result),instead it uses the result from cache.

    | Of Course, per each new row the cache is invalidated.
        

S3 select parser definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~
    | The implementation of s3-select uses the `boost::spirit <https://www.boost.org/doc/libs/1_71_0/libs/spirit/classic/doc/grammar.html>`_ the definition of s3-select command is according to AWS.
     
    | Upon parsing is initiated on input text, and a specific rule is identified, an action which is bound to that rule is executed.
    | Those actions are building the AST, each action is unique (as its rule), at the end of the process it forms a structure similar to a tree. 
    
    | As mentioned, running eval() on the root node, execute the s3-select statement (per projection).
    | The input stream is accessible to the execution tree, by the scratch-area object, that object is constantly updated per each new row. 

Basic functionalities
~~~~~~~~~~~~~~~~~~~~~

    | **S3select** has a definite set of functionalities that should be implemented (if we wish to stay compliant with AWS), currently only a portion of it is implemented.
    
    | The implemented software architecture supports basic arithmetic expressions, logical and compare expressions, including nested function calls and casting operators, that alone enables the user reasonable flexibility. 
    | review the bellow feature-table_.



Memory handling
~~~~~~~~~~~~~~~

    | S3select structures and objects are lockless and thread-safe, it uses placement-new in order to reduce the alloc/dealloc intensive cycles, which may impact the main process hosting s3-select.
    
    | Once AST is built there is no need to allocate memory for the execution itself, the AST is “static” for the query-execution life-cycle.
    
    | The execution itself is stream-oriented, meaning there is no pre-allocation before execution, object size has no impact on memory consumption.
    
    | It processes chunk after chunk, row after row, all memory needed for processing resides on AST. 
    
    | The AST is similar to stack behaviour in that it consumes already allocated memory and “releases” it upon completing its task.

S3 Object different types
~~~~~~~~~~~~~~~~~~~~~~~~~

    | The processing of input stream is decoupled from s3-select-engine, meaning , each input-type should have its own parser, converting s3-object into columns.
    
    | Current implementation includes only CSV reader; its parsing definitions are according to AWS.
    | The parser is implemented using `boost::state-machine <https://www.boost.org/doc/libs/1_64_0/libs/msm/doc/HTML/index.html>`_.
    
    | The CSV parser handles NULL,quote,escape rules,field delimiter,row delimiter and users may define (via AWS CLI) all of those dynamically.

Error Handling
~~~~~~~~~~~~~~
    | S3-select statement may be syntactically correct but semantically wrong, for one example ``select a * b from …`` , where a is number and b is a string.
    | Current implementation is for CSV file types, CSV has no schema, column-types may evaluate on runtime.
    | The above means that wrong semantic statements may occur on runtime.
    
    | As for syntax error ``select x frm stdin;`` , the builtin parser fails on first miss-match to language definition, and produces an error message back to client (AWS-CLI).
    | The error message is point on location of miss-match.
    
    | Fatal severity (attached to the exception) will end execution immediately, other error severity are counted, upon reaching 100, it ends execution with an error message.


AST denostration
~~~~~~~~~~~~~~~~
.. ditaa::

                                          +---------------------+ 
                                          |   select            | 
                                  +------ +---------------------+---------+
                                  |                    |                  |
                                  |                    |                  |      
                                  |                    |                  |
                                  |                    V                  |
                                  |        +--------------------+         |
                                  |        |      s3object      |         | 
                                  |        +--------------------+         |
                                  |                                       |
                                  V                                       V
                    +---------------------+                        +-------------+
                    |  projections        |                        |  where      |
                    +---------------------+                        +-------------+
                      |                  |                                |                        
                      |                  |                                |
                      |                  |                                |
                      |                  |                                |
                      |                  |                                |
                      |                  |                                |
                      V                  V                                V
               +-----------+      +-----------+                    +-------------+ 
               |  multiply |      |    date   |                    |    and      |
               +-----------+      +-----------+                    +-------------+
                |         |                                          |         |  
                |         |                                          |         |
                |         |                                          |         |
                |         |                                          |         |
                V         V                                          V         V
         +-------+    +-------+                                   +-----+   +-----+
         |payment|    | 0.3   |                                   | EQ  |   | LT  |
         +-------+    +-------+                                +--+-----+   +-----+--+
                                                               |        |   |        |
                                                               |        |   |        |
                                                               V        V   V        V
                                                          +-------+ +----+ +-----+ +-----+
                                                          | region| |east| |age  | | 30  |
                                                          +-------+ +----+ +-----+ +-----+

Features Support
----------------

.. _feature-table:

The following table describes the support for s3-select functionalities:

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Detailed        | Example                                                               |
+=================================+=================+=======================================================================+
| Arithmetic operators            | ^ * / + - ( )   | select (int(_1)+int(_2))*int(_9) from stdin;                          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 |                 | select ((1+2)*3.14) ^ 2 from stdin;                                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Compare operators               | > < >= <= == != | select _1,_2 from stdin where (int(1)+int(_3))>int(_5);               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | AND OR          | select count(*) from stdin where int(1)>123 and int(_5)<200;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | int(expression) | select int(_1),int( 1.2 + 3.4) from stdin;                            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 |float(expression)|                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | timestamp(...)  | select timestamp("1999:10:10-12:23:44") from stdin;                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | sum             | select sum(int(_1)) from stdin;                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | min             | select min( int(_1) * int(_5) ) from stdin;                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | max             | select max(float(_1)),min(int(_5)) from stdin;                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | count           | select count(*) from stdin where (int(1)+int(_3))>int(_5);            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | extract         | select count(*) from stdin where                                      |
|                                 |                 | extract("year",timestamp(_2)) > 1950                                  |    
|                                 |                 | and extract("year",timestamp(_1)) < 1960;                             |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | dateadd         | select count(0) from stdin where                                      |
|                                 |                 | datediff("year",timestamp(_1),dateadd("day",366,timestamp(_1))) == 1; |  
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | datediff        | select count(0) from stdin where                                      |  
|                                 |                 | datediff("month",timestamp(_1),timestamp(_2))) == 2;                  | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | utcnow          | select count(0) from stdin where                                      |
|                                 |                 | datediff("hours",utcnow(),dateadd("day",1,utcnow())) == 24 ;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | substr          | select count(0) from stdin where                                      |
|                                 |                 | int(substr(_1,1,4))>1950 and int(substr(_1,1,4))<1960;                |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| alias support                   |                 |  select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3                  | 
|                                 |                 |  from stdin where a3>100 and a3<300;                                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+

Sending Query to RGW
--------------------

Syntax
~~~~~~
CSV default defintion for field-delimiter,row-delimiter,quote-char,escape-char are: { , \\n " \\ }

::

 aws --endpoint-url http://localhost:8000 s3api select-object-content 
  --bucket {BUCKET-NAME}  
  --expression-type 'SQL'     
  --input-serialization 
  '{"CSV": {"FieldDelimiter": "," , "QuoteCharacter": "\"" , "RecordDelimiter" : "\n" , "QuoteEscapeCharacter" : "\\" , "FileHeaderInfo": "USE" }, "CompressionType": "NONE"}' 
  --output-serialization '{"CSV": {}}' 
  --key {OBJECT-NAME} 
  --expression "select count(0) from stdin where int(_1)<10;" output.csv

CSV parsing behavior
--------------------

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Description     | input ==> tokens                                                      |
+=================================+=================+=======================================================================+
|     NULL                        | successive      | ,,1,,2,    ==> {null}{null}{1}{null}{2}{null}                         |
|                                 | field delimiter |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     QUOTE                       | quote character | 11,22,"a,b,c,d",last ==> {11}{22}{"a,b,c,d"}{last}                    |
|                                 | overrides       |                                                                       |
|                                 | field delimiter |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     Escape                      | escape char     | 11,22,str=\\"abcd\\"\\,str2=\\"123\\",last                            |
|                                 | overrides       | ==> {11}{22}{str="abcd",str2="123"}{last}                             |
|                                 | meta-character. |                                                                       |
|                                 | escape removed  |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     row delimiter               | no close quote, | 11,22,a="str,44,55,66                                                 |
|                                 | row delimiter is| ==> {11}{22}{a="str,44,55,66}                                         |
|                                 | closing line    |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     csv header info             | FileHeaderInfo  | "**USE**" value means each token on first line is column-name,        |
|                                 | tag             | "**IGNORE**" value means to skip the first line                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
