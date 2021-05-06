# s3select
**The purpose of s3select engine** is to create an efficient pipe between user client to storage node (the engine should be as close as possible to storage, "moving computation into storage").

It enables the user to define the exact portion of data should received by his side.

It also enables for higher level analytic-applications (such as SPARK-SQL) , using that feature to improve their latency and throughput.

https://aws.amazon.com/blogs/aws/s3-glacier-select/

https://www.qubole.com/blog/amazon-s3-select-integration/

The engine is using boost::spirit to define the grammar , and by that building the AST (abstract-syntax-tree). upon statement is accepted by the grammar it create a tree of objects.

The hierarchy(levels) of the different objects also define their role, i.e. function could be a finite expression, or an argument for an expression, or an argument for other functions, and so forth.

Bellow is an example for “SQL” statement been parsed and transform into AST.
![alt text](/s3select-parse-s.png)

The where-clause is boolean expression made of arithmetic expression building blocks.

Projection is a list of arithmetic expressions

I created a container (**sudo docker run -it galsl/boost:latest /bin/bash/**) built with boost libraries , for building and running the s3select demo application.

**The demo can run on CSV files only, as follow. (folder s3select_demo)**
* bash> s3select -q ‘select _1 +_2,_5 * 3 from /...some..full-path/csv.txt where _1 > _2;’

* bash> cat /...some..full-path/csv.txt | s3select -q ‘select _1,_5 from stdin where _1 > _2;’

* bash> cat /...some..full-path/csv.txt | s3select -q ‘select c1,c5 from stdin where c1 > c2;’ -s ‘c1,c2,c3,c4,c5’

* bash> cat /...some..full-path/csv.txt | s3select -q 'select min(int(substr(_1,1,1))) from  stdin where  substr(_1,1,1) ==  substr(_2,1,1);'

-s flag is defining a schema (no type only names) , without schema each column can be accessed with _N (_1 is the first column).

-q flag is for the query.

the engine supporting the following arithmetical operations +,-,*,/,^ , ( ) , and also the logical operators and,or.

s3select is supporting float,decimal,string; it also supports aggregation functions such as max,min,sum,count; the input stream is accepted as string attributes, to operate arithmetical operation it need to CAST, i.e. int(_1) is converting text to integer.

The demo-app is producing CSV format , thus it can be piped into another s3select statement.

there is a small app /generate_rand_csv {number-of-rows} {number-of-columns}/ , which generate CSV rows containing only numbers.

the random numbers are produced with same seed number.

since it works with STDIN , it possible to concatenate several files into single stream.

cat file1 file2 file1 file2 | s3select -q ‘ ….. ‘
