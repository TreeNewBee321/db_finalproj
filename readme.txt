Netid & Name:
mf3971 Mengqi Fan 
dl4222 Ding Li


command.txt is a file to automatically input command from stardinput
mf3971_dl4222_AllOperations.txt contains all resulting table 

If the command reports an error of 
ModuleNotFoundReport:No module named 'ZODB', 
please try "pip install ZODB" or following instructions would help

1. git clone https://github.com/zopefoundation/ZODB.git
2. cd ZODB
3. python setup.py build 
4. python setup.py build_ext -DZODB_64BIT_INTS build
5. python setup.py install

then try the command again.

If an error "Python.h is not found", please try "sudo apt-get install python3-dev"(debian) or "sudo yum install python3-devel"(centOS)

Here are some information about the method writing in main.py

1. inputfromfile
load_file(s,db)
This function is used to load data.
Input: s is the name of input file, db is the table data will be imported in.
Output: a table with data and specified names.

2. select
select_func(data)
This function is to select rows which meets corresponding conditions.
Input: a list of slices of the command after split.
Output: a table with rows that meets corresponding conditions.

3. project
project_func(data):
The function is used to project value of specified attributes of all rows in a table.
Input: a list of slices of the command after split.
Output: a table of pecified attributes of all rows.

4. avg
avg_func(data):
This function is used to calculate average on the specified attribute.
Input: a list of slices of the command after split.
Output: the average on the specified attribute.

5. sum
sum_func(data):
This function is used to sum the value of element for specified attributes.
Input: a list of slices of the command after split.
Output:  the sum of all elements in a column.

6. count
count_func(data):
This function is used to count the number of rows in the table.
Input: a list of slices of the command after split.
Output: the number of rows for specified table.

7. join
join_func(data):
This function is use to join two tables if some of there attributes meet the condition.
Input: a list of slices of the command after split.
Output: a new table with new column names after join.

8. sort
sort_func(data):
The function is to sort the given table.
Input: a list of slices of the command after split.
Output: a list of sorted data.

9. avggroup
avggroup_func(data):
This function is used to calculate averages on a specified attribute , grouped by several columns.
Input: a list of slices of the command after split.
Output: a table with averages and the columns mentioned above, grouped by this columns.

10. sumgroup
sumgroup_func(data):
This function is used to sum the value of element grouped by specified attributes.
Input: a list of slices of the command after split.
Output:  sum the value of element grouped by specified attributes.

11. countgroup
countgroup_func(data):
The function is used to count elements grouped by specified attributes.
Input: a list of slices of the command after split.
Output: a table with count varies by value and groupby specified attributes.

12. movavg
movavg_func(data):
This function is used to calculate the moving averages on specified attributes.
Input: a list of slices of the command after split.
Output: the moving average on specified attributes.

13. movsum
movsum_func(data):
This function is used to calculate the moving sum on specified attributes.
Input: a list of slices of the command after split.
Output:  the moving sum on specified attributes.

14. concat
concat_func(data):
The function is to concatenate two tables.
Input: a list of slices of the command after split.
Output: a list of several dicts, whose length is the sum of the lengths of the two tables.

15. Hash
Hash(data):
The function is to set up an Hash index for a specified column.
Input: a list of slices of the command after split.
utput: a dict of a specified column of a specified table, which the value is a dict.

16. Btree
Btree(data):
The function is to set up an Btree index for a specified column.
Input: a list of slices of the command after split.
Output: a dict of a specified column of a specified table, which the value is Btree().

17. outputtofile
outputtofile(data):
This function is used to generate outputfile for a specified table.
Input: a list of slices of the command after split.
Output: an output file for the specified table.
