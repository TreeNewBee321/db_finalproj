import re
import sys
import time
import operator
from itertools import groupby
import ZODB
from BTrees.IOBTree import BTree

#This is for storing all resulting tables
dbs = dict()
#This is for storing all hash indexes
h = dict()
#This is for storing all btree indexes
b = dict()

# A file with all resulting tables and times
op_file = open("mf3971_dl4222_AllOperations.txt","a+")

# This function is used to load data
# Input: s is the name of input file, db is the table data will be imported in 
# Output: a table with data and specified names
def load_file(s,db):
    st = time.time()
    f = open(s,'r')
    lines = f.readlines()
    colhead_list = list()
    for i,val in enumerate(lines):
        if i == 0:
            col_head = re.split(r'[\s\|]',val)
            for _ in col_head:
                if _ == '':
                    continue
                colhead_list.append(_)
        else:
            datalist = re.split(r'[\s\|]',val)
            temp = dict()
            cnt = 0
            for index,d in enumerate(datalist):
                if d == '':
                    continue
                try:
                    temp[colhead_list[cnt]] = int(d)
                    # db[colhead_list[index]].append(int(d))
                except ValueError:
                    try:
                        temp[colhead_list[cnt]] = float(d)
                    except ValueError:
                        temp[colhead_list[cnt]] = d
                cnt = cnt+1
            db.append(temp)
    e = time.time()
    print(str(e-st))
    op_file.write("The time for inputfromfile is")
    op_file.write(str(e-st))
    op_file.write("s\n")

# This function is to select rows which meets corresponding conditions
# Input: a list of slices of the command after split
# Output: a table with rows that meets corresponding conditions
def select_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    temp_db = list()
    another_db = list()
    for _ in data[3:]:
        if _.strip() == "or":
            r = [x for x in another_db]+[y for y in temp_db if y not in another_db]
            another_db = temp_db
            temp_db.clear()
        elif _.strip() == "and":
            target_db = temp_db
            temp_db.clear()
        elif _.strip() == '' :
            pass
        else:
            # To see if the attribute is on the left or right
            left_side = True
            attr = ""
            arithop = ""
            constant = sys.maxsize
            relop = ""
            value = sys.maxsize

            ops = parse_relop(_)
            relop = ops[1]
            parts_1 = parse_arithop(ops[0])
            if len(parts_1)>1:
                attr = parts_1[0].strip()
                arithop = parts_1[1].strip()
                constant = float(parts_1[2].strip())
            else:
                try:
                    value = float(parts_1[0].strip())
                    left_side = False
                except:
                    attr = parts_1[0].strip()
            parts_2 = parse_arithop(ops[2])
            if len (parts_2)>1:
                attr = parts_2[0].strip()
                arithop = parts_2[1].strip()
                constant = float(parts_2[2].strip())
                left_side = False
            else:
                try:
                    value = float(parts_2[0].strip())
                except:
                    attr = parts_2[0].strip()
                    left_side = False
            try:
                if h[data[2].strip()][attr]:
                    for k in h[data[2].strip()][attr].keys():
                        ans = arith_trans(arithop,k,constant)
                        if left_side:
                            if rel_trans(relop,ans,value):
                                for index in h[data[2].strip()][attr][k]:
                                    temp_db.append(target_db[index])
                        else:
                            if rel_trans(relop,value,ans):
                                for index in h[data[2].strip()][attr][k]:
                                    temp_db.append(target_db[index])
            except:
                try:
                    if b[data[2].strip()][attr]:
                        for k in b[data[2].strip()][attr].items():
                            ans = arith_trans(arithop,k[0],constant)
                            if left_side:
                                if rel_trans(relop,ans,value):
                                    for v in enumerate(k[1]):
                                        temp_db.append(target_db[v])
                            else:
                                if rel_trans(relop,value,ans):
                                    for v in enumerate(k[1]):
                                        temp_db.append(target_db[v])
                except:
                    for i,x in enumerate(target_db):
                        a = x[attr]
                        ans = 0
                        ans = arith_trans(arithop,a,constant)
                        if left_side:
                            if rel_trans(relop,ans,value):
                                temp_db.append(x)
                        else:
                            if rel_trans(relop,value,ans):
                                temp_db.append(x)
    r = [x for x in another_db]+[y for y in temp_db if y not in another_db]
    dbs[data[0].strip()] = r
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)

# This function is used to parse th relation operator and operands
# Input: an equation string
# Output: a list of the first operand, the relation operator and the second operand
def parse_relop(line):
    new_line = re.split('(=|!=|>=|<=|>|<)',line)
    print(len(new_line))
    print(new_line)
    return new_line

# The function is used to parse the arithmatic operator and operands
# Input: an equation string
# Output: a list of the first operand, the arithmatic operator and the second operand
def parse_arithop(line):
    new_line = re.split('(\+|\-|\*|\/)',line)
    # print(new_line)
    return new_line

# The function is used to arithmatic calculation like +,-,*and/
# Input: an string type arithmatic operator, an operand x and an operand y
# Output: calculation results
def arith_trans(op,x,y):
    if op == "+":
        return operator.add(x,y)
    if op == "-":
        return operator.sub(x,y)
    if op == "*":
        return operator.mul(x,y)
    if op == "/":
        return operator.truediv(x,y)
    if op == '':
        return x

# The function is used to determine the relational between two values
# Input: an string type relational operator, an operand x and an operand y
# Output: True denotes the relation holds, False denotes the relation does not hold
def rel_trans(op,x,y):
    if op == "=":
        return x == y
    if op == "!=":
        return x != y
    if op == ">":
        return x>y
    if op == ">=":
        return x>=y
    if op == "<":
        return x<y
    if op == "<=":
        return x<=y

# The function is used to project value of specified attributes of all rows in a table
# Input: a list of slices of the command after split
# Output: a table of pecified attributes of all rows
def project_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    col = list()
    res_db = list()
    
    for _ in data[3:]:
        if _.strip() == '':
            continue
        col.append(_.strip())
    for _ in target_db:
        temp = dict()
        # print(_)
        for x in col:
            # print(x)
            temp[x] = _[x]
        res_db.append(temp)
    dbs[data[0].strip()] = res_db
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
    

# This function is used to group rows from a table by several attributes
# Input: db is the table, *arglist is the list of attributes
# Output: a list that gathered rows with same value on *arglist together in an element
def group(db,*arglist):
    group_key = operator.itemgetter(*arglist)
    res = list()
    for k,g in groupby(sorted(db,key=group_key),group_key):
        res.append(list(g))
    return res
# This function is used to calculate average on the specified attribute
# Input: a list of slices of the command after split
# Output: the average on the specified attribute
def avg_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    sum = 0
    for _ in target_db:
        sum+=_[attr]
    avg = operator.truediv(sum,len(target_db))
    title = "avg("+attr+")"
    temp = dict()
    temp[title] = avg
    dbs[data[0].strip()] = list()
    dbs[data[0].strip()].append(temp)
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used to calculate averages on a specified attribute , grouped by several columns
# Input: a list of slices of the command after split
# Output: a table with averages and the columns mentioned above ,grouped by this columns
def avggroup_func(data):
    s = time.time()
    dbs[data[0].strip()] = list()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    arglist = list()
    for _ in data[4:]:
        if _.strip() != '':
            arglist.append(_.strip())
    res = group(target_db,*arglist)
    for g in res:
        temp = dict()
        sum = 0
        for x in g:
            sum+=x[attr]
        avg = operator.truediv(sum,len(g))
        title = "avg("+attr+")"
        temp[title] = avg
        for x in arglist:
            temp[x] = g[0][x]
        dbs[data[0].strip()].append(temp)
        print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used to calculate the moving average for a list
# Input : l is the given list, N is the size of the window
# Output: a list of moving averages
def running_mean(l, N):
    sum = 0
    result = list( 0 for x in l)
    for i in range( 0, N ):
        sum = sum + l[i]
        result[i] = sum / (i+1)

    for i in range( N, len(l) ):
        sum = sum - l[i-N] + l[i]
        result[i] = sum / N
    return result
# This function is used to calculate the moving sum for a list
# Input : l is the given list, N is the size of the window
# Output: a list of moving sum
def running_sum (l, N):
    sum = 0
    result = list( 0 for x in l)
    for i in range( 0, N ):
        sum = sum + l[i]
        result[i] = sum

    for i in range( N, len(l) ):
        sum = sum - l[i-N] + l[i]
        result[i] = sum
    return result   

# This function is used to calculate the moving averages on specified attributes
# Input : a list of slices of the command after split
# Output: the moving average on specified attributes
def movavg_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    window = int(data[4].strip())
    itemlist = list()
    for _ in target_db:
        itemlist.append(_[attr])
    print(itemlist)
    res = running_mean(itemlist,window)
    temp = dict()
    title = "movavg("+attr+")"
    dbs[data[0].strip()] = list()
    for _ in res:
        temp[title] = _
        dbs[data[0].strip()].append(temp)
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used to calculate the moving sum on specified attributes
# Input : a list of slices of the command after split
# Output:  the moving sum on specified attributes
def movsum_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    window = int(data[4].strip())
    itemlist = list()
    for _ in target_db:
        itemlist.append(_[attr])
    res = running_sum(itemlist,window)
    temp = dict()
    title = "movsum("+attr+")"
    dbs[data[0].strip()] = list()
    for _ in res:
        temp[title] = _
        dbs[data[0].strip()].append(temp)
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used to sum the value of element for specified attributes
# Input : a list of slices of the command after split
# Output:  the sum of all elements in a column
def sum_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    sum = 0
    for _ in target_db:
        sum+=_[attr]
    title = "sum("+attr+")"
    temp = dict()
    temp[title] = sum
    dbs[data[0].strip()] = list()
    dbs[data[0].strip()].append(temp)  
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used to sum the value of element grouped by specified attributes
# Input : a list of slices of the command after split
# Output:  sum the value of element grouped by specified attributes
def sumgroup_func(data):
    s = time.time()
    dbs[data[0].strip()] = list()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    arglist = list()
    for _ in data[4:]:
        if _.strip() != '':
            arglist.append(_.strip())
    res = group(target_db,*arglist)
    for g in res:
        temp = dict()
        sum = 0
        for x in g:
            sum+=x[attr]
        title = "sum("+attr+")"
        temp[title] = sum
        for x in arglist:
            temp[x] = g[0][x]
        dbs[data[0].strip()].append(temp)
        print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used to count the number of rows in the table
# Input: a list of slices of the command after split
# Output: the number of rows for specified table
def count_func(data):
    s = time.time()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    cnt = len(target_db)
    title = "count("+attr+")"
    temp = dict()
    temp[title] = cnt
    dbs[data[0].strip()] = list()
    dbs[data[0].strip()].append(temp) 
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# The function is used to count elements grouped by specified attributes
# Input: a list of slices of the command after split
# Output: a table with count varies by value and groupby specified attributes
def countgroup_func(data):
    s = time.time()
    dbs[data[0].strip()] = list()
    target_db = dbs[data[2].strip()]
    attr = data[3].strip()
    arglist = list()
    for _ in data[4:]:
        if _.strip() != '':
            arglist.append(_.strip())
    res = group(target_db,arglist)
    for g in res:
        temp = dict()
        cnt = len(g)
        title = "count("+attr+")"
        temp[title] = cnt
        for x in arglist:
            temp[x] = g[0][x]
        dbs[data[0].strip()].append(temp)
        print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is use to join two tables if some of there attributes meet the condition
# Input: a list of slices of the command after split
# Output: a new table with new column names after join
def join_func(data):
    s = time.time()
    dbs[data[0].strip()] = list()
    target_db = list()
    for x in dbs[data[2].strip()]:
        for y in dbs[data[3].strip()]:
            target_db.append((x,y))
    temp_db =list()
    another_db = list()
    for _ in data[4:]:
        if _.strip() == '':
            pass
        elif _.strip() == 'and':
            target_db = temp_db
            temp_db.clear()
        else :
            table1 = ""
            attr1 = ""
            arith1 = ""
            cons1 = sys.maxsize

            table2 = ""
            attr2 = ""
            arith2 = ""
            cons2 = sys.maxsize

            ops = parse_relop(_)
            relop = ops[1]
            parts1 = parse_arithop(ops[0])
            if len(parts1) > 1:
                table_attr = parts1[0].strip()
                arith1 = parts1[1].strip()
                cons1 = float(parts1[2].strip())

                ta_at = table_attr.split('.')
                table1 = ta_at[0].strip()
                attr1 = ta_at[1].strip()
            else:
                table_attr = parts1[0].strip()
                ta_at = table_attr.split('.')
                table1 = ta_at[0].strip()
                attr1 = ta_at[1].strip()

            parts2 = parse_arithop(ops[2])
            if len(parts2) > 1:
                table_attr = parts2[0].strip()
                arith2 = parts2[1].strip()
                cons2 = float(parts2[2].strip())     
            else:
                table_attr = parts2[0].strip()
                ta_at = table_attr.split('.')
                table2 = ta_at[0].strip()
                attr2 = ta_at[1].strip()
            
            for (x,y) in target_db:
                res_x = arith_trans(arith1,x[attr1],cons1)
                res_y = arith_trans(arith2,y[attr2],cons2)
                
                if rel_trans(relop,res_x,res_y):
                    temp_db.append((x,y))
    r = [x for x in another_db]+[y for y in temp_db if y not in another_db]    
    colhead = list()
    colhead.append({})
    colhead.append({})
    change_column(data[2].strip(),data[3].strip(),colhead)
    for _ in r:
        temp = dict()
        for i,ele in enumerate(_):
            for k,v in ele.items():
                temp[colhead[i][k]] = v
        dbs[data[0].strip()].append(temp)
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# This function is used in the 'join' operation. 
# If any two columns with same name exist, both of them should update their names
# Input: table 1 , table 2, and a list with no element
# Output: a list of column names for the new table
def change_column(db1,db2,colhead):
    x = dbs[db1][0].keys()
    y = dbs[db2][0].keys()
    for k in x:
        if k not in y:
            colhead[0][k] = k
        else:
            colhead[0][k] = db1+"_"+k
    for k in y:
        if k not in x:
            colhead[1][k] = k
        else:
            colhead[1][k] = db2+'_'+k
    
# The function is to sort the given table
# Input: a list of slices of the command after split
# Output: a list of sorted data
def sort_func(data):
    s = time.time()
    dbs[data[0].strip()] = list()
    target_db = dbs[data[2].strip()] 
    arglist = list()
    for _ in data[3:]:
        if _.strip() != '':
            arglist.append(_.strip())
    res_db = sorted(target_db,key = operator.itemgetter(*arglist))
    print(res_db)
    dbs[data[0].strip()] = res_db
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# The function is to concatenate two tables
# Input: a list of slices of the command after split
# Output: a list of several dicts, whose length is the sum of the lengths of the two tables
def concat_func(data):
    s = time.time()
    db1 = dbs[data[2].strip()]
    db2 = dbs[data[3].strip()]
    dbs[data[0].strip()] = list()
    dbs[data[0].strip()] = db1+db2
    print(dbs[data[0].strip()])
    e = time.time()
    print(str(e-s))
    get_Operations(dbs[data[0].strip()],str(e-s),op_file)
# The function is to set up an Hash index for a specified column
# Input: a list of slices of the command after split
# Output: a dict of a specified column of a specified table, which the value is a dict
# Each key of Hash is a value of the original column, 
# and the value of that key is a list of indexes which the corresponding value of the original key equals to 
# the current key of Hash

def Hash(data):
    s = time.time()
    target_db = dbs[data[1].strip()]
    attr = data[2].strip()
    h[data[1].strip()] = dict()
    h[data[1].strip()][attr] = dict()
    for i,_ in enumerate(target_db):
        v = _[attr]
        if _[attr] not in h[data[1].strip()][attr].keys():
            h[data[1].strip()][attr][v] = list()
        h[data[1].strip()][attr][v].append(i)
    print(h[data[1].strip()][attr])
    e = time.time()
    print(str(e-s))
    op_file.write("The time for add Hash index is "+str(e-s)+"s\n")

# The function is to set up an Btree index for a specified column
# Input: a list of slices of the command after split
# Output: a dict of a specified column of a specified table, which the value is Btree().
# Each key of Btree is a value of the original column, 
# and the value of that key is a list of indexes which the corresponding value of the original key equals to 
# the current key of Btree 
def Btree(data):
    s = time.time()
    target_db = dbs[data[1].strip()]
    attr = data[2].strip()   
    b[data[1].strip()] = dict()
    b[data[1].strip()][attr] = BTree()
    for i,_ in enumerate(target_db):
        v = _[attr]
        if b[data[1].strip()][attr].has_key(v):
            l = b[data[1].strip()][attr].get(v)
            l.append(i)
            b[data[1].strip()][attr].update([(v,l)])
        else:
            b[data[1].strip()][attr].insert(v,[i])
    e = time.time()
    op_file.write("The time for add Btree index is "+str(e-s)+"s\n")

#This function is used to generate outputfile for a specified table
#Input: a list of slices of the command after split
#Output: an output file for the specified table
def outputtofile(data):
    s = time.time()
    target_db = dbs[data[1].strip()]
    name = data[2].strip()
    f = open("mf3971_dl4222_"+name+".txt",'w')
    for i,title in enumerate(target_db[0].keys()):
        if i > 0:
            f.write("|")
        f.write(str(title))
    f.write("\n")
    for _ in target_db:
        for i,v in enumerate(_.values()):
            if i > 0:
                f.write("|")
            f.write(str(v))
        f.write("\n")
    e = time.time()
    print(str(e-s))
    op_file.write("The time for outputfile is "+ str(e-s) +"s\n")

# This function is for parsing the command
# Input: a list of slices of the command after split
# Output: No output.It just parse the command and execute corresponding operations   
def parse_command(command):
    data = re.split('[,()]|:=',command)
    print(data)
    if data[1].strip() == 'inputfromfile':
        s = data[2].strip()+'.txt'
        dbs[data[0].strip()] = list()
        load_file(s,dbs[data[0].strip()])
        # select clause
    elif data[1].strip() == 'select':
        select_func(data)
    elif data[1].strip() == 'project':
        project_func(data)
    elif data[1].strip() == "avg":
        avg_func(data)
    elif data[1].strip() == "sum":
        sum_func(data)
    elif data[1].strip() == "count":
        count_func(data)
    elif data[1].strip() == 'join':
        join_func(data)
    elif data[1].strip() == 'sort':
        sort_func(data)
    elif data[1].strip() == "avggroup":
        avggroup_func(data)   
    elif data[1].strip() == "sumgroup":
        sumgroup_func(data) 
    elif data[1].strip() == "countgroup":
        countgroup_func(data) 
    elif data[1].strip() == "movavg":
        movavg_func(data) 
    elif data[1].strip() == "movsum":
        movsum_func(data)   
    elif data[1].strip() == "concat":
        concat_func(data)  
    elif data[0].strip() == "Hash":
        Hash(data)
    elif data[0].strip() == "Btree":
        Btree(data)
    elif data[0].strip() == "outputtofile":
        outputtofile(data)
    else:
        print("Nothing happens\n")
# This function is used to print the resulting table for current operation
# Input: db is the name of the resulting table,t is the running time, f is a file to write in the data
# Output: all rows in the given table with vertical bar separators
def get_Operations(db, t, f):
    if db:
        for i,k in enumerate(db[0].keys()):
            if i > 0:
                f.write("|")
            f.write(k)
        f.write("\n")
        for _ in db:
            for i,v in enumerate(_.values()):
                if i > 0:
                    f.write("|")
                f.write(str(v))
            f.write("\n")
    f.write("The execute time is "+ t + "s")


if __name__ == "__main__":
    commandfile = open(sys.argv[1],'r')
    for command in commandfile.readlines():
        c = re.split('//',command)
        print(c)
        if c[0].strip() != '':  
            parse_command(c[0])
        op_file.write("\n")
    op_file.close()

