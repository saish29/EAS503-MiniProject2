### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def execute_many_insert(sql_statement, values, conn):
    cur = conn.cursor()
    cur.executemany(sql_statement, values)
    conn.commit()
    
    return cur.lastrowid


def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    query = "Create table If not exists Region  (RegionID Integer not null primary key, Region Text not null)"
    create_table(conn, query)
    
    with open(data_filename) as f:
        ls = f.readlines()
        regions = set()
        
        for i, line in enumerate(ls):
            
            if i == 0:
                continue
            
            cols = line.split('\t')
            regions.add(cols[4])
    
    regions = list(regions)
    regions.sort()
    
    data = [(i + 1, x) for i, x in enumerate(regions)]
    
    region = "INSERT INTO Region VALUES (?,?)"
    execute_many_insert(region, data, conn)
    conn.close()
    
    ### END SOLUTION


def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    dict = {}
    
    con = create_connection(normalized_database_filename)
    reg_req = "Select * from Region"
    reg_ls = execute_sql_statement(reg_req, con)
    
    for l in reg_ls:
        dict[l[1]] = l[0]
    
    return dict

    ### END SOLUTION


def create_country_table(con,create,insert,flist):
    with con:
        create_table(con,create)
        cur = con.cursor()
        cur.executemany(insert, flist)


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    
    
    with open(data_filename, "r") as file:
        lines = file.readlines()
        
        # Creating Country Dictionary
        
        country_dict = {}
        final = []
        i = 1
    
        while i < len(lines):
            llist = lines[i].split("\t")
            reg = llist[4]
            ct = llist[3]
            
            if ct not in country_dict.keys():
                country_dict[ct] = reg
            i += 1
        
        fdict = dict(sorted(country_dict.items(), key = lambda x:(x[0])))
            
        # Country List (parsing)
        
        rdict = step2_create_region_to_regionid_dictionary(normalized_database_filename)
        id = 1
        #rdict = {}

        for k, v in fdict.items():
        
            if v in rdict.keys():
                final.append([id, k, rdict[v]])
            
            else:
                pass
            
            id = id + 1
    
        #
    country = "CREATE TABLE COUNTRY ( [CountryID] integer not null Primary key, [Country] Text not null,[RegionID] integer not null,FOREIGN KEY(RegionID) REFERENCES REGION(RegionID));"
    insert_country = "INSERT INTO COUNTRY VALUES (?,?,?);"

    conn = create_connection(normalized_database_filename)

    create_country_table(conn,country,insert_country,final)
    
         
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    country_id = "Select * from COUNTRY"
    lines = execute_sql_statement(country_id, conn)
    
    country_dict ={}

    for l in lines:
        country_dict[l[1]] = l[0]
        
    return country_dict

    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    
    conn = create_connection(normalized_database_filename)
    
    cust = '''Create table If not exists Customer  (
                CustomerID integer not null Primary Key, 
                FirstName Text not null,
                LastName Text not null,
                Address Text not null,
                City Text not null,
                CountryID integer not null,
                FOREIGN KEY(CountryID) REFERENCES Country(CountryID)
                )'''
    
    create_table(conn, cust)
    country_up = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    data = []
    
    with open(data_filename) as fp:
        lines = fp.readlines()
        
        
        for a, line in enumerate(lines):
            
            if (a == 0):
                continue
            
            split_lines = line.split('\t')
            data.append((split_lines[0].split()[0],' '.join(split_lines[0].split()[1:]),split_lines[1],split_lines[2],country_up[split_lines[3]]))
    
    data.sort(key = lambda x:x[0]+x[1])
    split_lines = [ ]
    
    for b,c in enumerate(data):
        split_lines.append((b + 1,*c))
    
    insert_cust = "INSERT INTO Customer VALUES (?,?,?,?,?,?)"
    
    execute_many_insert(insert_cust, split_lines, conn)

    ### END SOLUTION

def create_tocustid_dict(line):
    cdict = {}
    
    for ele in line:
        cdict[ele[0]] = ele[1]
    
    return cdict


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    cust_sql = "Select FirstName || ' ' || LastName AS fullname, CustomerID from customer"
    rows = execute_sql_statement(cust_sql, conn)  
    
    cust_dict = {}

    for row in rows:
        cust_dict[row[0]] = row[1]
    
    return cust_dict

    ### END SOLUTION
        

def create_prod_table(con,flist,create,insert):
    with con:
        create_table(con,create)
        cur = con.cursor()
        cur.executemany(insert, flist)


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    with open("data.csv", "r") as fp:
        lines = fp.readlines()
        prod_id = 1

    i = 1
    temp = []
    final = []
    
    while (i < len(lines)):
        list = lines[i].split("\t")
        prodCatDesc = list[7]
        prodCat = list[6]
        
        for cat, desc in zip(prodCat.split(';'), prodCatDesc.split(';')):
            
            if cat not in temp:
                final.append([prod_id,cat,desc])
                temp.append(cat)      
            else:
                pass
        
        i += 1
    
    final = sorted(final, key = lambda k: (k[1]))
   
    for f in final:
        f[0] = prod_id
        prod_id += 1

    create_prod = "CREATE TABLE ProductCategory ( [ProductCategoryID] integer not null Primary key, [ProductCategory] Text not null, [ProductCategoryDescription] Text no null);"
    insert_prod = "INSERT INTO ProductCategory VALUES (?,?,?);"

    conn = create_connection(normalized_database_filename)

    create_prod_table(conn,final,create_prod,insert_prod)

        

   
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    
    conn = create_connection(normalized_database_filename)
    prodcat_id = "Select ProductCategory,ProductCategoryID from ProductCategory"
    lines = execute_sql_statement(prodcat_id, conn)
    #prodcat_dict = create_prodcatid_dict(lines)
    #return prodcat_dict

    pid = {}
    
    for line in lines:
        pid[line[0]] = line[1]
    
    return pid
    
    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    pass
   
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    pass

    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    pass
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """

    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
       
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement