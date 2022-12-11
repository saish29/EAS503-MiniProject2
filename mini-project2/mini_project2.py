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
        
def create_prodf_table(con,create,insert,plist):
    with con:
        create_table(con,create)
        cur = con.cursor()
        cur.executemany(insert, plist)



def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    with open('data.csv') as fp:
        lines = fp.readlines()
    prod_id = 1
    
    i = 1
    temp = []
    final = []
    
    while (i < len(lines)):
        ele = lines[i].split("\t")  
        price = ele[8]  # Price 8th
        cat = ele[6]    # Cat 6th
        name = ele[5]   # Name 5th
        product_dict = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
        
        for p_name,p_price,p_prodcat in zip(name.split(';'), price.split(';'), cat.split(';')):
            
            if p_prodcat in product_dict:
                
                if p_name not in temp:
                    
                    temp.append(p_name)
                    p_price = str(round((float(p_price)),2))
                    final.append([prod_id,p_name,p_price, product_dict[p_prodcat]])
                
                else:
                    pass
            
            else:
                pass
        
        i += 1
    final = sorted(final, key = lambda k: (k[1]))

    for f in final:
        f[0] = prod_id
        prod_id = prod_id + 1
    
    create_prod = "CREATE TABLE Product ([ProductID] integer not null Primary key, [ProductName] Text not null,[ProductUnitPrice] Real not null,[ProductCategoryID] integer not null,FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID));"
    insert_prod = "INSERT INTO Product VALUES (?,?,?,?);"

    conn = create_connection(normalized_database_filename)

    create_prodf_table(conn,create_prod,insert_prod,final)

    
    ### END SOLUTION



def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    
    conn = create_connection(normalized_database_filename)
    prod = "Select ProductID,ProductName from Product"
    lines = execute_sql_statement(prod, conn)
    
    prod = {ele[1]: ele[0] for ele in lines}

    return prod

    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    from datetime import datetime
    
    # Create Table

    conn = create_connection(normalized_database_filename)
    query = '''Create table If not exists OrderDetail  (
                OrderID integer not null Primary Key, 
                CustomerID integer not null,
                ProductID integer not null,
                OrderDate integer not null,
                QuantityOrdered integer not null,
                FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
                FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
                )'''
    create_table(conn, query)

    # Check normalized DB

    cust_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    prod_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)
    temp = []

    with open(data_filename) as fp:
        lines = fp.readlines()

        for k, l in enumerate(lines):

            if (k == 0):
                continue

            count = l.split("\t")
            productName = count[5].split(";")
            QuantityOrdered = count[9].split(";")
            OrderDate = count[10].split(";")

            for ele in zip(productName, QuantityOrdered, OrderDate):
                date_string = str(ele[2]).strip()
                date = datetime.strptime(date_string, '%Y%m%d').strftime('%Y-%m-%d')
                temp.append((cust_dict[count[0]], prod_dict[ele[0]], date, int(ele[1])))

    final = []

    for k,v in enumerate(temp):
        final.append((k + 1, *v))

    order = "INSERT INTO OrderDetail VALUES (?,?,?,?,?)"

    execute_many_insert(order, final, conn)


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
    
    # Using the cust funtion

    cdict = step6_create_customer_to_customerid_dictionary('normalized.db')
    
    for k,v in cdict.items():
        if (k == CustomerName):
            cust_id = v
            break
    
    sql_statement = """
    
        SELECT c.FirstName || ' ' || c.LastName AS Name,
            p.ProductName,
            o.OrderDate,
            p.ProductUnitPrice,
            o.QuantityOrdered,
            ROUND(p.ProductUnitPrice * o.QuantityOrdered, 2) AS Total
        FROM OrderDetail o
        JOIN Customer c ON o.CustomerID = c.CustomerID
        JOIN Product p ON o.ProductID = p.ProductID
        WHERE c.CustomerID = '%s' """ % cust_id

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
    
    SELECT c.FirstName || " " || c.LastName as Name, 
    ROUND(sum(p.ProductUnitPrice * o.QuantityOrdered),2) as Total 
    FROM OrderDetail o 
    JOIN Product p ON o.ProductID = p.ProductID 
    JOIN customer c ON o.CustomerID = c.CustomerID
    WHERE name = '{}' GROUP BY name """.format(CustomerName)

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
    
    SELECT c.FirstName || ' ' || c.LastName AS Name,
       ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY c.CustomerID
    ORDER BY Total DESC;
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

    SELECT r.Region, t.Total
    FROM Region r
    JOIN (
        SELECT co.RegionID,
            ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) AS Total
        FROM OrderDetail o
        JOIN Customer c ON o.CustomerID = c.CustomerID
        JOIN Product p ON o.ProductID = p.ProductID
        JOIN Country co ON c.CountryID = co.CountryID
        GROUP BY co.RegionID
    ) t ON r.RegionID = t.RegionID
    ORDER BY t.Total DESC;

    
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

    SELECT ct.Country, ROUND(sum(p.ProductUnitPrice * o.QuantityOrdered)) as CountryTotal
    FROM OrderDetail o
    JOIN Product p ON o.ProductID = p.ProductID
    JOIN Customer c ON o.CustomerID = c.CustomerID 
    JOIN Country ct ON c.CountryID = ct.CountryID
    JOIN Region r ON r.RegionID = ct.RegionID
    GROUP BY Country
    ORDER BY CountryTotal DESC
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
     SELECT r.Region, ct.Country, ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS CountryTotal,
        ROW_NUMBER() OVER (PARTITION BY r.Region ORDER BY SUM(p.ProductUnitPrice * o.QuantityOrdered) DESC) CountryRegionalRank
        From OrderDetail o
        JOIN Customer c ON o.CustomerID = c.CustomerID
        JOIN Product p ON o.ProductID = p.ProductID
        JOIN Country ct ON c.CountryID = ct.CountryID
        JOIN Region r ON r.RegionID = ct.RegionID
        GROUP BY Country
        ORDER BY r.Region ASC, CountryTotal DESC
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
      WITH country_rank AS (
        SELECT r.Region, ct.Country, ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS CountryTotal,
        ROW_NUMBER() OVER (PARTITION BY r.Region ORDER BY SUM(p.ProductUnitPrice * o.QuantityOrdered) DESC) CountryRegionalRank
        From OrderDetail o
        JOIN Customer c ON o.CustomerID = c.CustomerID
        JOIN Product p ON o.ProductID = p.ProductID
        JOIN Country ct ON c.CountryID = ct.CountryID
        JOIN Region r ON r.RegionID = ct.RegionID
        GROUP BY Country
        ORDER BY r.Region ASC, CountryTotal DESC
      )
      SELECT Region, Country, CountryTotal, CountryRegionalRank
      FROM country_rank
      WHERE CountryRegionalRank = 1
      ORDER BY Region ASC
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
       
    SELECT
            CASE
                WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 1 AND 3 THEN 'Q1'
                WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 4 AND 6 THEN 'Q2'
                WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 7 AND 9 THEN 'Q3'
                WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 10 AND 12 THEN 'Q4'
            END AS Quarter,
            CAST(strftime('%Y', o.OrderDate) AS INT) Year, c.CustomerID,
            ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS Total
        FROM OrderDetail o
        JOIN Customer c ON c.CustomerID = o.CustomerID
        JOIN Product p ON p.ProductID = o.ProductID
        GROUP BY c.CustomerID, Year, Quarter
        ORDER BY Year, Quarter, c.CustomerID

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
    
    WITH sales_quarter AS (
    SELECT
        CASE 
            WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 1 AND 3 THEN 'Q1'
            WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 4 AND 6 THEN 'Q2'
            WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 7 AND 9 THEN 'Q3'
            WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 10 AND 12 THEN 'Q4'
            END AS Quarter,
            CAST(strftime('%Y', o.OrderDate) AS INT) Year, c.CustomerID,
            ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS Total
            FROM OrderDetail o
            JOIN Customer c ON c.CustomerID = o.CustomerID
            JOIN Product p ON p.ProductID = o.ProductID
            GROUP BY c.CustomerID, Year, Quarter
            ORDER BY Year, Total DESC 
    )
    SELECT * FROM ( 
    SELECT Quarter, Year, CustomerID, Total, 
    ROW_NUMBER() OVER(PARTITION BY Year,Quarter ORDER BY Year,Quarter ASC) CustomerRank FROM sales_quarter
    ) 
    WHERE CustomerRank BETWEEN 1 AND 5



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

    WITH sales_month AS (
    SELECT
        CASE
            WHEN strftime('%m', o.OrderDate) = '01' THEN 'January'
            WHEN strftime('%m', o.OrderDate) = '02' THEN 'February'
            WHEN strftime('%m', o.OrderDate) = '03' THEN 'March'
            WHEN strftime('%m', o.OrderDate) = '04' THEN 'April'
            WHEN strftime('%m', o.OrderDate) = '05' THEN 'May'
            WHEN strftime('%m', o.OrderDate) = '06' THEN 'June'
            WHEN strftime('%m', o.OrderDate) = '07' THEN 'July'
            WHEN strftime('%m', o.OrderDate) = '08' THEN 'August'
            WHEN strftime('%m', o.OrderDate) = '09' THEN 'September'
            WHEN strftime('%m', o.OrderDate) = '10' THEN 'October'
            WHEN strftime('%m', o.OrderDate) = '11' THEN 'November'
            WHEN strftime('%m', o.OrderDate) = '12' THEN 'December'
        END AS Month,
        SUM(ROUND(p.ProductUnitPrice * o.QuantityOrdered)) AS Total
    FROM OrderDetail o
    JOIN Customer c ON c.CustomerID = o.CustomerID
    JOIN Product p ON p.ProductID = o.ProductID
    GROUP BY Month
    )

    SELECT Month, Total,
    ROW_NUMBER() OVER(ORDER BY total DESC) AS TotalRank
    FROM sales_month
    ORDER BY Total DESC
      
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

    WITH orders AS (
    SELECT c.CustomerID, c.FirstName, c.LastName, ct.Country, o.OrderDate,
        LAG(o.OrderDate) OVER (PARTITION BY c.CustomerID ORDER BY o.OrderDate) AS PreviousOrderDate
    FROM OrderDetail o
    JOIN Product p ON o.ProductID = p.ProductID
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Country ct ON c.CountryID = ct.CountryID
    JOIN Region r ON r.RegionID = ct.RegionID
    )

    SELECT CustomerID,FirstName, LastName, Country, OrderDate, PreviousOrderDate,
        MAX(JULIANDAY(OrderDate) - JULIANDAY(PreviousOrderDate)) AS MaxDaysWithoutOrder
    FROM orders
    GROUP BY CustomerID
    ORDER BY MaxDaysWithoutOrder DESC
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement