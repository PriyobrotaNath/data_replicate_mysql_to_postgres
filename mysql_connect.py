import pymysql

# MySQL database configuration
#establishing the conn
conn = pymysql.connect(
   database="my_db",
    user='root', 
    password='mysql123', 
    host='127.0.01', 
    port= 3306
)

# conn.autocommit = True
# Create a cursor object using the cursor() method
cursor = conn.cursor()

#Creating table as per requirement
# sql ='''CREATE TABLE my_db(
#    FIRST_NAME CHAR(20) NOT NULL,
#    LAST_NAME CHAR(20),
#    AGE INT,
#    SEX CHAR(1),
#    INCOME FLOAT
# )'''

# #Creating a database
# cursor.execute(sql)
# print("Table created successfully........")



# Preparing SQL queries to INSERT a record into the database.
# cursor.execute('''INSERT INTO my_db(FIRST_NAME, LAST_NAME, AGE, SEX,
#    INCOME) VALUES ('Ramya', 'Rama priya', 27, 'F', 9000)''')
# cursor.execute('''INSERT INTO my_db(FIRST_NAME, LAST_NAME, AGE, SEX,
#    INCOME) VALUES ('Vinay', 'Battacharya', 20, 'M', 6000)''')
# cursor.execute('''INSERT INTO my_db(FIRST_NAME, LAST_NAME, AGE, SEX,
#    INCOME) VALUES ('Sharukh', 'Sheik', 25, 'M', 8300)''')
# cursor.execute('''INSERT INTO my_db(FIRST_NAME, LAST_NAME, AGE, SEX,
#    INCOME) VALUES ('Sarmista', 'Sharma', 26, 'F', 10000)''')
# cursor.execute('''INSERT INTO my_db(FIRST_NAME, LAST_NAME, AGE, SEX,
#    INCOME) VALUES ('Tripthi', 'Mishra', 24, 'F', 6000)''')

# print("Records inserted........")

##Populating the table
# insert_stmt = "INSERT INTO my_db (FIRST_NAME, LAST_NAME, AGE, SEX, INCOME) VALUES (%s, %s, %s, %s, %s)"
# data = [('Krishna1', 'Sharma', 29, 'M', 2500), 
#    ('Raj1', 'Kandukuri', 10, 'M', 7300),
#    ('Ramya1', 'Ramapriya', 35, 'M', 5600),
#    ('Mac1', 'Mohan', 16, 'M', 2900)]
# cursor.executemany(insert_stmt, data)

# print("Records inserted Many........")

#Retrieving data
cursor.execute('''SELECT * from my_db''')



#Fetching 1st row from the table
result = cursor.fetchone()
print(result)

#Fetch a single row using the fetchone() method
# data = cursor.fetcall()
# result= [[word.strip() if type(word) == str else word for word in _] for _ in data]
# print(result)

# print("MySQL Server Version:", data)

# Close the cursor and conn
cursor.close()
conn.close()
