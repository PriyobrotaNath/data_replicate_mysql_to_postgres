import pymysql
import time
import datetime
import random
from faker import Faker

# MySQL database configuration
def connect_to_mysql():
    try:
        mysql_conn = pymysql.connect(
        database="my_db",
            user='root', 
            password='mysql123', 
            host='127.0.0.1', 
            port= 3306
        )
        if mysql_conn:
            print("Sucessfully connected with MySQL")
            return mysql_conn
        else:
            print("Failed to connected with MySQL")
            return False
    except Exception as e:
        print("Exception found during mysql connection: ", e)
   
    mysql_conn.autocommit = True


fake = Faker()


def create_table(conn_mysql ,table_name):
    conn_mysql = connect_to_mysql()
    conn_mysql.cursor()
    
    try:
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name}(
            order_id INT(5) PRIMARY KEY ,
            sender_id INT(5),
            receipient_id INT(5),
            product_id VARCHAR(8),
            price DECIMAL(10,2),
            create_at TIMESTAMP
        )"""
        conn_mysql.execute(sql)
        print(f"Successfully Created table: {table_name}")

        pass
    except Exception as e:
        print("Exception found during creation ", e)

def create_and_populate_orders_table(mysql_conn, data):   
    mysql_cursor = mysql_conn.cursor()
    try:
        print("Data: ", data)
        sql_query = """INSERT IGNORE INTO Orders (order_id, sender_id, receipient_id, product_id, price, create_at) VALUES (%s, %s, %s, %s, %s, %s)"""
        # sql_query = """INSERT INTO Orders (order_id, sender_id, receipient_id, product_id, price, create_at) VALUES (%s, %s, %s, %s, %s, %s)"""
        mysql_cursor.executemany(sql_query, data)
        mysql_conn.commit()
        # print("Data inserted successfully")
    except Exception as e:
        print(f"Exception found during insertion: {e}")

def generate_random_timestamp():
    start_date = datetime.datetime(2023, 10, 22)
    end_date = datetime.datetime(2023, 10, 26, 23, 59, 59)
    random_date = start_date + datetime.timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    formatted_timestamp = random_date.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_timestamp
        

 
if __name__=="__main__":
    conn=connect_to_mysql()
    #create_table(mysql_conn, "Orders")
    # Randomy data generate, data stored
    while True:
        for _ in range(3):
            order_id = random.randint(2,500)
            sender_id =random.randint(0,1000)
            receipient_id =random.randint(0,1000)
            product_id = str(random.randint(10000000, 99999999))
            price = round(random.uniform(10.0, 100.0), 2)
            create_at = generate_random_timestamp()
            
            data = [
                (order_id, sender_id, receipient_id, product_id, price, create_at)
        ]
        # for _ in range(3):
        #     data = [  
        #         (random.randint(2,500),
        #         random.randint(0,1000),
        #         random.randint(0,1000),
        #         str(random.randint(10000000, 99999999)),
        #         round(random.uniform(10.0, 100.0), 2),
        #         datetime.datetime(2023, 10, random.randint(22, 26), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        #         )
        #     ]
        #p=random.randint(1,10000)
        # data = [
        #         (p+i, 10, 20, 'ABC123', 100.00, '2023-10-27 03:21:50'),
        #         (p+1, 11, 21, 'DEF456', 200.00, '2023-10-27 03:22:00'),
        #         (p+2, 12, 22, 'GHI789', 300.00, '2023-10-27 03:22:10')
        #     ]
        
            create_and_populate_orders_table(conn, data)
        time.sleep(60)
     

 