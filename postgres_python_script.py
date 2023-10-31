import pymysql
import psycopg2
import time


# MySQL database configuration
#establishing the conn
# MYSQL Connection 
def connect_to_mysql():
    try:
        # mysql_conn = mysql.connector.connect(
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

# PostgreSQL Database Configuration
# Function for connected with Postgres
def connected_to_postgres():
    try:
        pgsql_conn = psycopg2.connect(
            database="postgres",
            user='postgres', 
            password='priyo', 
            host='localhost', 
            port= '54320'
        )
        if pgsql_conn:
            print("Succesfully connected with postgres")
            return pgsql_conn
        else:
            print("Failed to connect with postgresSQL")
            return False
    except Exception as e:
        print("Exception found during postgres connection ", e)


    
    pgsql_conn.autocommit = True


def create_table(conn_pgsql, table_name):
    pgsql_cursor = conn_pgsql.cursor()

    try:
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name}(
            order_id INTEGER PRIMARY KEY,
            sender_id INTEGER,
            recipient_id INTEGER,
            product_id VARCHAR(8),
            price DECIMAL(10, 2),
            create_at TIMESTAMP
        )"""
        pgsql_cursor.execute(sql)
        print(f"Successfully Created table: {table_name}")
    except Exception as e:
        print("Exception found during creation", e)


def replicate_data_to_postgresql():
    
    print("replicated_data")
    while True:
        mysql_cursor = conn_mysql.cursor()

        pgsql_cursor = conn_pgsql.cursor()

        mysql_cursor.execute('SELECT * FROM Orders')
        data = mysql_cursor.fetchall()
        print(data)
        print("replicate")
     
    

        


       ## Clear existing data in PostgreSQL
        # pgsql_cursor.execute("DELETE FROM Orders")
        # formatted_data = []
        for row in data:
            ##formated data
            order_id, sender_id, recipient_id, product_id, price, create_at = row
            formatted_row = (order_id, sender_id, recipient_id, product_id, '{:.2f}'.format(price), create_at.strftime('%Y-%m-%d %H:%M:%S'))
            

            try:
               
                pgsql_cursor.execute("""INSERT INTO Orders (order_id, sender_id, recipient_id, product_id, price, create_at) VALUES (%s, %s, %s, %s, %s,%s)ON CONFLICT (order_id) DO NOTHING""", formatted_row)
            except psycopg2.Error as e:
                print("Error inserting data:", e)
                conn_pgsql.rollback()  # Rollback the transaction in case of an error
            else:
                conn_pgsql.commit()  # Commit the transaction if there are no errors

        pgsql_cursor.execute("SELECT * FROM Orders")
        all_data = pgsql_cursor.fetchall()
        print(all_data)
        print("replicate_data")

        time.sleep(180) 




if __name__ =='__main__':
    conn_pgsql=connected_to_postgres()
    conn_mysql=connect_to_mysql()
    create_table(conn_pgsql ,'Orders')
    replicate_data_to_postgresql()


