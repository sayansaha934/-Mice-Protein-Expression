import mysql.connector as connection



class App_Logger:
    def __init__(self):
        pass


    def createTableForLogging(self, db, table_name):
        mydb = connection.connect(host="localhost", database=db, user="root", password="password", use_pure=True)

        try:
            query=f'SELECT * FROM {table_name} LIMIT 1;'
            cursor = mydb.cursor()
            cursor.execute(query)
        except:
            query=f'CREATE TABLE {table_name} (log_time DATETIME DEFAULT CURRENT_TIMESTAMP , tag text, log_message text);'
            cursor = mydb.cursor()
            cursor.execute(query)

    def log(self, db, table_name, tag, log_message):
        mydb = connection.connect(host="localhost", database=db, user="root", password="password", use_pure=True)
        query = f'INSERT INTO {table_name} (tag, log_message) VALUES ("{tag}", "{log_message}");'
        cursor = mydb.cursor()
        cursor.execute(query)
        mydb.commit()
