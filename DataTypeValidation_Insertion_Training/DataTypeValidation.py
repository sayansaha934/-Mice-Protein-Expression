import shutil
from os import listdir
import os
import csv
import pandas as pd
import mysql.connector as connection
from application_logging.logger import App_Logger


class dBOperation:
    """
      This class shall be used for handling all the SQL operations.

      Written By: Sayan Saha
      Version: 1.0
      Revisions: None

      """
    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()


    def createDatabasesForTraining(self, Databases):

        """
                Method Name: createDatabaseForTraining
                Description: This method created databases for training operation i.e  logging and stroring data in database
                Output: None
                On Failure: Exception

                Written By: Sayan Saha
                Version: 1.0
                Revisions: None

        """
        try:
            conn=connection.connect(host="localhost", user="root", password="password",use_pure=True)
            for db in Databases:
                query=f'CREATE DATABASE IF NOT EXISTS {db}'
                cursor=conn.cursor()
                cursor.execute(query)
            conn.close()
        except Exception as e:
            raise e


    def dataBaseConnection(self,DatabaseName):

        """
                Method Name: dataBaseConnection
                Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
                Output: Connection to the DB
                On Failure: Raise ConnectionError

                 Written By: Sayan Saha
                Version: 1.0
                Revisions: None

                """
        logging_db='training_logs'
        logging_tableName='DataBaseConnectionLog'
        self.logger.createTableForLogging(logging_db, logging_tableName)
        try:
            conn = connection.connect(host="localhost", database=DatabaseName, user="root", password="password", use_pure=True)
            self.logger.log(logging_db, logging_tableName, 'INFO', "Opened %s database successfully" % DatabaseName)
        except Exception as e:
            self.logger.log(DatabaseName, logging_tableName, 'ERROR', "Error while connecting to database: %s" %e)
            raise e
        return conn

    def createTableDb(self,TableName,column_names):
        """
                        Method Name: createTableDb
                        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                        Output: None
                        On Failure: Raise Exception

                         Written By: Sayan Saha
                        Version: 1.0
                        Revisions: None

                        """
        logging_db='training_logs'
        dataset_db='training_dataset'
        logging_tableName='DbTableCreateLog'
        self.logger.createTableForLogging(logging_db, logging_tableName)

        try:

            conn=self.dataBaseConnection(dataset_db)
            query=f'SHOW TABLES IN {dataset_db}'
            cursor = conn.cursor()
            cursor.execute(query)
            present_tables=[i[0] for i in cursor.fetchall()]

            if TableName in present_tables:

                self.logger.log(logging_db, logging_tableName,'INFO' , "Tables created already!!")


                conn.close()
                self.logger.log(logging_db, 'DataBaseConnectionLog', 'INFO', "Closed %s database successfully" % dataset_db)

            else:
                for column_name in column_names.keys():
                    dataType = column_names[column_name]

                    #in try block we check if the table exists, if yes then add columns to the table
                    # else in catch block we will create the table
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f'ALTER TABLE {TableName} ADD COLUMN {column_name} {dataType};')
                    except:
                        cursor = conn.cursor()
                        cursor.execute(f'CREATE TABLE  {TableName} ({column_name} {dataType} );')
                self.logger.log(logging_db, logging_tableName, 'INFO', "Tables created successfully!!")

                conn.close()
                self.logger.log(logging_db,'DataBaseConnectionLog', 'INFO', "Closed %s database successfully" % dataset_db)



        except Exception as e:
            self.logger.log(logging_db, logging_tableName, 'ERROR', "Error while creating table: %s " % e)

            raise e


    def insertIntoTableGoodData(self,TableName):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception

                                Written By: Sayan Saha
                               Version: 1.0
                               Revisions: None

        """
        dataset_db='training_dataset'
        logging_db='training_logs'
        logging_tableName='DbInsertLog'

        conn = self.dataBaseConnection(dataset_db)
        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        self.logger.createTableForLogging(logging_db, logging_tableName)

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in reader:
                        try:
                            cursor=conn.cursor()
                            cursor.execute(f'INSERT INTO {TableName}  VALUES ({line[0]});')
                            conn.commit()
                        except Exception as e:
                            raise e
                    self.logger.log(logging_db, logging_tableName, 'INFO', " %s: File loaded successfully!!" % file)

            except Exception as e:

                self.logger.log(logging_db, logging_tableName, 'ERROR', "Error while inserting into table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(logging_db, logging_tableName, 'ERROR', "File Moved to Training_Raw_files_validated/Bad_Raw Successfully %s" % file)

        conn.close()


    def selectingDatafromtableintocsv(self, TableName):

        """
                               Method Name: selectingDatafromtableintocsv
                               Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                            above created .
                               Output: None
                               On Failure: Raise Exception

                                Written By: Sayan Saha
                               Version: 1.0
                               Revisions: None

        """
        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        dataset_db='training_dataset'
        logging_db='training_logs'
        logging_tableName='ExportToCsv'
        self.logger.createTableForLogging(logging_db, logging_tableName)
        try:

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            conn=self.dataBaseConnection(dataset_db)
            query=f'SELECT * FROM {TableName};'
            df=pd.read_sql(query, conn)
            df.to_csv(self.fileFromDb + self.fileName, header=True, index=None)


            self.logger.log(logging_db, logging_tableName, 'INFO', "File exported successfully!!!")
            conn.close()

        except Exception as e:
            self.logger.log(logging_db, logging_tableName, 'ERROR', "File exporting failed. Error : %s" %e)
            raise e





