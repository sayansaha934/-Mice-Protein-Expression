from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = dataTransform()
        self.dBOperation = dBOperation()
        self.dBOperation.createDatabasesForTraining(['training_logs', 'training_dataset'])
        self.db='training_logs'
        self.table_name='Training_Main_Log'
        self.log_writer = logger.App_Logger()
        self.log_writer.createTableForLogging(self.db, self.table_name)

    def train_validation(self):
        try:
            self.log_writer.log(self.db, self.table_name, 'INFO', 'Start of Validation on files for prediction!!')
            # extracting values from training schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of training files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.db, self.table_name, 'INFO', "Raw Data Validation Complete!!")

            self.log_writer.log(self.db, self.table_name, 'INFO', "Starting Data Transforamtion!!")

            #adding quotation to categorical values to insert in table
            self.dataTransform.addQuotesToStringValuesInColumn()

            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()


            self.log_writer.log(self.db, self.table_name, 'INFO', "DataTransformation Completed!!!")

            self.log_writer.log(self.db, self.table_name, 'INFO',
                                "Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('good_raw_data', column_names)
            self.log_writer.log(self.db, self.table_name, 'INFO', "Table creation Completed!!")
            self.log_writer.log(self.db, self.table_name, 'INFO', "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('good_raw_data')
            self.log_writer.log(self.db, self.table_name, 'INFO', "Insertion in Table completed!!!")
            self.log_writer.log(self.db, self.table_name, 'INFO', "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.db, self.table_name, 'INFO', "Good_Data folder deleted!!!")
            self.log_writer.log(self.db, self.table_name, 'INFO', "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.db, self.table_name, 'INFO', "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.db, self.table_name, 'INFO', "Validation Operation completed!!")
            self.log_writer.log(self.db, self.table_name, 'INFO', "Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('good_raw_data')

        except Exception as e:
            raise e









