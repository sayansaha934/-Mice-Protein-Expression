from datetime import datetime
from os import listdir
import pandas as pd
from application_logging.logger import App_Logger


class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                  Written By: iNeuron Intelligence
                  Version: 1.0
                  Revisions: None

                  """

     def __init__(self):
          self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
          self.logger = App_Logger()

     def replaceMissingWithNull(self):
          """
                                           Method Name: replaceMissingWithNull
                                           Description: This method replaces the missing values in columns with "NULL" to
                                                        store in the table. We are using substring in the first column to
                                                        keep only "Integer" data for ease up the loading.
                                                        This column is anyways going to be removed during training.

                                            Written By: iNeuron Intelligence
                                           Version: 1.0
                                           Revisions: None

                                                   """

          db = 'prediction_logs'
          table_name = 'dataTransformLog'
          self.logger.createTableForLogging(db, table_name)
          try:
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    data = pd.read_csv(self.goodDataPath + "/" + file)
                    data.fillna('NULL', inplace=True)
                    data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                    self.logger.log(db, table_name, 'INFO', "  %s: File Transformed successfully!!" % file)
          except Exception as e:
               self.logger.log(db, table_name, 'ERROR', "Data Transformation failed because:: %s" % e)
               raise e



     def addQuotesToStringValuesInColumn(self):

          """
                                  Method Name: addQuotesToStringValuesInColumn
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.

                                   Written By: Sayan Saha
                                  Version: 1.0
                                  Revisions: None

                                          """

          db = 'prediction_logs'
          table_name = 'addQuotesToStringValuesInColumn'
          self.logger.createTableForLogging(db, table_name)
          try:
               column = ['MouseID', 'Genotype', 'Treatment', 'Behavior', 'class']

               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    data = pd.read_csv(self.goodDataPath + "/" + file)
                    # list of columns with string datatype variables

                    for col in data.columns:
                         if col in column:  # add quotes in string value
                              data[col] = data[col].apply(lambda x: "'" + str(x) + "'" if not pd.isna(x) else x)

                    data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                    self.logger.log(db, table_name, 'INFO', " %s: Quotes added successfully!!" % file)
          except Exception as e:
               self.logger.log(db, table_name, 'ERROR', "Data Transformation failed because:: %s" % e)
               raise e
