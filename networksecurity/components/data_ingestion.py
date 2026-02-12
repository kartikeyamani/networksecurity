from networksecurity.logging import logger
from networksecurity.exception.exception import NetworkSecurityException

from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
import os
import sys
import pymongo
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
load_dotenv()

MONGO_DB_URL=os.getenv("MONGO_DB_URL")

class DataIngestion:
    def __init__(self,data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestionconfig=data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    def export_collection_as_dataframe(self):
        try:
            database_name=self.data_ingestionconfig.database_name
            collection_name=self.data_ingestionconfig.collection_name
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            collection=self.mongo_client[database_name][collection_name]
            df=pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df=df.drop(columns=["_id"],axis=1)
            
            df.replace({"na":np.nan},inplace=True)
            return df
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    def export_data_into_feature_store(self,dataframe:pd.DataFrame):
        try:
            feature_store_file_path=self.data_ingestionconfig.feature_store_file_path
            dir_path=os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    def split_data_as_train_and_test(self,dataframe:pd.DataFrame):

        try:
            train_set,test_set=train_test_split(dataframe,test_size=self.data_ingestionconfig.train_test_split_ratio)
            

            logger.logging.info("Performed train test split on the dataframe")

            logger.logging.info(
                    "Exited split_data_as_train_test method of Data_Ingestion class"
                )
            dirname=os.path.dirname(self.data_ingestionconfig.training_file_path)
            os.makedirs(dirname,exist_ok=True)
            logger.logging.info("Initiated train test split")
            train_set.to_csv(
                self.data_ingestionconfig.training_file_path,index=False,header=True
            )
            test_set.to_csv(
                self.data_ingestionconfig.testing_file_path,index=False,header=True
            )
            logger.logging.info("Train test split files are created properly")
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
        
    def initiate_data_ingestion(self):
        try:    
            dataframe=self.export_collection_as_dataframe()
            dataframe=self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_and_test(dataframe)
            dataingestion_artifact=DataIngestionArtifact(
                trained_file_path=self.data_ingestionconfig.training_file_path,
                test_file_path=self.data_ingestionconfig.testing_file_path
                )
            return dataingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)









