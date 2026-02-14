from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException

import os,sys
import yaml
import numpy as np
import pickle


def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path,"rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise NetworkSecurityException(e,sys)
def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise NetworkSecurityException(e, sys)

def save_numpy_array_data(filepath:str,arr:np.array):
    try:
        dir_path=os.path.dirname(filepath)
        os.makedirs(dir_path,exist_ok=True)
        print(filepath)
        with open(filepath,"wb") as file_obj:
            np.save(file_obj,arr)
    except Exception as e:
        raise NetworkSecurityException(e,sys)
def save_object(filepath,obj):
    try:
        dir_path=os.path.dirname(filepath)
        os.makedirs(dir_path,exist_ok=True)
        with open(filepath,"wb") as fileobj:
            pickle.dump(obj,fileobj)
    except Exception as e:
        raise NetworkSecurityException(e,sys)

