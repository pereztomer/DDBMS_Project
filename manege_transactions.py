import glob
import os
import findspark
from pyspark.sql import SparkSession

# Constants
X = 11
Y = 6
Z = 59
from pyspark.sql.functions import asc
from pyspark.sql.functions import avg
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def lock_directory(directory_pass='C:/Users/Tomer/PycharmProjects/DDBMS_Project/orders/'):
    pass


def manege_transactions(T):
    dir_name = 'C:/Users/Tomer/PycharmProjects/DDBMS_Project/orders/'
    list_of_files = sorted(filter(os.path.isfile, glob.glob(dir_name + '*')))
    lock_directory()
    if len(list_of_files) == 0:
        print("Currently no queries available")
        return

    spark, sc = init_spark("manege_transactions")

    for file_path in list_of_files:
        if not file_path.endswith('_' + str(X)):
            print(f"File {file_path} not in correct format - was rejected")
        else:
            query = spark.read.csv(file_path)
            print(query.take(10))
            exit()
