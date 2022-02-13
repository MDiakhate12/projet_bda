from curses.ascii import SP
import requests
import json

TINY_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/tiny"
SMALL_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/small"
MEDIUM_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/medium"
LARGE_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/large"

HDFS_ROOT = "hdfs:///user/p2112974"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyse Projet BDA").master("local").getOrCreate()

def etl():
    data = requests.get(TINY_URL).json()

    print(data)

a = [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]
l = [5, 3, 9, 8, 12, 30, 7, 5, 3, 9, 8, 12, 30, 7, 5, 3, 9, 8, 12, 30, 7, 5, 3, 9, 8, 12, 30, 7, 5, 3, 9, 8, 12, 30, 7,]
b = [15, 18, 13, 8, 3, 0, 5, 10, 15, 18, 13, 8, 3, 0, 5, 10, 15, 18, 13, 8, 3, 0, 5, 10, 15, 18, 13, 8, 3, 0, 5, 10, 15, 18,]

def check_reguliere(deltas):
    if(deltas[0] == deltas[1]):
        return True, deltas[0]

    return False

def check_cyclique(deltas):
    repetition_index = deltas[1:].index(deltas[0]) + 1

    if (deltas[repetition_index + 1] == deltas[1]) & (deltas[repetition_index + 2] == deltas[2] & repetition_index > 1):
        return True, repetition_index , deltas[:repetition_index]
    return False

def check_fatiguee(deltas): 
    initial_speed = max(deltas)
    initial_speed_index = deltas.index(initial_speed)

    if (deltas[initial_speed_index + 1] - deltas[initial_speed_index] ==  deltas[initial_speed_index + 2] - deltas[initial_speed_index + 1]) & (deltas[initial_speed_index] != deltas[initial_speed_index + 1]):
        return True, initial_speed , deltas[initial_speed_index + 1] - deltas[initial_speed_index]
    return False


# with open("./collecte.json", 'r') as f:
#     a = json.loads(f.read())

#     print(check_fatiguee(a[1]['delta']))

# print(check_reguliere(l))
# print(check_cyclique(a))
# print(check_fatiguee(l))


# tiny = spark.read.json("data/tiny/*")

# tiny.write.json(f"{HDFS_ROOT}/project_data/tiny")

spark.read.csv(f"{HDFS_ROOT}/users.json")