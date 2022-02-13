from pyspark.sql.types import StructType, StructField, ArrayType, LongType, DoubleType
from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, explode, col
import json
import requests
import time
# import asyncio
# import aiohttp
import sys

TINY_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/tiny"
SMALL_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/small"
MEDIUM_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/medium"
LARGE_URL = "http://tortues.ecoquery.os.univ-lyon1.fr/race/large"

WAIT_TIME = 1.5

# spark = SparkSession.builder.appName(
#     "Tortoise Prediction").master('local').getOrCreate()

# course1 = spark.read.json("data/tiny/course-513.json")
# course2 = spark.read.json("data/tiny/course-514.json")
# course3 = spark.read.json("data/tiny/course-515.json")


# file_paths = glob("data/tiny/course-*.json")
# file_paths.sort()
# print(file_paths)


# course1.show()
# course2.show()
# course3.show()

# base_schema = StructType([
#     StructField("qualite", DoubleType(), True),
#     StructField("temperature", DoubleType(), True),
#     StructField("tortoises",
#                 ArrayType(
#                     StructType([
#                         StructField("id", LongType(), True),
#                         StructField("position", LongType(), True),
#                         StructField("top", LongType(), True)
#                     ])
#                 ),
#                 True
#                 )
# ]
# )

# desired_schema = StructType([
#     StructField("qualite", DoubleType(), True),
#     StructField("temperature", DoubleType(), True),
#     StructField("id", LongType(), True),
#     StructField("position", LongType(), True),
#     StructField("top", LongType(), True)
# ]
# )


# async def get_data_from_url(url):
#     return await requests.get(url).json()


# def explode_tortoises(df):
#     return df.select("qualite", "temperature", explode(df.tortoises)) \
#         .withColumn("id", col("col.id")) \
#         .withColumn("position", col("col.position")) \
#         .withColumn("top", col("col.top")) \
#         .drop("col")


# def json2DF(json_dict, schema):
#     dumped_json_string_list = [json.dumps(json_dict)]

#     return spark.read.json(
#         spark.sparkContext.parallelize(dumped_json_string_list),
#         schema
#     )


# async def extract_and_transform_from_url(url):
#     df = spark.createDataFrame([], desired_schema)

#     while True:
#         response_data = await get_data_from_url(url)

#         # await asyncio.sleep(WAIT_TIME)

#         print(response_data)

#         new_df = json2DF(response_data, base_schema)

#         new_df = new_df.filter(col("tortoises.id") == 1)

#         new_df = explode_tortoises(new_df)

#         df = df.union(new_df)

#         # df = df.sort("id", "top")

#         df.show()


def extract_transform():

    data = requests.get(TINY_URL).json()

    for i in range(len(data['tortoises'])):
        data['tortoises'][i]['delta'] = [{'top': data['tortoises'][i]['top'], 'value': }]

    print(data['tortoises'])

    while True:
        current_data = requests.get(TINY_URL).json()

        time.sleep(WAIT_TIME)

        for i in range(len(current_data['tortoises'])):
            data['tortoises'][i]['delta'].append(current_data['tortoises'][i]['position'] - data['tortoises'][i]['position'])



# def diaf():
#     simple_schema = StructType([
#         StructField("id", LongType(), True),
#         StructField("position", LongType(), True),
#         StructField("top", LongType(), True)
#     ]
#     )

#     files = glob("data/tiny/course-51*")
#     df = spark.createDataFrame([], simple_schema)
#     for file in files:
#         new_df = spark.read.json(file)

#         new_df = new_df.select(explode("tortoises")) \
#             .withColumn("id", col("col.id")) \
#             .withColumn("position", col("col.position")) \
#             .withColumn("top", col("col.top")) \
#             .drop("col")

#         new_df = new_df.filter(new_df.id == 1)

#         df = df.union(new_df)

#     df = df.sort("top")
#     df.show()

#     return df


# diaf()

# asyncio.run(extract_and_transform_from_url(TINY_URL))

# async def print_something(something):
#     print(something)


# async def get_data(url):
#     async with aiohttp.ClientSession() as session:

#         async with session.get(url) as response_data:
#             await asyncio.sleep(5)
#             data = await response_data.json()
#             print(data)

#     return data


# async def main():
#     task1 = asyncio.create_task(print_something("DIAF"))
#     task3 = asyncio.create_task(get_data(TINY_URL))
#     task2 = asyncio.create_task(print_something("BRO"))

#     await task1
#     await task3
#     await task2

# asyncio.run(main())

extract_transform()
