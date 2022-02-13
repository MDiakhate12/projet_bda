import json
import os

# from pyspark.sql import SparkSession

import requests

tiny = "http://tortues.ecoquery.os.univ-lyon1.fr/race/tiny"
small = "http://tortues.ecoquery.os.univ-lyon1.fr/race/small"
medium = "http://tortues.ecoquery.os.univ-lyon1.fr/race/medium"
large = "http://tortues.ecoquery.os.univ-lyon1.fr/race/large"

HDFS_ROOT = "hdfs:///user/p2112974"
WAIT_TIME = 1.5

import time

# spark = SparkSession.builder.appName("Tortoise Prediction").master('local').getOrCreate()

def save_to_hdfs(hdfs_url, data, filename):
  data.write.json(f"{hdfs_url}/project_data/{filename}")

def get_data_from_url(url):
    return requests.get(url).json()

def collect_data_to_hdfs():
    while True:
        r_previous = get_data_from_url(tiny)
        time.sleep(WAIT_TIME)
        r_current = get_data_from_url(tiny)

        if(r_previous == r_current):
            save_to_hdfs(HDFS_ROOT, r_previous, f"course-{r_previous.top}")
            
        save_to_hdfs(HDFS_ROOT, r_current, f"course-{r_current.top}")

def collect_data_to_local(url):
    dirname = url.split('/')[-1]

    try:
        os.mkdir(f"data/{dirname}")
    except OSError as error:
        print(error)

    while True:
        r_previous = get_data_from_url(url)
        time.sleep(WAIT_TIME)
        r_current = get_data_from_url(url)

        if(r_previous == r_current):
            print(r_previous)
            with open(f"data/{dirname}/course-{r_previous['tortoises'][0]['top']}.json", "w") as file:
                file.write(json.dumps(r_previous))
                
        with open(f"data/{dirname}/course-{r_current['tortoises'][0]['top']}.json", "w") as file:
            file.write(json.dumps(r_current))

#print(get_data_from_url(tiny))

collect_data_to_local(large)
    