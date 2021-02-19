import datetime
from collections import namedtuple
import json
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
from utils.spark_utils import insertHive
from pyspark.accumulators import Accumulator
def print_tuple(x):
    tuple_list = []
    for item in x[1]:
        tuple_list.append(item)
    print((x[0], tuple_list))

def print_rdd(x):
    print(x)

def requirement1():
    sparkSession = (SparkSession
                        .builder
                        .appName('example-pyspark-read-and-write-from-hive')
                        .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                        .enableHiveSupport()
                        .getOrCreate()
                        )
    user_action_rdd = sparkSession.sql("select * from user_visit_action_table").rdd

    session_to_action_rdd = user_action_rdd.map(lambda x: (x.session_id, x))
    session_to_action_group = session_to_action_rdd.groupByKey()

    session_to_action_group.foreach(print_tuple)

    sparkSession.stop()

# requirement1()
# a = 1 + 3 if 1 < 2 \
#     and 2 < 3 else 2
#
# print(a)
def time_earlier(time1, time2):
    # time1 = '2021-01-27 4:34:30'
    # time2 = '2021-01-27 4:35:06'

    time1 = get_time_tuple(time1)
    time2 = get_time_tuple(time2)

    time1 = datetime.datetime(int(time1[0]), int(time1[1]), int(time1[2]), int(time1[3]), int(time1[4]), int(time1[5]))
    time2 = datetime.datetime(int(time2[0]), int(time2[1]), int(time2[2]), int(time2[3]), int(time2[4]), int(time2[5]))

    return time1 < time2

def get_time_tuple(time):
    time_date =  time.split(' ')[0].split('-')
    time_hour_to_second = time.split(' ')[1].split(':')

    return (time_date[0], time_date[1], time_date[2], time_hour_to_second[0], time_hour_to_second[1], time_hour_to_second[2])

def get_datetime_minus_second(time1, time2):
    d1 = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S')
    d2 = datetime.datetime.strptime(time2, '%Y-%m-%d %H:%M:%S')

    delta = d2 - d1
    return delta.seconds

# print(get_datetime_minus_second('2021-01-27 4:34:30','2021-01-27 4:35:06'))
# print(1 >= 1)
# res = [i * 300 for i in range(20)]
# print(res)
# a = {'one' : 1, 'two' : 2}
# b = {'one' : 1, 'two' : 2}
# print(a ==b)
#
# def a():
#     def b():
#         print(c)
#     c = 5
#     b()
# a()