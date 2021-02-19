from django.shortcuts import render
from django.http import HttpResponse
from django.utils import timezone
from mock.models import Test
from datetime import date
import uuid
import random
from utils import string_utils
from utils.data_model import user_visit_action, user_info, product_info
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
from utils.spark_utils import insertHive

def index(request):
    return HttpResponse("This is the module for generating the mocking data")
# Create your views here.

def create_test_record(request):
    test = Test(question_text='Hello Django', pub_date=timezone.now())
    test.save()
    return HttpResponse(test.question_text)

"""
include:
    create mock data(user action, user info, product info)
    requirement 1: calculate the visit duration, step length of filtered user session data, with user info
    requirement 2: sampling the filtered data, like pick 100 out of the filtered data 
"""


"""
create the mock user action data, user info data and product info data.
"""
def create_mock_data(request):
    # build spark client
    sparkSession = (SparkSession
                    .builder
                    .appName('example-pyspark-read-and-write-from-hive')
                    .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                    .enableHiveSupport()
                    .getOrCreate()
                    )



    # genetate data for 3 tables
    user_action_data = create_user_action_records()
    user_info_data = create_user_info_records()
    product_data = create_product_info()

    # convert all the data into spark dataframe
    user_action_df = sparkSession.createDataFrame(user_action_data)
    user_info_df = sparkSession.createDataFrame(user_info_data)
    product_df = sparkSession.createDataFrame(product_data)

    # insert hive
    insertHive(sparkSession, 'USER_VISIT_ACTION_TABLE', user_action_df)
    insertHive(sparkSession, 'USER_INFO_TABLE', user_info_df)
    insertHive(sparkSession, 'PRODUCT_INFO_TABLE', product_df)
    # terminate sparksession
    sparkSession.stop()

    return HttpResponse('Tables Created')

# generate user action data using nape_tuple data model
def create_user_action_records():

    searchKeywords = ['Huawei Cell Phone',
                      'Lenovo Laptops',
                      'Lobsters',
                      'Napkins',
                      'Vacuum',
                      'Lamer',
                      'Machine Learning',
                      'Apple',
                      'Facial Lotions',
                      'Mugs']

    today = str(date.today())

    actions = ['search', 'click', 'order', 'pay']

    rows = []

    # 100 user in total
    for i in range(100):
        # print('user: ' + str(i))
        userid = random.randint(1, 100)

        # 10 sessions per user
        for j in range(10):
            # unique session id, used for the key in the database
            sessionid = uuid.uuid1().__str__().replace('-', '')

            # add a random hour
            base_action_time = today + " " + str(random.randint(0,23))

            # random 1 - 100 user actions per sessions
            for k in range(random.randint(20, 100)):
                # print('number ' + str(k) + ' action')
                pageid = random.randint(1, 100)

                # after the hour, add ramdom min and seconds
                minute = string_utils.fulfill_minutes_or_seconds(str(random.randint(0, 59)))
                second = string_utils.fulfill_minutes_or_seconds(str(random.randint(0, 59)))
                action_time = base_action_time + ':' + minute + ':' + second

                search_key_word = None
                click_category_id = -1
                click_product_id = -1
                order_category_ids = None
                order_product_ids = None
                pay_category_ids = None
                pay_product_ids = None
                city_id = random.randint(0, 10)
                action = actions[random.randint(0, 3)]

                if action == 'search':
                    search_key_word = searchKeywords[random.randint(0, 9)]
                elif action == 'click':
                    click_category_id = random.randint(0, 100)
                    click_product_id = random.randint(0, 100)
                elif action == 'order':
                    order_category_ids = str(random.randint(0, 100))
                    order_product_ids = str(random.randint(0, 100))
                elif action == 'pay':
                    pay_category_ids = random.randint(0, 100)
                    pay_product_ids = random.randint(0, 100)

                rows.append(user_visit_action(today,
                                              userid,
                                              sessionid,
                                              pageid,
                                              action_time,
                                              search_key_word,
                                              click_category_id,
                                              click_product_id,
                                              order_category_ids,
                                              order_product_ids,
                                              pay_category_ids,
                                              pay_product_ids,
                                              city_id))
    return rows

# generate userinfo data
def create_user_info_records():
    rows = []
    sexes = ['Male', 'Female']

    # create 100 users
    for i in range(100):
        userid = i
        username = 'user' + str(i)
        name = 'name' + str(i)
        age = random.randint(10, 60)
        professional = "professional" + str(random.randint(0, 100))
        city = "city" + str(random.randint(1, 100))
        sex = sexes[random.randint(0, 1)]

        rows.append(user_info(userid,
                              username,
                              name,
                              age,
                              professional,
                              city,
                              sex))

    return rows

# generate product info data
def create_product_info():
    rows = []
    product_status = [0 ,1]

    # generate 100 product
    for i in range(100):
        product_id = i
        product_name = 'product' + str(i)
        extend_info = '{"product_status":' + str(product_status[random.randint(0, 1)])

        rows.append(product_info(product_id, product_name, extend_info))

    return rows