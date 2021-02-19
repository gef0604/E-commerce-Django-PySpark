"""
A bunch of functions operating spark or hive database
"""
from pyspark import AccumulatorParam
import json

from op_func.requirement1 import get_initialed_count_accumulator, filter_rdd_by_param, get_userid_to_info, \
    aggr_user_and_session
from utils.time_utils import time_earlier, get_datetime_minus_second

"""
insert hive table with a dataframe
"""
def insertHive(spark_session, table_name, dataDF):
    spark_session.sql("DROP TABLE IF EXISTS " + table_name)
    dataDF.write.saveAsTable(table_name)

"""
given a user session raw data, return the filtered rdd which meet the criteria of properties.filter_config
the filtered info aggregate all the actions in a session with user info
"""
def raw_data_to_filtered_full_info(sparkSession, user_action_rdd, sc):
    # get all the records from user visit action
    # user_action_rdd = sparkSession.sql("select * from user_visit_action_table").rdd

    # change the Row to (sessionid, Row)
    session_to_action_rdd = user_action_rdd.map(lambda x: (x.session_id, x))

    # group by key, get (session_id, iterable)
    session_to_action_group = session_to_action_rdd.groupByKey()

    """
    to get a aggregate info
    cause we need to join the table with user info table, so the key will be changed to userid
    (userid : session_id | search keywords | click categories | visit length | step length | start time)
    """
    user_id_to_aggr_info_rdd = session_to_action_group.map(lambda x: get_userid_to_info(x))

    """
    to get the user info here, need to join the user table
    """
    user_info_rdd = sparkSession.sql("select * from user_info_table").rdd

    user_id_to_user_info_rdd = user_info_rdd.map(lambda x: (x.user_id, x))

    # now join
    # user_id_to_user_info_rdd.join(user_id_to_aggr_info_rdd).collect()
    session_id_to_aggr_info_and_user_info_rdd = user_id_to_aggr_info_rdd.join(user_id_to_user_info_rdd).map(lambda x: aggr_user_and_session(x))

    acc = get_initialed_count_accumulator(sc)
    session_filter_rdd = session_id_to_aggr_info_and_user_info_rdd.filter(lambda x: filter_rdd_by_param(x, acc))

    return session_filter_rdd
