"""
requirement 1
"""
import json

from pyspark import AccumulatorParam

from properties import filter_config
from utils.string_utils import get_value_from_session_user_aggr_info
from utils.time_utils import time_earlier, get_datetime_minus_second

"""
for the (sessionid, iterable), change into (userid, aggr_session_data), to join with user info table
"""
"""
accumulator which maintain a hashmap
"""
class session_accumulator_param(AccumulatorParam):
    """
    maintain a countmap, to count the session steps
    """
    def zero(self, value):

        return '{}'

    def addInPlace(self, value1, value2):
        value1 = json.loads(value1)

        if value2 in value1.keys():
            value1[value2] = value1[value2] + 1

        else:
            value1[value2] = 1

        return json.dumps(value1, sort_keys=True)

def get_userid_to_info(x):
    user_id = -1
    start_time = None
    end_time = None
    step = 0
    search_keywords = ''
    click_categories = ''
    session_id, action_infos = x
    aggr_info = ''

    for action in action_infos:
        user_id = action.user_id if user_id == -1 else user_id

        step = step + 1

        # if search kw not in and not none
        search_keywords = search_keywords + action.search_keyword + ',' if action.search_keyword != None and action.search_keyword not in search_keywords else search_keywords

        click_categories = click_categories + str(
            action.click_category_id) + ',' if action.click_category_id != -1 and str(
            action.click_category_id) not in click_categories else click_categories

        # deal with time
        start_time = action.action_time if start_time == None or time_earlier(action.action_time,
                                                                              start_time) else start_time
        end_time = action.action_time if end_time == None or time_earlier(end_time, action.action_time) else end_time

    # trim the last ',' of click_categories id and search keywords
    search_keywords = search_keywords[0: len(search_keywords) - 1] if search_keywords.endswith(',') else search_keywords
    click_categories = click_categories[0: len(click_categories) - 1] if click_categories.endswith(
        ',') else click_categories

    visit_length = get_datetime_minus_second(start_time, end_time)
    aggr_info = 'session_id=' + session_id + '|search_keywords=' + search_keywords + '|click_categories=' + click_categories + '|visit_length=' + str(
        visit_length) + '|step_length=' + str(step) + '|start_time=' + start_time
    return (user_id, aggr_info)


def get_session_id_from_aggr_info_string(x):
    return x.split('|')[0].split('=')[1]


def aggr_user_and_session(x):
    session_info, user_info = x[1]

    age = user_info.age
    professional = user_info.professional
    sex = user_info.sex
    city = user_info.city

    full_info = session_info + '|age=' + str(age) + '|professional=' + professional + '|sex=' + sex + '|city=' + city
    session_id = get_session_id_from_aggr_info_string(session_info)
    return (session_id, full_info)

def get_acc_visit_length_key(visit_length):
    visit_range = [i * 300 for i in range(25)]
    if visit_length > visit_range[len(visit_range) - 1]:
        return 'visit_length > 7200'
    else:
        for i in range(len(visit_range)):
            if visit_length <= visit_range[i]:
                return 'visit_' + str(visit_range[i - 1]) + '_' + str(visit_range[i])


def get_acc_step_length_key(step_length):
    step_range = [i * 5 for i in range(41)]
    if step_length > step_range[len(step_range) - 1]:
        return 'step_length > 200'
    else:
        for i in range(len(step_range)):
            if step_length <= step_range[i]:
                return 'step_' + str(step_range[i - 1]) + '_' + str(step_range[i])


# so far just take the age
def filter_rdd_by_param(x, accumulator):
    param = filter_config.filter_param
    age = get_value_from_session_user_aggr_info('age', x[1])
    visit_length = int(get_value_from_session_user_aggr_info('visit_length', x[1]))
    step_length = int(get_value_from_session_user_aggr_info('step_length', x[1]))
    param = filter_config.filter_param

    visit_length_key = get_acc_visit_length_key(visit_length)
    step_length_key = get_acc_step_length_key(step_length)

    # modify accumulator
    if int(age) >= param['start_age'] and int(age) <= param['end_age']:
        accumulator.add('sum_visit')
        accumulator.add('sum_step')
        accumulator.add(visit_length_key)
        accumulator.add(step_length_key)

    return int(age) >= param['start_age'] and int(age) <= param['end_age']


def get_initialed_count_accumulator(sc):
    acc = sc.accumulator('initial', session_accumulator_param())
    acc.value = json.dumps({}, sort_keys=True)
    return acc


"""
requirement 1 ends
"""

