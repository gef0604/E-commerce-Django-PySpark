import datetime
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