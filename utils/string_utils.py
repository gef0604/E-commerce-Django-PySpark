def fulfill_minutes_or_seconds(s):
    s = '0' + s if len(s) == 1 else s
    return s

def get_value_from_session_user_aggr_info(field_name, s):
    index_map = {'session_id' : 0,
                 'search_keywords' : 1,
                 'click_categories' : 2,
                 'visit_length' : 3,
                 'step_length' : 4,
                 'start_time' : 5,
                 'age' : 6,
                 'professional' : 7,
                 'sex' : 8,
                 'city' : 9}

    return s.split('|')[index_map[field_name]].split('=')[1]

