from collections import namedtuple

user_visit_action = namedtuple("user_visit_action",
                      ['date',
                       'user_id',
                       'session_id',
                       'page_id',
                       'action_time',
                       'search_keyword',
                       'click_category_id',
                       'click_product_id',
                       'order_category_ids',
                       'order_product_ids',
                       'pay_category_ids',
                       'pay_product_ids',
                       'city_id'])

user_info = namedtuple("user_info",
            ['user_id',
            'username',
            'name',
            'age',
            'professional',
            'city',
            'sex'])

product_info = namedtuple('product_info',
                ['product_id',
                'product_name',
                'extend_info'])

"""
Session steps ratio data model
"""
step_length_ratio = namedtuple('step_length_ratio',
                               ['task_id',
                                'session_count',
                                'step_0_5',
                                'step_5_10',
                                'step_10_15',
                                'step_15_20',
                                'step_20_25',
                                'step_25_30',
                                'step_30_35',
                                'step_35_40',
                                'step_40_45',
                                'step_45_50',
                                'step_50_55',
                                'step_55_60',
                                'step_60_65',
                                'step_65_70',
                                'step_70_75',
                                'step_75_80',
                                'step_80_85',
                                'step_85_90',
                                'step_90_95',
                                'step_95_100'])

visit_length_ratio = namedtuple('visit_length_ratio',
                                ['task_id',
                                 'session_count',
                                 'visit_0_300',
                                 'visit_300_600',
                                 'visit_600_900',
                                 'visit_900_1200',
                                 'visit_1200_1500',
                                 'visit_1500_1800',
                                 'visit_1800_2100',
                                 'visit_2100_2400',
                                 'visit_2400_2700',
                                 'visit_2700_3000',
                                 'visit_3000_3300',
                                 'visit_3300_3600'])

# data model for data sampling
user_action_sampling = namedtuple('user_action_sampling',
                                  ['task_id',
                                   'session_id',
                                   'start_time',
                                   'search_keywords',
                                   'click_categories'])

"""
requirement 2
"""


# used to sort the top10
class category_action_count:

    def __init__(self, click_count, order_count, pay_count):
        self.click_count = click_count
        self.order_count = order_count
        self.pay_count = pay_count

    def __eq__(self, other):
        return self.click_count == other.click_count and \
               self.order_count == other.order_count and \
               self.pay_count == other.pay_count

    def __gt__(self, other):
        if self.click_count > other.click_count:
            return True
        elif self.click_count == other.click_count and self.order_count > other.order_count:
            return True
        elif self.click_count == other.click_count and self.order_count == other.order_count and self.pay_count > other.pay_count:
            return True

top10_category = namedtuple('top10_category',
                            ['task_id',
                             'category_id',
                             'click_count',
                             'order_count',
                             'pay_count'])