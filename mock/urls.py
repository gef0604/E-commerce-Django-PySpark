from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('create_test_record/', views.create_test_record, name='create_test_record'),
    # path('create_user_action_records/', views.create_user_action_records, name='create_user_action_records')
    path('create_mock_data/', views.create_mock_data, name = 'create_mock_data'),
]