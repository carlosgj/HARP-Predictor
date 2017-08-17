from django.conf.urls import url

from . import views

urlpatterns=[
    url(r'^$', views.index, name='index'),
    url(r'^list\/?$', views.PredictionList, name='list'),
    url(r'^map/(?P<pred_id>[0-9]+)\/?$', views.Map, name='map'),
    url(r'^multiMap\/?$', views.MultiMap, name='multimap'),
    url(r'^addLaunchPoint\/?$', views.addLaunchPoint, name="addLaunchPoint"),
]