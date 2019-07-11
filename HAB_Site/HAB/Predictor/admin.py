from django.contrib import admin
from .models import Prediction, PredictionPoint, launchLocation, BalloonParameterSet

# Register your models here.
admin.site.register(Prediction)
admin.site.register(PredictionPoint)
#admin.site.register(Prediction, PredictionAdmin)
admin.site.register(launchLocation)
admin.site.register(BalloonParameterSet)
