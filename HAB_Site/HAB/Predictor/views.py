from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from .models import Prediction, elevationPoint, getAltitudeAtPoint, PredictionPoint, launchLocation
from datetime import datetime, timedelta
from .forms import NewPredictionForm, NewLaunchPointForm
from django.views import generic

def addLaunchPoint(request):
    #If we're getting submitted data
    if request.method == 'POST':
        form = NewLaunchPointForm(request.POST)
        if form.is_valid():
            print form.cleaned_data
            if (not form.cleaned_data['autoAltitude'] and not form.cleaned_data['altitude']):
                form._errors['altitude'] = form.error_class(["Please enter a launch altitude or select Auto."])
                return render(request, "Predictor/addLaunchPoint.html", {'form':form})
            name = form.cleaned_data['name']
            description = form.cleaned_data['description']
            latitude = form.cleaned_data['latitude']
            longitude = form.cleaned_data['longitude']
            if form.cleaned_data['autoAltitude']:
                altitude = getAltitudeAtPoint(latitude, longitude)
            else:
                altitude = form.cleaned_data['altitude']
            launchLocation.objects.create(
                name = name,
                description = description,
                latitude = latitude,
                longitude = longitude,
                altitude = altitude
            )
            return HttpResponseRedirect('/Predictor')
        else:
            return render(request, "Predictor/addLaunchPoint.html", {'form':form})

    #If someone is requesting a new form
    else:
        form = NewLaunchPointForm()
        return render(request, "Predictor/addLaunchPoint.html", {'form':form})

def index(request):
    context = {}
    return render(request, "Predictor/index.html", context)

def Map(request, pred_id):
    pred = Prediction.objects.get(pk=pred_id)
    points = PredictionPoint.objects.filter(prediction=pred).order_by('time')
    return render(request, "Predictor/map.html", {"pred":pred, 'points':points})

def MultiMap(request):
    preds = Prediction.objects.exclude(landingLatitude__isnull=True)
    return render(request, "Predictor/multiMap.html", {"preds":preds,})

def PredictionList(request):
    data = list(Prediction.objects.all())
    for pred in data:
        auxDict = {}
        if pred.burstTime:
            pred.burstElapsed = pred.burstTime - pred.launchTime
        else:
            pred.burstElapsed = None
        if pred.landingTime:
            pred.landingElapsed = pred.landingTime - pred.launchTime
        else:
            pred.landingElapsed = None
    context = {'prediction_list':data,}
    return render(request, 'Predictor/list.html', context)
