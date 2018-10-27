from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from .models import Prediction, elevationPoint, getAltitudeAtPoint, PredictionPoint, launchLocation
from datetime import datetime, timedelta
from .forms import NewPredictionForm, NewLaunchPointForm
from django.views import generic

import MySQLdb

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
    print pred_id
    print points
    return render(request, "Predictor/map.html", {"pred":pred, 'points':points})

def MultiMap(request):
    wdb = MySQLdb.connect(user='readonly', db='test_dataset', passwd='', host='weatherdata.kf5nte.org')
    wc = wdb.cursor()
    preds = Prediction.objects.exclude(burstLatitude__isnull=True)
    launchpoints = launchLocation.objects.all()
    weatherSquares = []
    for i in range(33, 36):
        for j in range (-120, -116):
            wc.execute("""SELECT * FROM WeatherData WHERE Latitude=%f AND Longitude=%f LIMIT 1"""%(i+0.5, j+0.5))
            if len(wc.fetchall()) > 0:
                weatherSquares.append({'lat':i+0.5, 'lon':j+0.5})

    elevationSquares = []
    for i in range(32, 38):
        for j in range (-122, -114):
            result = elevationPoint.objects.using('elevationdata').filter(latitude=i+0.5)
            result = result.filter(longitude=j+0.5)
            if result:
                elevationSquares.append({'lat':i+0.5, 'lon':j+0.5})


    return render(request, "Predictor/multiMap.html", {"preds":preds,'launchpoints':launchpoints,'elevData':elevationSquares, 'weatherData':weatherSquares})

def PredictionList(request):
    data = list(Prediction.objects.all().order_by('launchTime'))
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

def LaunchPointList(request):
    data = list(launchLocation.objects.all())
    context = {'lp_list':data,}
    return render(request, 'Predictor/listLaunchPoints.html', context)

def WeatherDataList(request):
    wdb = MySQLdb.connect(user='readonly', db='test_dataset', passwd='', host='weatherdata.kf5nte.org')
    wdcrs = wdb.cursor()
    wdcrs.execute("""SELECT PredictionTime FROM WeatherData GROUP BY PredictionTime""")
    rawResults = wdcrs.fetchall()
    wdb.close()
    data = []
    for i in rawResults:
        this = {}
        this["predictionTime"] = i[0]
        data.append(this)
    context = {'weather_list':data,}
    return render(request, 'Predictor/weatherList.html', context)

def WeatherDataJson(request, table_name):
    wdb = MySQLdb.connect(user='readonly', db='test_dataset', passwd='', host='weatherdata.kf5nte.org')
    wdcrs = wdb.cursor()
    print "Getting data..."
    try:
        wdcrs.execute("""SELECT ValueTime, Resolution, Latitude, Longitude, Isobar, HGT, TMP, UGRD, VGRD FROM %s"""%table_name)
    except MySQLdb.ProgrammingError, e:
        if e.args[0] == 1146:
            return HttpResponse("No such table in db!")
        else:
            raise e
    print "Fetching data..."
    rawResults = wdcrs.fetchall()
    wdb.close()
    print "Processing data..."
    rows = []
    for i in rawResults:
        this = {
        "ValueTime":    i[0],
        "Resolution":   i[1],
        "Latitude":     i[2],
        "Longitude":    i[3],
        "Isobar":       i[4],
        "HGT":          i[5],
        "TMP":          i[6],
        "UGRD":         i[7],
        "VGRD":         i[8],
        }
        rows.append(this)
    context = {'rows':rows}
    print "Returning data..."
    return JsonResponse(rows, safe=False)

def ShowWeatherData(request, table_name):
    context = {'table':table_name}
    return render(request, 'Predictor/weatherDataAnalysis.html', context)

