from django import forms

class NewPredictionForm(forms.Form):
    latitude = forms.FloatField(label='Launch latitude')
    longitude = forms.FloatField(label='Launch longitude')
    time = forms.DateTimeField(label='Launch time')
    altitude = forms.IntegerField(label='Launch altitude (ft ASL)',required=False)
    autoAltitude = forms.BooleanField(label='Auto',required=False)
    prediction = forms.DateTimeField(label='Prediction time',required=False)
    autoPrediction = forms.BooleanField(label='Auto',required=False)
    ascentSpeed = forms.FloatField(label='Ascent speed (ft/s)')
    descentSpeed = forms.FloatField(label='Descent speed (ft/s)')
    burstAltitude = forms.IntegerField(label='Burst altitude (ft ASL)')

class NewMultiPredictionForm(forms.Form):
    latitude = forms.FloatField(label='Launch latitude')
    longitude = forms.FloatField(label='Launch longitude')
    startTime = forms.DateTimeField(label='Start Time')
    interval = forms.FloatField(label='Interval')
    number = forms.IntegerField(label='# of Predictions')
    altitude = forms.IntegerField(label='Launch altitude (ft ASL)',required=False)
    autoAltitude = forms.BooleanField(label='Auto',required=False)
    ascentSpeed = forms.FloatField(label='Ascent speed (ft/s)')
    descentSpeed = forms.FloatField(label='Descent speed (ft/s)')
    burstAltitude = forms.IntegerField(label='Burst altitude (ft ASL)')

class NewLaunchPointForm(forms.Form):
    name = forms.CharField(label="Name", max_length=25)
    description = forms.CharField(label="Description", max_length=200, required=False, widget=forms.Textarea)
    latitude = forms.FloatField(label="Latitude", min_value=-90, max_value=90)
    longitude = forms.FloatField(label="Longitude", min_value=-180, max_value=180)
    altitude = forms.IntegerField(label="Altitude", min_value=0, required=False)
    autoAltitude = forms.BooleanField(label="Auto")
