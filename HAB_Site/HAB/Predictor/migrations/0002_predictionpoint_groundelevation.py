# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-08-22 23:17
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Predictor', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='predictionpoint',
            name='groundElevation',
            field=models.IntegerField(default=0),
            preserve_default=False,
        ),
    ]
