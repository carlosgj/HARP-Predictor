# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-08-13 21:55
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Predictor', '0004_auto_20170813_2112'),
    ]

    operations = [
        migrations.AlterField(
            model_name='launchlocation',
            name='altitude',
            field=models.IntegerField(verbose_name='Altitude'),
        ),
    ]