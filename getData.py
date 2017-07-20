def roundHalf(n):
    return str(round(n * 2) / 2)
def pad3(s):
    retval = "000"+str(s)
    return retval[-3:]
    
def downloadGrib(latIn, longIn, forecastHour):
    # Get the values from the form
    #tmpTime = currentPrediction.forecastCycleTime;
    #forecastCycle = tmpTime.format("yyyyMMddHH");
    forecastCycle = "2017071912"
    #forecastCycleHour = tmpTime.format("HH");
    forecastCycleHour = "12"
    # calculate the 4 lat/long points surrounding the desired point
    latTop = latIn + (5 * 0.5);
    latTop = roundHalf(latTop);
    latBot = latIn - (5 * 0.5);
    latBot = roundHalf(latBot);
    longLeft = longIn - (5 * 0.5);
    longLeft = roundHalf(longLeft);
    longRight = longIn + (5 * 0.5);
    longRight = roundHalf(longRight);
    # Create the URL with all of the params
    bigURL = "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?file=gfs.t";
    bigURL += forecastCycleHour;
    bigURL += "z.pgrb2full.0p50.f";
    bigURL += pad3(forecastHour);
    bigURL += "&lev_1_mb=on&lev_2_mb=on&lev_3_mb=on&lev_5_mb=on&lev_7_mb=on&";
    bigURL += "lev_10_mb=on&lev_20_mb=on&lev_30_mb=on&lev_50_mb=on&lev_70_mb=on&lev_100_mb=on&lev_125_mb=on&lev_150_mb=on&lev_175_mb=on&";
    bigURL += "lev_200_mb=on&lev_225_mb=on&lev_250_mb=on&lev_275_mb=on&lev_300_mb=on&lev_325_mb=on&lev_350_mb=on&lev_375_mb=on&";
    bigURL += "lev_400_mb=on&lev_425_mb=on&lev_450_mb=on&lev_475_mb=on&lev_500_mb=on&lev_525_mb=on&lev_550_mb=on&lev_575_mb=on&";
    bigURL += "lev_600_mb=on&lev_625_mb=on&lev_650_mb=on&lev_675_mb=on&lev_700_mb=on&lev_725_mb=on&lev_750_mb=on&lev_775_mb=on&";
    bigURL += "lev_800_mb=on&lev_825_mb=on&lev_850_mb=on&lev_875_mb=on&lev_900_mb=on&lev_925_mb=on&lev_950_mb=on&lev_975_mb=on&";
    bigURL += "lev_1000_mb=on&var_HGT=on&var_TMP=on&var_UGRD=on&var_VGRD=on&subregion=&leftlon=";
    bigURL += longLeft;
    bigURL += "&rightlon=";
    bigURL += longRight;
    bigURL += "&toplat=";
    bigURL += latTop;
    bigURL += "&bottomlat=";
    bigURL += latBot;
    bigURL += "&dir=%2Fgfs.";
    bigURL += forecastCycle;
    print bigURL
    
if __name__=="__main__":
    downloadGrib(34, -118, 9)