from suntime import Sun, SunTimeException
from datetime import datetime, timedelta, timezone
from dateutil import tz
import pandas as pd


vilaverde_lat =  39.41
vilaverde_lon = -8.07

sun = Sun(vilaverde_lat, vilaverde_lon)

start_utc = datetime(2023, 1, 1)
end = datetime.now().date()

sunlight = {}
cursor = start_utc
while cursor<datetime.now():
    try:
        sr = sun.get_sunrise_time(cursor)
        ss = sun.get_sunset_time(cursor)
        
        if ss <= sr:
            ss = sun.get_sunset_time(cursor + timedelta(days=1))

        #print('On {} at somewhere in the north the sun raised at {} and get down at {}.'.
            #format(cursor, abd_sr.strftime('%H:%M'), abd_ss.strftime('%H:%M')))
        if cursor.day == 31 and cursor.month == 12:
            ss = ss - timedelta(days=1)
            print(sr)
            print(ss)
        sun_light = ss-sr
        minutes = sun_light.total_seconds() / 60.0
        format = '%Y-%m-%d'
        datetime_str =  cursor.strftime(format)


        sunlight[datetime_str]=minutes
        cursor   = min(cursor + timedelta(days=1), datetime.now())
    except SunTimeException as e:
        print("Error: {0}.".format(e))
print(sunlight)
df = pd.DataFrame()
df['Data'] = sunlight.keys()
df['Sunlight (em minutos)'] = sunlight.values()
df.to_csv('sunlight_perday.csv')
print(df.head())