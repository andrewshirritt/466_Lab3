import pandas as pd
import numpy as np
from datetime import datetime
from meteostat import Point, Daily

import weather_pb2

def _fetch_daily_data(location: weather_pb2.Location, date_range: weather_pb2.DateRange) -> pd.DataFrame:
    """Fetch daily weather data from Meteostat."""

    start = datetime.strptime(date_range.start_date, '%Y-%m-%d')
    end = datetime.strptime(date_range.end_date, '%Y-%m-%d')
    point = Point(location.lat, location.lon)
    
    query = Daily(point, start, end)
    data = query.fetch()

    data.replace([np.inf, -np.inf, np.nan], 0.0, inplace=True)
    
    return data


def fetch_summary_data(location: weather_pb2.Location, date_range: weather_pb2.DateRange) -> tuple[float, float, float]:
    """Fetch weather data and return summary statistics.
    
    Args:
        location: A pb2 Location object with lat and lon attributes.
        date_range: A pb2 DateRange object with start_date and end_date.
        
    Returns:
        tuple: A tuple of (average_temperature, max_wind_speed, total_precipitation).
    """
    data = _fetch_daily_data(location, date_range)
    
    avg_temp = data['tavg'].mean()
    max_wind = data['wspd'].max()
    total_precip = data['prcp'].sum()

    return (
        float(avg_temp),
        float(max_wind),
        float(total_precip)
    )


def fetch_analysis_data(location: weather_pb2.Location, date_range: weather_pb2.DateRange) -> list:
    """Fetch weather data and return a list of precipitation events.
    
    Args:
        location: A pb2 Location object with lat and lon attributes.
        date_range: A pb2 DateRange object with start_date and end_date.
        
    Returns:
        list: A list of pb2.PrecipitationEvent objects.
    """
    data = _fetch_daily_data(location, date_range)

    events = []
    for index, val in data['prcp'].items():
        events.append(weather_pb2.PrecipitationEvent(
            date=index.strftime('%Y-%m-%d'),
            precipitation_mm=val
        ))
            
    return events