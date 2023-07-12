# Import the required library
import pandas as pd
from geopy.geocoders import Nominatim

df = pd.read_csv("./dataset/indeed_de_jobs_cleaned.csv")
cities = list(df["city"])
# Initialize Nominatim API

for city in set(cities):
    if city != "unknown":
        geolocator = Nominatim(user_agent="MyApp")
        location = geolocator.geocode(city)
        if location is not None:
            print("City: ", city)
            print("The latitude of the location is: ", location.latitude)
            print("The longitude of the location is: ", location.longitude)
# geolocator = Nominatim(user_agent="MyApp")
# location = geolocator.geocode("Seattle")
# print("The latitude of the location is: ", location.latitude)
