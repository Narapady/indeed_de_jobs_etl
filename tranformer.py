import re
from pathlib import Path

import pandas as pd
from geopy.geocoders import Nominatim

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York State": "NY",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington State": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "Washington, DC": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "Virgin Islands": "VI",
}


def get_zipcode(location: str) -> str:
    zipcode = re.findall(r"\d+", location)
    if not zipcode:
        return "unknown"

    return zipcode[0].strip()


def get_city(location: str) -> str:
    city = location.split(", ")[0]

    if not city:
        city = "unknown"

    elif len(location.split(", ")) == 1:
        city = "unknown"

    elif "Washington, DC" in location:
        city = "Washington, DC"

    elif "Remote in " in city:
        city = city.replace("Remote in ", "")

    elif "Hybrid remote in " in city:
        city = city.replace("Hybrid remote in ", "")

    elif city.strip() == "Remote":
        city = "unknown"

    return city.strip()


def get_state(location: str) -> str:
    states = list(us_state_to_abbrev.values())
    state = location.split(", ")
    if len(state) == 1 and "Remote in " in state[0]:
        state = state[0].replace("Remote in ", "")
        return us_state_to_abbrev[state]

    if len(state) == 1 and "Hybrid remote in " in state[0]:
        state = state[0].replace("Hybrid remote in ", "")
        return us_state_to_abbrev[state]

    regex = re.compile(r"\b(" + "|".join(states) + r")\b")
    state = re.findall(regex, location)

    if not state:
        return "unknown"

    return state[0].strip()


def tranform_work_hour(work_hour: str) -> str:
    if work_hour == "unknown":
        return "unknown"
    return work_hour


def get_work_type(location: str) -> str:
    if "Hybrid remote in" in location:
        return "Hybrid remote"
    if "Remote in" in location:
        return "Remote"
    return "On site"


def city_latitude(city: str) -> str:
    if city == "unknown":
        return "unknown"

    geolocator = Nominatim(user_agent="MyApp")
    location = geolocator.geocode(city)
    if location is not None:
        return str(location.latitude)


def city_longitude(city: str) -> str:
    if city == "unknown":
        return "unknown"

    geolocator = Nominatim(user_agent="MyApp")
    location = geolocator.geocode(city)
    if location is not None:
        print(f"Longitude", location.longitude)
        return str(location.longitude)


def tranform_salary(salary: str) -> str:
    salary = salary.replace("Estimated", "")
    salary = salary.replace("$", "")
    return salary


def main() -> None:
    df = pd.read_csv("./dataset/indeed_de_jobs.csv")

    df["location"] = df["location"].apply(lambda x: x.replace("\xa0", " "))
    df["city"] = df["location"].apply(lambda x: get_city(x))
    df["state"] = df["location"].apply(lambda x: get_state(x))
    df["zipcode"] = df["location"].apply(lambda x: get_zipcode(x))
    df["work_hour"] = df["work_hour"].apply(lambda x: tranform_work_hour(x))
    df["salary"] = df["salary"].apply(lambda x: tranform_salary(x))
    df["work_type"] = df["location"].apply(lambda x: get_work_type(x))
    df["latitude"] = df["city"].apply(lambda x: city_latitude(x))
    df["longitude"] = df["city"].apply(lambda x: city_longitude(x))

    df = df.rename(
        columns={"salary": "estimated_salary_usd", "location": "company_address"}
    )
    path = Path.cwd() / "dataset" / "indeed_de_jobs_cleaned.csv"
    df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
