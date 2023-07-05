import re

import numpy as np
import pandas as pd

df = pd.read_csv("./indeed_de_jobs_full.csv")
df["location"] = df["location"].apply(lambda x: x.replace("\xa0", " "))


def get_zipcode(location: str) -> str:
    zipcode = re.findall(r"\d+", location)
    if not zipcode:
        return np.nan

    return zipcode[0].strip()


def get_city(location: str) -> str:
    city = location.split(", ")[0]

    if not city:
        return np.nan

    if "Remote in " in city:
        city = city.replace("Remote in ", "")

    if "Hybrid remote in " in city:
        city = city.replace("Hybrid remote in ", "")

    return city.strip()


def get_state(location: str) -> str:
    states = [
        "IA",
        "KS",
        "UT",
        "VA",
        "NC",
        "NE",
        "SD",
        "AL",
        "ID",
        "FM",
        "DE",
        "AK",
        "CT",
        "PR",
        "NM",
        "MS",
        "PW",
        "CO",
        "NJ",
        "FL",
        "MN",
        "VI",
        "NV",
        "AZ",
        "WI",
        "ND",
        "PA",
        "OK",
        "KY",
        "RI",
        "NH",
        "MO",
        "ME",
        "VT",
        "GA",
        "GU",
        "AS",
        "NY",
        "CA",
        "HI",
        "IL",
        "TN",
        "MA",
        "OH",
        "MD",
        "MI",
        "WY",
        "WA",
        "OR",
        "MH",
        "SC",
        "IN",
        "LA",
        "MP",
        "DC",
        "MT",
        "AR",
        "WV",
        "TX",
    ]
    regex = re.compile(r"\b(" + "|".join(states) + r")\b")
    state = re.findall(regex, location)

    if not state:
        return np.nan

    return state[0].strip()


def tranform_work_hour(work_hour: str) -> str:
    if work_hour == "unknown":
        return np.nan
    return work_hour


df["city"] = df["location"].apply(lambda x: get_city(x))
df["state"] = df["location"].apply(lambda x: get_state(x))
df["zipcode"] = df["location"].apply(lambda x: get_zipcode(x))
df["work_hour"] = df["work_hour"].apply(lambda x: tranform_work_hour(x))
df.head(40)
