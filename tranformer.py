import re
from pathlib import Path

import numpy as np
import pandas as pd


def get_zipcode(location: str) -> str:
    zipcode = re.findall(r"\d+", location)
    if not zipcode:
        return "unknown"

    return zipcode[0].strip()


def get_city(location: str) -> str:
    city = location.split(", ")[0]

    if not city:
        return "unknown"

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
        return "unknown"

    return state[0].strip()


def tranform_work_hour(work_hour: str) -> str:
    if work_hour == "unknown":
        return "unknown"
    return work_hour


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

    df = df.rename(
        columns={"salary": "estimated_salary_usd", "location": "company_address"}
    )
    path = Path.cwd() / "dataset" / "indeed_de_jobs_cleaned.csv"
    df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
