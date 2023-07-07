import os
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

titles = []
companies = []
locations = []
salaries = []
work_hours = []


def get_job_list(page: int) -> BeautifulSoup:
    URL = f"https://www.indeed.com/jobs?q=Data+engineer&l=United+States&start={page}&vjk=3dbde0befe814ec1"
    payload = {"api_key": os.getenv("API_KEY"), "url": URL, "dynamic": "false"}
    resp = requests.get("https://api.scrapingdog.com/scrape", params=payload)
    soup = BeautifulSoup(resp.text, "html.parser")
    jobs = soup.find_all("td", "resultContent")

    return jobs


def extract_job_title(job: BeautifulSoup) -> None:
    title = job.find("h2")
    if title is not None:
        try:
            titles.append(title.a.span["title"])
        except TypeError:
            title.append("unknown")


def extract_company_name(job: BeautifulSoup) -> None:
    company = job.find("span", class_="companyName")
    if company is not None:
        try:
            companies.append(company.text)
        except TypeError:
            companies.append("unknown")


def extract_job_location(job: BeautifulSoup) -> None:
    location = job.find("div", class_="companyLocation")
    if location is not None:
        try:
            locations.append(location.text)
        except TypeError:
            locations.append("unknown")


def extract_salary(job: BeautifulSoup) -> None:
    job_details = job.contents[2].strings
    if job_details:
        for word in job_details:
            if "$" in word:
                salaries.append(word)
    if len(salaries) < len(companies):
        salaries.append("unknown")


def extract_work_hours(job: BeautifulSoup) -> None:
    work_types_list = [
        "full-time",
        "contract",
        "part-time",
        "remote",
        "fully-remote",
        "monday to friday",
    ]
    job_details = job.contents[2].strings
    if job_details:
        for word in job_details:
            if word.lower() in work_types_list or "shift" in word.lower():
                work_hours.append(word.lower())
                break

    if len(salaries) > len(work_hours):
        work_hours.append("unknown")


def extract(job: BeautifulSoup) -> None:
    extract_job_title(job)
    extract_company_name(job)
    extract_job_location(job)
    extract_salary(job)
    extract_work_hours(job)

    assert (
        len(titles)
        == len(companies)
        == len(locations)
        == len(salaries)
        == len(work_hours)
    )


def main() -> None:
    for i in range(0, 661, 10):
        job_lists = get_job_list(page=i)
        for job in job_lists:
            extract(job)
        print(f"done page {i}")

    df = pd.DataFrame(
        {
            "job_title": titles,
            "company_name": companies,
            "location": locations,
            "salary": salaries,
            "work_hour": work_hours,
        }
    )
    path = Path.cwd() / "datasets" / "indeed_de_jobs.csv"
    df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
