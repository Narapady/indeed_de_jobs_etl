import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()


titles = []
companies = []
locations = []
salaries = []
work_types = []


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
        titles.append(title.a.span["title"])


def extract_company_name(job: BeautifulSoup) -> None:
    company = job.find("span", class_="companyName")
    if company is not None:
        companies.append(company.text)


def extract_job_location(job: BeautifulSoup) -> None:
    location = job.find("div", class_="companyLocation")
    if location is not None:
        locations.append(location.text)


def extract_salary(job: BeautifulSoup) -> None:
    job_details = job.contents[2].strings
    for word in job_details:
        if "$" in word:
            salaries.append(word)


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
    for word in job_details:
        if word.lower() in work_types_list or "shift" in word.lower():
            work_types.append(word.lower())
        if len(salaries) == len(work_types):
            break

    if len(salaries) > len(work_types):
        work_types.append("unknown")


def extract(job: BeautifulSoup) -> None:
    extract_job_title(job)
    extract_company_name(job)
    extract_job_location(job)
    extract_salary(job)
    extract_work_hours(job)


def main() -> None:
    job_lists = get_job_list(page=0)
    for job in job_lists:
        extract(job)

    # df = pd.DataFrame(
    #     {
    #         "job_title": titles,
    #         "company_name": companies,
    #         "location": locations,
    #         "salary": salaries,
    #         "work_type": work_types,
    #     }
    # )
    # print(tabulate(df, headers="keys", tablefmt="psql"))

    print(len(titles))
    print(titles, "\n\n")
    print(len(companies))
    print(companies, "\n\n")
    print(len(locations))
    print(locations, "\n\n")
    print(len(salaries))
    print(salaries)
    print(len(work_types))
    print(work_types)


if __name__ == "__main__":
    main()
