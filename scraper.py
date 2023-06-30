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


def get_soup(start_at: int) -> BeautifulSoup:
    URL = f"https://www.indeed.com/jobs?q=Data+engineer&l=United+States&start={start_at}&vjk=3dbde0befe814ec1"
    payload = {"api_key": os.getenv("API_KEY"), "url": URL, "dynamic": "false"}
    resp = requests.get("https://api.scrapingdog.com/scrape", params=payload)
    soup = BeautifulSoup(resp.text, "html.parser")

    return soup


def extract(soup: BeautifulSoup) -> None:
    jobs = soup.find_all("td", "resultContent")
    for job in jobs:
        title = job.find("h2")
        company = job.find("span", class_="companyName")
        location = job.find("div", class_="companyLocation")

        if title and company and location:
            titles.append(title.a.span["title"])
            companies.append(company.text)
            locations.append(location.text)

        for word in job.contents[2].strings:
            if "$" in word:
                salaries.append(word)
            else:
                salaries.append("unknown")

            if word.lower() in ("full-time", "contract", "part-time"):
                work_types.append(word.lower())
            else:
                work_types.append("unknown")


def main() -> None:
    soup = get_soup(start_at=0)
    extract(soup)
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
