import os

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()


titles = []
companies = []
locations = []
job_details = []


def get_soup(start_at: int) -> BeautifulSoup:
    URL = f"https://www.indeed.com/jobs?q=Data+engineer&l=United+States&start={start_at}&vjk=3dbde0befe814ec1"
    payload = {"api_key": os.getenv("API_KEY"), "url": URL, "dynamic": "false"}
    resp = requests.get("https://api.scrapingdog.com/scrape", params=payload)
    soup = BeautifulSoup(resp.text, "html.parser")
    return soup


def extract(soup: BeautifulSoup) -> None:
    jobs = soup.find_all("td", "resultContent")
    for job in jobs:
        title = job.find("h2").a.span["title"]
        company = job.find("span", class_="companyName").text
        location = job.find("div", class_="companyLocation").text
        job_detail = [word for word in job.contents[2].strings]

        titles.append(title)
        companies.append(company)
        locations.append(location)
        job_details.append(job_detail)

    # titles = [job.find("h2").a.span["title"] for job in jobs]
    # companies = [job.find("span", class_="companyName").text for job in jobs]
    # locations = [job.find("div", class_="companyLocation").text for job in jobs]
    # job_details = [[word for word in job.contents[2].strings] for job in jobs]


def main() -> None:
    soup = get_soup(start_at=0)
    extract(soup)
    print(titles)
    print(companies)
    print(locations)
    print(job_details)


if __name__ == "__main__":
    main()
