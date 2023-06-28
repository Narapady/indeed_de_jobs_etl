import requests
from bs4 import BeautifulSoup

URL = "https://www.indeed.com/jobs?q=Data+engineer&l=United+States&from=searchOnHP&vjk=248e26d0ad4e6008"
payload = {"api_key": "649b892597d4a95c9e66005b", "url": URL, "dynamic": "false"}

titles = []
companies = []
locations = []
job_details = []


def extract(url: str) -> None:
    resp = requests.get("https://api.scrapingdog.com/scrape", params=payload)
    soup = BeautifulSoup(resp.text, "html.parser")
    jobs = soup.find_all("td", "resultContent")

    for job in jobs:
        title = job.find("h2").a.span["title"]
        company = job.find("span", class_="companyName").text
        location = job.find("div", class_="companyLocation").text
        job_detail = [word for word in job.contents[2].strings]

        titles.append(title)
        companies.append(company)
        location.append(location)
        job_details.append(job_detail)
