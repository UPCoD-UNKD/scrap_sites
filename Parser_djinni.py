"""
1. Parse all companies profiles from djinni
2. Each profile must have URL at Jinni, URL at DOU, Name, Description, Name of HR, URL at HR Profile
3. If a company have more than one HR - add row and duplicate data except HR
4. Output should be in a CSV, UTF-8
5. Don't shady ask additional information
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import concurrent.futures
# --- constants ---
MAX_THREADS = 30
ts = time.strftime("%Y%m%d")
CSV = '{}_djinni.csv'.format(ts)
HOST = 'https://djinni.co'
URL = 'https://djinni.co/jobs/'
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0'
}


def async_scraping(scrape_function, urls):
    threads = min(MAX_THREADS, len(urls))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_function, urls)

    return results


def get_html(url, params=''):
    res = requests.get(url, headers=HEADERS, params=params)
    return res


def get_company_detail(url):
    session = requests.Session()

    recipe_cards = []
    response = session.get(f"{HOST}{url}")
    soup = BeautifulSoup(response.content, 'html.parser')

    res = soup.find("div", {"class": "profile-page-section"})
    print(res.text)
    description = res.text[0:res.text.find('Company website')].strip()
    return {'url': url, 'description': description, "duo": res.text[res.text.find('https://jobs.dou.ua'):].strip()}


def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    jobs = soup.find_all('li', class_='list-jobs__item')
    companies = {}
    for job in jobs:
        info = job.find('div', class_='list-jobs__details__info')
        link = info.find_all('a')
        name = link[0]['href']  # company profile url is unique
        if not link[0]['href'] in companies:
            # IF company new
            companies[name] = {}
            companies[name]['title'] = link[0].text.strip()

            companies[name]['hr'] = {}
        # add HR to hr list of company
        hr_link = info.find('a', class_="link-muted")
        companies[name]['hr'][hr_link['href']
                              ] = hr_link.text.replace("\n", "").strip()

    # GET company description and dou page url
    urls = [url for url in companies.keys()]
    details = async_scraping(get_company_detail, urls)

    for detail in details:
        companies[detail['url']]['description'] = detail['description']
        companies[detail['url']]['duo'] = detail['duo']
    return companies


def save_doc(companies, path):
    with open(path, 'w', newline='',encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Name', 'Description', 'HR Name',
                        'HR Profile', 'Djinni URL', 'DUO URL'])
        for com_key, company in companies.items():
            for hr_key, hr in company['hr'].items():
                writer.writerow([company['title'], company['description'],
                                hr, f"{HOST}{hr_key}", f"{HOST}{com_key}", company['duo']])


html = get_html(URL)
# print(get_content(html.text))
save_doc(get_content(html.text), CSV)
get_content(html.text)
