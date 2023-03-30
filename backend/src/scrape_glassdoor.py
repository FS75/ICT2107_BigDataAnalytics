import asyncio
import random
import time

import httpx
import json
import re
from typing import Tuple, List, Dict
from parsel import Selector
import pandas as pd

from main import reviews_to_scrape, details_folder_prefix, review_folder_prefix

user_agent_list = [
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    ]

country_code = {
    "Argentina": 13,
    "Australia": 5,
    "Belgique (Français)": 15,
    "België (Nederlands)": 14,
    "Brasil": 9,
    "Canada (English)": 3,
    "Canada (Français)": 19,
    "Deutschland": 7,
    "España": 8,
    "France": 6,
    "Hong Kong": 20,
    "India": 4,
    "Ireland": 18,
    "Italia": 23,
    "México": 12,
    "Nederland": 10,
    "New Zealand": 21,
    "Schweiz (Deutsch)": 16,
    "Singapore": 22,
    "Suisse (Français)": 17,
    "United Kingdom": 2,
    "United States": 1,
    "Österreich": 11
}

# "HSBC", "United Overseas Bank", "Micron Technology, Inc",
# "Meta", "Amazon", "Apple",
# "Dbs Bank", "Accenture", "Netflix",
# "Google", "Infineon Technologies", "St Engineering",
# "Sembcorp", "Ntuc Fairprice"

# "HSBC", "Micron Technology, Inc",
# "Amazon", "Apple",
# "Dbs Bank", "Accenture", "Netflix",
# "Google", "Infineon Technologies",
companies_to_scrape = ["HSBC"]

pandas_glassdoor_details_dict = {
    "employer_name": [],
    "employer_id": [],
    "employer_website": [],
    "employer_description": [],
    "employer_size": [],
    "employer_revenue": [],
    "employer_headquarters": [],
    "employer_founded": [],
    "employer_industry": []
}

pandas_glassdoor_reviews_dict = {
    "Rating": [],
    "Title": [],
    "Pro": [],
    "Con": [],
    "Job Title": [],
    "Status": [],
    "Region": [],
    "Date": [],
}


# define async for review scrape
async def scrape_glassdoor():
    for company in companies_to_scrape:
        print(f"Sleeping for 1 seconds before scraping {company}...")
        await asyncio.sleep(1)
        print(f"Scraping {company}'s details from Glassdoor...")
        glassdoor_company_payload = glassdoor_find_companies(company)
        # glassdoor_response = await get_response(f"https://www.glassdoor.com/Overview/Working-at-"
        #                                   f"{glassdoor_company_payload['suggestion']}-EI_IE"
        #                                   f"{glassdoor_company_payload['id']}.htm")
        # glassdoor_details_payload = get_glassdoor_details_payload(glassdoor_company_payload, glassdoor_response)
        # details_payload_to_pandas_dict(glassdoor_details_payload, pandas_glassdoor_details_dict)
        # pandas_dict_to_csv(pandas_glassdoor_details_dict, f"{details_folder_prefix}/glassdoor_details.csv")

        glassdoor_reviews_payload = await get_glassdoor_reviews(glassdoor_company_payload)

        for key in pandas_glassdoor_reviews_dict:
            pandas_glassdoor_reviews_dict[key].clear()

        review_payload_to_pandas_dict(glassdoor_reviews_payload, pandas_glassdoor_reviews_dict)
        pandas_dict_to_csv(pandas_glassdoor_reviews_dict, f"{review_folder_prefix}/{company}_glassdoor_review.csv")


"""
Purpose: Function to generate a response for specified URL
PARAMS: url : string - contains URL to a website 
        eg. "https://www.glassdoor.com/Overview/Working-at-eBay-EI_IE7853.11,15.htm"
"""


async def get_response(url):
    timeout = httpx.Timeout(30.0, read=120.0)
    client = httpx.AsyncClient(timeout=timeout)
    res = await client.get(
        url,
        cookies={'tldp': f'{random.randint(1, 10000)}'},
        follow_redirects=True,
        headers={'User-Agent': random.choice(user_agent_list)}
    )
    return res


"""
Purpose: Function to scrape glassdoor and get the payload with a specified response
PARAMS: response - contains entire page's CSS which will be read with Selector
"""


def get_glassdoor_details_payload(company_payload, response):
    selector = Selector(response.text)
    return {
        "employer_name": company_payload["suggestion"],
        "employer_id": company_payload["id"],
        "employer_website": selector.css('[data-test="employer-website"]::text').get(),
        "employer_description": selector.css('[data-test="employerDescription"]::text').get(),
        "employer_size": selector.css('[data-test="employer-size"]::text').get(),
        "employer_revenue": selector.css('[data-test="employer-revenue"]::text').get(),
        "employer_headquarters": selector.css('[data-test="employer-headquarters"]::text').get(),
        "employer_founded": selector.css('[data-test="employer-founded"]::text').get(),
        "employer_industry": selector.css('[data-test="employer-industry"]::text').get()
    }


def glassdoor_find_companies(query: str):
    """find company Glassdoor ID and name by query. e.g. "ebay" will return "eBay" with ID 7853"""
    result = httpx.get(
        url=f"https://www.glassdoor.com/searchsuggest/typeahead?numSuggestions=8&source=GD_V2&version=NEW&rf=full&fallback=token&input={query}",
        headers={'User-Agent': random.choice(user_agent_list)}
    )
    # print(result.content)
    data = json.loads(result.content)
    return {
        "suggestion": data[0]["suggestion"],
        "id": data[0]["employerId"]
    }


def details_payload_to_pandas_dict(payload, pandas_dict):
    # value is dict[key]
    for key in payload:
        pandas_dict[key].append(payload[key])


def review_payload_to_pandas_dict(payload, pandas_dict):
    for key in payload:
        for item in payload[key]:
            pandas_dict[key].append(item)


def pandas_dict_to_csv(pandas_dict, filename):
    key_lengths = []
    keys = ["Rating", "Title", "Pro", "Con", "Job Title", "Status", "Region", "Date"]
    for key in pandas_dict:
        key_lengths.append(len(pandas_dict[key]))

    for i in range(0, len(key_lengths)):
        while key_lengths[i] != max(key_lengths):
            pandas_dict[keys[i]].append("")

    df = pd.DataFrame(pandas_dict)
    df.to_csv(filename)


async def get_glassdoor_reviews(company_payload):
    first_page = await get_response(
        url=f"https://www.glassdoor.com/Reviews/"
            f"{company_payload['suggestion']}-Reviews-E"
            f"{company_payload['id']}_P1.htm"
    )

    await asyncio.sleep(1)

    reviews_payload = {
        "Rating": [],
        "Title": [],
        "Pro": [],
        "Con": [],
        "Job Title": [],
        "Status": [],
        "Region": [],
        "Date": [],
    }

    reviews = parse_reviews(first_page.text)
    total_pages = reviews["numberOfPages"]

    print(f"Total pages available to scrape for {company_payload['suggestion']}'s reviews: {total_pages}")

    pages_to_scrape = int(reviews_to_scrape / 10)

    if total_pages < pages_to_scrape:
        actual_pages_to_scrape = total_pages
    else:
        actual_pages_to_scrape = pages_to_scrape

    print(f"Commencing scraping of {actual_pages_to_scrape} pages for {company_payload['suggestion']}'s reviews")

    for page in range(1, actual_pages_to_scrape + 1):
        print(f"Scraping {company_payload['suggestion']}'s reviews from page {page}")
        response = await get_response(f"https://www.glassdoor.com/Reviews/"
                                f"{company_payload['suggestion']}-Reviews-E"
                                f"{company_payload['id']}_P"
                                f"{page}.htm")
        await asyncio.sleep(1)
        glassdoor_reviews_payload = get_glassdoor_reviews_payload(response)
        for key in glassdoor_reviews_payload:
            for item in glassdoor_reviews_payload[key]:
                reviews_payload[key].append(item)

            # to ensure same rows for pandas to convert to df
            # while len(reviews_payload[key]) <= 10:
            # #     # print(reviews_payload[key])
            #     reviews_payload[key].append("")

    return reviews_payload


def get_glassdoor_reviews_payload(response):
    selector = Selector(response.text)
    job_details = selector.css('[class="common__EiReviewDetailsStyle__newUiJobLine"]').getall()

    job_title_holder = []
    region_holder = []
    date_holder = []

    # print(job_details)
    for job in job_details:
        date, role, region = extract_job_details(job)
        job_title_holder.append(role)
        region_holder.append(region)
        date_holder.append(date)

    statuses = selector.css('[class="pt-xsm pt-md-0 css-1qxtz39 eg4psks0"]::text').getall()

    status_holder = []

    for status in statuses:
        status = extract_status(status)
        status_holder.append(status)

    return {
        "Rating": selector.css('[class="ratingNumber mr-xsm"]::text').getall(),
        "Title": selector.css('[class="reviewLink"]::text').getall(),
        "Pro": selector.css('[data-test="pros"]::text').getall(),
        "Con": selector.css('[data-test="cons"]::text').getall(),
        "Job Title": job_title_holder,
        "Status": status_holder,
        "Region": region_holder,
        "Date": date_holder
    }


def extract_apollo_state(html):
    """Extract apollo graphql state data from HTML source"""
    data = re.findall('apolloState":\s*({.+})};', html)[0]
    data = json.loads(data)
    return data


def parse_reviews(html) -> Tuple[List[Dict], int]:
    """parse jobs page for job data and total amount of jobs"""
    cache = extract_apollo_state(html)
    xhr_cache = cache["ROOT_QUERY"]
    reviews = next(v for k, v in xhr_cache.items() if k.startswith("employerReviews") and v.get("reviews"))
    return reviews


def extract_job_details(string):
    # Extract date
    date = string[:string.find(" - ")]
    date = date[date.rfind(">") + 1:]

    # print(string)

    role = string[string.find(" - ") + 3:]
    role = role[:role.find("<")]

    if string.find("in<!") != -1:
        region = string[string.find("in<!") + 17:]
        region = region[:region.find("<")]
    else:
        region = ""

    return date, role, region


def extract_status(string):
    if string.find(",") != 1:
        return string[:string.find(",")]
