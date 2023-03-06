import asyncio
import time

import httpx
import json
import re
from typing import Tuple, List, Dict
from parsel import Selector
import pandas as pd

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

companies_to_scrape = ["meta", "amazon", "apple", "netflix", "google"]

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
    "pros": [],
    "cons": [],
    "advice": [],
}

details_folder_prefix = "../dataset/details_dataset"
review_folder_prefix = "../dataset/reviews_dataset"

pages_to_scrape = 50


# define async for review scrape
async def scrape_glassdoor():
    # company_payload = find_companies("ebay")
    for company in companies_to_scrape:
        print(f"Scraping {company}'s details from Glassdoor...")
        company_payload = find_companies(company)
        # response = get_response(f"https://www.glassdoor.com/Overview/Working-at-"
        #                         f"{company_payload['suggestion']}-EI_IE"
        #                         f"{company_payload['id']}.htm")
        # glassdoor_details_payload = get_glassdoor_details_payload(company_payload, response)
        # details_payload_to_pandas_dict(glassdoor_details_payload, pandas_glassdoor_details_dict)
        # pandas_dict_to_csv(pandas_glassdoor_details_dict, f"{details_folder_prefix}/glassdoor_details.csv")

        glassdoor_reviews_payload = get_glassdoor_reviews(company_payload)
        review_payload_to_pandas_dict(glassdoor_reviews_payload, pandas_glassdoor_reviews_dict)
        pandas_dict_to_csv(pandas_glassdoor_reviews_dict, f"{review_folder_prefix}/{company}_glassdoor_review2.csv")


"""
Purpose: Function to generate a cookie to specify country for response
PARAMS: country : string - contains the country you want crawled (look at dictionary at top of file)
        eg. "Singapore"
"""


def generate_country_cookie(country):
    return {"tldp": f"{country_code[country]}"}


"""
Purpose: Function to generate a response for specified URL
PARAMS: url : string - contains URL to a website 
        eg. "https://www.glassdoor.com/Overview/Working-at-eBay-EI_IE7853.11,15.htm"
"""


def get_response(url):
    timeout = httpx.Timeout(15.0, read=60.0)
    client = httpx.Client(timeout=timeout)
    return client.get(
        url,
        cookies=generate_country_cookie("Singapore"),
        follow_redirects=True,
    )


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


def find_companies(query: str):
    """find company Glassdoor ID and name by query. e.g. "ebay" will return "eBay" with ID 7853"""
    result = httpx.get(
        url=f"https://www.glassdoor.com/searchsuggest/typeahead?numSuggestions=8&source=GD_V2&version=NEW&rf=full&fallback=token&input={query}",
    )
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
    df = pd.DataFrame(pandas_dict)
    df.to_csv(filename)


def get_glassdoor_reviews(company_payload):
    first_page = get_response(
        url=f"https://www.glassdoor.com/Reviews/"
            f"{company_payload['suggestion']}-Reviews-E"
            f"{company_payload['id']}_P1.htm",
    )

    reviews_payload = {
        "pros": [],
        "cons": [],
        "advice": []
    }

    reviews = parse_reviews(first_page.text)
    total_pages = reviews["numberOfPages"]

    print(f"Total pages available to scrape for {company_payload['suggestion']}'s reviews: {total_pages}")

    if total_pages < pages_to_scrape:
        actual_pages_to_scrape = total_pages
    else:
        actual_pages_to_scrape = pages_to_scrape

    print(f"Commencing scraping of {actual_pages_to_scrape} pages for {company_payload['suggestion']}'s reviews")

    for page in range(50, 51):
        print(f"Scraping {company_payload['suggestion']}'s reviews from page {page}")
        response = get_response(f"https://www.glassdoor.com/Reviews/"
                                f"{company_payload['suggestion']}-Reviews-E"
                                f"{company_payload['id']}_P"
                                f"{page}.htm")
        glassdoor_reviews_payload = get_glassdoor_reviews_payload(response)
        for key in glassdoor_reviews_payload:
            for item in glassdoor_reviews_payload[key]:
                reviews_payload[key].append(item)

            # to ensure same rows for pandas to convert to df
            while len(reviews_payload["advice"]) != len(reviews_payload["pros"]):
                reviews_payload["advice"].append("nil")

    return reviews_payload


def get_glassdoor_reviews_payload(response):
    selector = Selector(response.text)
    return {
        "pros": selector.css('[data-test="pros"]::text').getall(),
        "cons": selector.css('[data-test="cons"]::text').getall(),
        "advice": selector.css('[data-test="advice-management"]::text').getall()
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
