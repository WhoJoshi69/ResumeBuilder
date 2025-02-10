import aiohttp
import asyncio
import pandas as pd
from typing import List, Dict
import json
from datetime import datetime
import time
import random
import argparse

# Constants
JOBS_URL = 'https://www.naukri.com/jobapi/v3/search'
COMPANIES_URL = 'https://www.naukri.com/companyapi/v1/search'
COMPANIES_JSON_FILE = 'companies.json'

# Headers for jobs API
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Brave";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sec-gpc': '1',
    'Referer': 'https://www.naukri.com/',
    'appid': '109',
    'clientid': 'd3skt0p',
    'gid': 'LOCATION,INDUSTRY,EDUCATION,FAREA_ROLE',
    'systemid': '109'
}

# Cookies
COOKIES = {
    '_t_ds': '102f40301737661732-43102f4030-0102f4030',
    'J': '0'
}

# Rate limiting settings
BATCH_SIZE = 5  # Number of companies to process in parallel
DELAY_BETWEEN_BATCHES = 5  # Delay in seconds between batches
RETRY_DELAY = 2  # Delay in seconds before retrying a failed request
MAX_RETRIES = 3  # Maximum number of retries for a failed request

async def fetch_companies(session: aiohttp.ClientSession) -> Dict:
    """Fetch companies from Naukri API"""
    params = {
        'pageNo': 1,
        'qcount': 4800,
        'searchType': 'companySearch'
    }
    
    headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.5',
        'appid': '103',
        'clientid': 'd3skt0p',
        'content-type': 'application/json',
        'priority': 'u=1, i',
        'systemid': 'js',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
    }
    
    try:
        async with session.get(COMPANIES_URL, headers=headers, params=params, ssl=False) as response:
            if response.status == 200:
                data = await response.json()
                # Save to JSON file
                with open(COMPANIES_JSON_FILE, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
                print(f"Successfully fetched and saved {len(data.get('groupDetails', []))} companies")
                return data
            else:
                print(f"Error fetching companies: Status {response.status}")
                return {}
    except Exception as e:
        print(f"Exception in fetch_companies: {str(e)}")
        return {}

def load_companies(update_json: bool = False, company_ids: List[int] = None) -> List[Dict]:
    """
    Load companies from local JSON file or fetch from API if update requested
    
    Args:
        update_json (bool): If True, fetch fresh data from API
        company_ids (List[int], optional): List of company IDs to filter. If None, return all companies.
    """
    if not update_json:
        try:
            with open(COMPANIES_JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                companies = data['groupDetails']
                if company_ids:
                    # Filter companies based on provided IDs
                    companies = [company for company in companies if company['groupId'] in company_ids]
                    print(f"Filtered {len(companies)} companies based on provided IDs")
                return companies
        except Exception as e:
            print(f"Error loading companies from {COMPANIES_JSON_FILE}: {str(e)}")
            return []
    
    # If update_json is True, this will be handled in main()
    return []

async def fetch_jobs_with_retry(session: aiohttp.ClientSession, group_id: int) -> Dict:
    """Fetch jobs for a specific company with retry logic"""
    params = {
        'noOfResults': 20,
        'groupId': group_id,
        'pageNo': 1,
        'searchType': 'groupidsearch'
    }

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(JOBS_URL,
                                headers=HEADERS,
                                params=params,
                                cookies=COOKIES,
                                ssl=False) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Too Many Requests
                    print(f"Rate limited for group {group_id}, waiting longer...")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 2))  # Exponential backoff
                    continue
                else:
                    print(f"Error fetching jobs for group {group_id}: Status {response.status}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    return {}
        except Exception as e:
            print(f"Exception in fetch_jobs for group {group_id}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return {}
    return {}

async def process_company(session: aiohttp.ClientSession, company: Dict) -> List[Dict]:
    """Process a single company and its jobs"""
    jobs_data = []
    try:
        group_id = company['groupId']
        jobs_response = await fetch_jobs_with_retry(session, group_id)

        if 'jobDetails' in jobs_response:
            for job in jobs_response['jobDetails']:
                job_info = {
                    'company_name': company['groupName'],
                    'company_rating': company.get('rating', 'N/A'),
                    'about_company': company.get('aboutCompany', 'N/A'),
                    'website': company.get('website', 'N/A'),
                    'naukari_url': company.get('groupJobsURL', 'N/A'),
                    'job_title': job['title'],
                    'experience': next((p['label'] for p in job['placeholders'] if p['type'] == 'experience'), 'N/A'),
                    'location': next((p['label'] for p in job['placeholders'] if p['type'] == 'location'), 'N/A'),
                    'salary': next((p['label'] for p in job['placeholders'] if p['type'] == 'salary'), 'N/A'),
                    'skills': job['tagsAndSkills'],
                    'job_description': job['jobDescription'],
                    'posted_date': datetime.fromtimestamp(job['createdDate'] / 1000).strftime('%Y-%m-%d'),
                    'job_url': f"https://www.naukri.com{job['jdURL']}"
                }
                jobs_data.append(job_info)

    except Exception as e:
        print(f"Error processing company {company.get('groupName', 'Unknown')}: {str(e)}")

    return jobs_data

async def process_company_batch(session: aiohttp.ClientSession, companies: List[Dict]) -> List[Dict]:
    """Process a batch of companies"""
    tasks = [process_company(session, company) for company in companies]
    results = await asyncio.gather(*tasks)
    return [job for company_jobs in results for job in company_jobs]

async def main(update_json: bool = False, company_ids: List[int] = None):
    """Main function to fetch and process all jobs"""
    all_jobs = []
    
    # Set up aiohttp session for the entire process
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Update companies.json if requested
        if update_json:
            companies_data = await fetch_companies(session)
            if not companies_data:
                print("Failed to fetch companies data")
                return
            companies = companies_data.get('groupDetails', [])
            if company_ids:
                companies = [company for company in companies if company['groupId'] in company_ids]
        else:
            # Load companies from JSON file
            companies = load_companies(update_json=False, company_ids=company_ids)
        
        if not companies:
            print("No companies found")
            return

        total_companies = len(companies)

        # Process companies in batches
        for i in range(0, total_companies, BATCH_SIZE):
            batch = companies[i:i + BATCH_SIZE]
            print(f"Processing batch {i//BATCH_SIZE + 1} of {(total_companies + BATCH_SIZE - 1)//BATCH_SIZE}")
            
            # Process the batch
            batch_results = await process_company_batch(session, batch)
            all_jobs.extend(batch_results)
            
            # Add random delay between batches to avoid rate limiting
            if i + BATCH_SIZE < total_companies:
                delay = DELAY_BETWEEN_BATCHES + random.uniform(1, 3)
                print(f"Waiting {delay:.2f} seconds before next batch...")
                await asyncio.sleep(delay)

    # Save results to CSV
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        output_file = f'naukri_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Saved {len(all_jobs)} jobs to {output_file}")
    else:
        print("No jobs were found")

if __name__ == "__main__":
    asyncio.run(main(update_json=False, company_ids=[3556344]))
