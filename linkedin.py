from asyncio import sleep
import logging
import csv
import os
from datetime import datetime
import random
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, \
    OnSiteOrRemoteFilters, SalaryBaseFilters

# Change root logger level (default is WARN)
logging.basicConfig(level=logging.INFO)


# Define 100 job titles to scrape
job_titles = [  'Digital Marketing Specialist', 'Business Development Manager',
                'Quality Assurance Analyst', 'Systems Administrator', 'Database Administrator', 'Cybersecurity Analyst', 'DevOps Engineer',
                'Mobile App Developer', 'Cloud Solutions Architect', 'Technical Support Engineer', 'SEO Specialist', 'Social Media Manager',
                'Content Marketing Manager', 'E-commerce Manager', 'Brand Manager', 'Public Relations Specialist', 'Event Coordinator',
                'Logistics Manager', 'Supply Chain Analyst', 'Operations Analyst', 'Risk Manager', 'Compliance Officer', 'Auditor', 'Tax Specialist',
                'Investment Analyst', 'Portfolio Manager', 'Real Estate Agent', 'Insurance Underwriter', 'Claims Adjuster', 'Actuary', 'Loan Officer', 'Credit Analyst', 'Treasury Analyst', 'Financial Planner',
                'Marketing Analyst', 'Market Research Analyst', 'Advertising Manager', 'Media Planner', 'Copywriter', 'Video Producer', 'Animator', 'Illustrator', 'Interior Designer', 'Architect',
                'Civil Engineer', 'Mechanical Engineer', 'Electrical Engineer', 'Chemical Engineer', 'Environmental Engineer', 'Biomedical Engineer', 'Industrial Engineer', 'Aerospace Engineer', 'Petroleum Engineer', 'Nuclear Engineer',
                'Pharmacist', 'Nurse Practitioner', 'Physician Assistant', 'Medical Laboratory Technician', 'Radiologic Technologist', 'Physical Therapist', 'Occupational Therapist', 'Speech-Language Pathologist', 'Dietitian', 'Respiratory Therapist',
                'Teacher', 'School Counselor', 'Librarian', 'Social Worker', 'Psychologist', 'Counselor', 'Therapist', 'Coach', 'Trainer', 'Recruiter'
            ]

for title in job_titles:

        # CSV file setup
    csv_filename = f"linkedin_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_file = open(csv_filename, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)

    # Write CSV header
    csv_writer.writerow([
        'Job Title', 
        'Company', 
        'Company Link', 
        'Date', 
        'Date Text', 
        'Job Link', 
        'Insights', 
        'Description Length',
        'Description'
    ])

    # Fired once for each successfully processed job
    def on_data(data: EventData):
        print('[ON_DATA]', data.title, data.company, data.company_link, data.date, data.date_text, data.link, data.insights,
            len(data.description))
        
        # Write data to CSV
        csv_writer.writerow([
            data.title,
            data.company,
            data.company_link,
            data.date,
            data.date_text,
            data.link,
            ', '.join(data.insights) if data.insights else '',
            len(data.description),
            data.description.replace('\n', ' ').replace('\r', ' ') if data.description else ''
        ])


    # Fired once for each page (25 jobs)
    def on_metrics(metrics: EventMetrics):
        print('[ON_METRICS]', str(metrics))


    def on_error(error):
        print('[ON_ERROR]', error)


    def on_end():
        print('[ON_END]')
        csv_file.close()
        print(f'Data saved to {csv_filename}')


    scraper = LinkedinScraper(
        chrome_executable_path=None,  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver)
        chrome_binary_location=None,  # Custom path to Chrome/Chromium binary (e.g. /foo/bar/chrome-mac/Chromium.app/Contents/MacOS/Chromium)
        chrome_options=None,  # Custom Chrome options here
        headless=True,  # Overrides headless mode only if chrome_options is None
        max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
        slow_mo=2.0,  # Slow down the scraper to avoid 'Too many requests 429' errors (5 seconds delay between requests)
        page_load_timeout=40  # Page load timeout (in seconds)    
    )

    # Add event listeners
    scraper.on(Events.DATA, on_data)
    scraper.on(Events.ERROR, on_error)
    scraper.on(Events.END, on_end)

    queries = [
        Query(
            options=QueryOptions(
                limit=10000  # Limit the number of jobs to scrape.            
            )
        ),
        Query(
            query=title,
            options=QueryOptions(
                locations=['United States'],
                apply_link=True,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
                skip_promoted_jobs=True,  # Skip promoted jobs. Default to False.
                page_offset=0,  # How many pages to skip
                limit=10000,
                filters=QueryFilters(
                    company_jobs_url='https://www.linkedin.com/jobs/search/?currentJobId=4318116698&f_C=1035%2C1418841%2C165397%2C1386954%2C3763403%2C3290211%2C10073178%2C3238203%2C2270931%2C3641570%2C263515%2C1148098%2C5097047%2C589037%2C3178875%2C692068%2C18086638%2C19537%2C19053704%2C1889423%2C30203%2C5607466%2C11206713%2C2446424&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4318116698%2C4318725547%2C4308926318%2C4308911708%2C4315882404%2C4316549053%2C4318185831%2C4312941016%2C4312929683',  # Filter by companies.                
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                    on_site_or_remote=[OnSiteOrRemoteFilters.REMOTE],
                    experience=[ExperienceLevelFilters.MID_SENIOR],
                    base_salary=SalaryBaseFilters.SALARY_100K
                )
            )
        ),
    ]

    scraper.run(queries)

    sleep(random.randint(60, 240))  # Wait for a few seconds before starting the next title scrape to avoid rate limiting
