import asyncio
from playwright.async_api import async_playwright
import sys
import os

# Add local package to path
sys.path.append(os.path.abspath('linkedin_scraper'))
from linkedin_scraper.scrapers.job import JobScraper
from linkedin_scraper.core.browser import BrowserManager

async def test():
    async with BrowserManager(headless=False) as browser:
        await browser.load_session("linkedin_session.json")
        
        job_scraper = JobScraper(browser.page)
        print("Scraping job...")
        job = await job_scraper.scrape("https://www.linkedin.com/jobs/view/4374244548")
        
        print("\n--- Scraped Job ---")
        print(f"Title: {job.job_title}")
        print(f"Description length: {len(job.job_description) if job.job_description else 'None'}")
        
if __name__ == "__main__":
    asyncio.run(test())
