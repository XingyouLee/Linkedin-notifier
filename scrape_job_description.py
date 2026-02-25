import argparse
import asyncio
import json
import sys
import os

# 把本地的 linkedin_scraper repo 添加到环境变量
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'linkedin_scraper')))

# 引入 repo 中的类
from linkedin_scraper import BrowserManager, login_with_credentials
from linkedin_scraper.scrapers.job import JobScraper
from dotenv import load_dotenv

load_dotenv()

async def login_and_save(session_path: str):
    print("\n🔐 Auto Login Mode Active")
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")
    
    if not email or not password:
        print("❌ Error: LINKEDIN_EMAIL and LINKEDIN_PASSWORD must be set in .env file")
        return

    # non-headless mode is generally safer to avoid bot detection, but headless works too
    async with BrowserManager(headless=False) as browser:
        print(f"Logging in as {email}...")
        try:
            await login_with_credentials(
                browser.page,
                email=email,
                password=password
            )
            print(f"Saving session to: {session_path}...")
            await browser.save_session(session_path)
            print("✅ Session saved successfully!")
        except Exception as e:
            print(f"❌ Login failed: {e}")

async def scrape_job(url: str, session_path: str = "linkedin_session.json"):
    # Always set headless=False to avoid detection and see what happens, can be changed to True in production
    async with BrowserManager(headless=False) as browser:
        if os.path.exists(session_path):
            await browser.load_session(session_path)
            print(f"✓ Using existing session from: {session_path}")
        else:
            print(f"⚠️ Warning: Session file not found at {session_path}.")
            print("You might be redirected to login. Consider running with --login first.")
            
        print(f"📄 Scraping job details from: {url}...")
        job_scraper = JobScraper(browser.page)
        
        try:
            job = await job_scraper.scrape(url)
            
            result = {
                "title": job.job_title,
                "company": job.company,
                "location": job.location,
                "description": job.job_description,
                "url": url
            }
            print("\n--- Extracted Job Information ---")
            print(json.dumps(result, indent=2, ensure_ascii=False))
            
        except Exception as e:
            print(f"Extraction failed: {e}")

async def main():
    parser = argparse.ArgumentParser(description="LinkedIn Job Scraper using the local linkedin_scraper repo")
    parser.add_argument("url", nargs="?", help="LinkedIn job URL to scrape")
    parser.add_argument("--login", action="store_true", help="Open browser to log in manually and save session")
    parser.add_argument("--session", default="linkedin_session.json", help="Path to session JSON file")
    
    args = parser.parse_args()

    if args.login:
        await login_and_save(args.session)
    elif args.url:
        await scrape_job(args.url, args.session)
    else:
        parser.print_help()

if __name__ == "__main__":
    asyncio.run(main())
