from playwright.sync_api import sync_playwright
import json
import sys
import os
import argparse

def scrape_linkedin_job(url: str, session_path: str = "linkedin_session.json", login_mode: bool = False):
    with sync_playwright() as p:
        # Launch browser. Login mode needs a visible window.
        browser = p.chromium.launch(headless=not login_mode)
        
        # Realistic User Agent
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        
        # Create context with optional storage state
        context_args = {"user_agent": ua}
        if os.path.exists(session_path) and not login_mode:
            context_args["storage_state"] = session_path
            print(f"Using existing session from: {session_path}")
            
        context = browser.new_context(**context_args)
        page = context.new_page()

        if login_mode:
            print("\n🔐 Login Mode Active")
            print("1. Please log in to LinkedIn in the browser window.")
            print("2. Once logged in and on the homepage/feed, come back here.")
            page.goto("https://www.linkedin.com/login")
            
            input("\nPress Enter here after you have successfully logged in...")
            
            print(f"Saving session to: {session_path}...")
            context.storage_state(path=session_path)
            print("✅ Session saved!")
            browser.close()
            return None

        print(f"Navigating to: {url}...")
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        
        job_info = {
            "url": url,
            "title": None,
            "company": None,
            "location": None,
            "description": None
        }
        
        try:
            # Check if we are being redirected to a login page
            if "login" in page.url:
                print("⚠️ Warning: Redirected to login page. Please run with --login first.")
            
            # Wait for the main title
            page.wait_for_selector(".top-card-layout__title, .job-details-jobs-unified-top-card__job-title", timeout=10000)
            
            # Dismiss overlays
            for selector in ["button[aria-label='Dismiss']", "button[aria-label='Close']", "button:has-text('Accept')"]:
                try:
                    loc = page.locator(selector)
                    if loc.count() > 0 and loc.first.is_visible():
                        loc.first.click(timeout=1000)
                except Exception: pass

            # Click "Show more"
            show_more_selector = "button[data-tracking-control-name='public_jobs_show-more-html-btn'], button.jobs-description__footer-button"
            show_more_btn = page.locator(show_more_selector)
            if show_more_btn.count() > 0:
                try:
                    show_more_btn.first.click(timeout=5000)
                    page.wait_for_timeout(500)
                except Exception:
                    page.evaluate("() => document.querySelector('.jobs-description__footer-button')?.click()")

            # Extract info (Public vs Logged-in selectors)
            # Title
            title_loc = page.locator(".top-card-layout__title, .job-details-jobs-unified-top-card__job-title")
            if title_loc.count() > 0:
                job_info["title"] = title_loc.first.inner_text().strip()
            
            # Company
            company_loc = page.locator(".topcard__flavor a, .job-details-jobs-unified-top-card__company-name a")
            if company_loc.count() > 0:
                job_info["company"] = company_loc.first.inner_text().strip()
            
            # Location
            location_loc = page.locator(".topcard__flavor--bullet, .job-details-jobs-unified-top-card__bullet")
            if location_loc.count() > 0:
                job_info["location"] = location_loc.first.inner_text().strip()
            
            # Description
            desc_loc = page.locator(".show-more-less-html__markup, .jobs-description__content")
            if desc_loc.count() > 0:
                job_info["description"] = desc_loc.first.inner_text().strip()
                
        except Exception as e:
            print(f"Extraction failed: {e}")
        finally:
            browser.close()
            
        return job_info

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LinkedIn Job Scraper with Session Support")
    parser.add_argument("url", nargs="?", help="LinkedIn job URL to scrape")
    parser.add_argument("--login", action="store_true", help="Open browser to log in manually and save session")
    parser.add_argument("--session", default="linkedin_session.json", help="Path to session JSON file")
    
    args = parser.parse_args()

    if args.login:
        scrape_linkedin_job("", session_path=args.session, login_mode=True)
    elif args.url:
        result = scrape_linkedin_job(args.url, session_path=args.session)
        print("\n--- Extracted Job Information ---")
        print(json.dumps(result, indent=2))
    else:
        parser.print_help()

