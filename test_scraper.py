import asyncio
from playwright.async_api import async_playwright
import json

async def scrape():
    async with async_playwright() as p:
        # Launch browser in non-headless mode since LinkedIn heavily blocks headless
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            storage_state="linkedin_session.json"
        )
        
        page = await context.new_page()
        print("Navigating...")
        await page.goto("https://www.linkedin.com/jobs/view/4374244548", wait_until="domcontentloaded")
        
        print("Waiting for content to load...")
        # Wait a bit for JS to render
        await page.wait_for_timeout(5000)
        
        # Check if we got redirected to login or authwall
        print(f"Current URL: {page.url}")
        
        # Take a screenshot to see what's actually rendering
        await page.screenshot(path="job_page.png")
        print("Saved screenshot to job_page.png")
        
        # Try to extract the title
        title_selectors = [
            "h1",
            ".job-details-jobs-unified-top-card__job-title",
            ".top-card-layout__title",
            "h2.top-card-layout__title",
            "[data-test-job-title]"
        ]
        
        print("\nChecking title selectors:")
        for sel in title_selectors:
            count = await page.locator(sel).count()
            if count > 0:
                text = await page.locator(sel).first.inner_text()
                print(f"Found with '{sel}': {text.strip()}")
            else:
                print(f"Not found: {sel}")
                
        # Try to extract description
        desc_selectors = [
            "article",
            ".jobs-description__content",
            ".show-more-less-html__markup",
            "[class*='description']",
            "div.job-details-module__content"
        ]
        
        print("\nChecking description selectors:")
        for sel in desc_selectors:
            count = await page.locator(sel).count()
            if count > 0:
                text = await page.locator(sel).first.inner_text()
                print(f"Found with '{sel}': {len(text)} chars (starts with: {text[:50].strip()}...)")
            else:
                print(f"Not found: {sel}")
                
        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape())
