import asyncio
from playwright.async_api import async_playwright
import os

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state="linkedin_session.json")
        page = await context.new_page()
        
        print("Navigating to job page...")
        await page.goto("https://www.linkedin.com/jobs/view/4374244548", wait_until="domcontentloaded")
        
        # Wait a bit for dynamic content
        await page.wait_for_timeout(3000)
        
        title_count = await page.locator(".job-details-jobs-unified-top-card__job-title, .top-card-layout__title, h1").count()
        print(f"Found {title_count} possible title elements")
        
        if title_count > 0:
            titles = await page.locator(".job-details-jobs-unified-top-card__job-title, .top-card-layout__title, h1").all_text_contents()
            for i, t in enumerate(titles):
                print(f"Title {i}: {t.strip()}")
                
        # Get outer HTML for debugging if we can't find it
        content = await page.content()
        with open("job_page.html", "w") as f:
            f.write(content)
            
        print("Done. Saved HTML to job_page.html")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
