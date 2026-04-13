from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import StaleElementReferenceException, UnexpectedAlertPresentException, WebDriverException, TimeoutException
import pandas as pd
import time
import random
import os
import csv

# ── Heavy Duty Configuration ─────────────────────────────────────────

# WE MUST USE CSV: Excel has a hard limit of 1,048,576 rows.
# 34k pages x 50 rows = 1.7 million rows, which will CRASH Excel completely!
output_csv = "isbn_full_data.csv"

expected_columns = [
    "#", "Book Title", "ISBN", "Product Form", "Language",
    "Applicant Type", "Name of Publishing Agency/Publisher",
    "Imprint", "Name of Author/Editor", "Publication Date"
]

# Write headers if file doesn't exist
if not os.path.exists(output_csv):
    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(expected_columns)

page_num = 1
total_collected = 0

def run_scraper(start_page):
    """
    Runs the scraper starting from a specific page.
    Returns (last_successful_page, status_code)
    Status codes: 
        - "FINISHED": Reached the last page
        - "BLOCKED": Server blocked/rate limit alert
        - "ERROR": Any other unexpected exception
    """
    driver = None
    page_num = start_page
    total_collected_this_run = 0

    try:
        service = Service()
        driver = webdriver.Firefox(service=service)
        driver.set_page_load_timeout(120)  # Wait max 2 minutes for page loads
        driver.set_script_timeout(120)     # Wait max 2 minutes for scripts
        driver.get("https://isbn.gov.in/Home/IsbnSearch")
        wait = WebDriverWait(driver, 15)

        # Step 1: Click Search Type dropdown
        search_type_dropdown = wait.until(
            EC.element_to_be_clickable((By.ID, "drpSearchType"))
        )
        search_type_dropdown.click()

        # Step 2: Select ISBN Number / Series option
        isbn_series_option = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//option[contains(text(),'Series')]")
            )
        )
        isbn_series_option.click()

        # Step 3: Wait for input box and enter value 1
        search_input = wait.until(
            EC.presence_of_element_located((By.ID, "txtsearch"))
        )
        search_input.clear()
        search_input.send_keys("978")

        # Step 4: Click Search button
        search_button = driver.find_element(By.ID, "btnSearchIsbnRptNew")
        search_button.click()

        print(f"\n⏳ Waiting for server to fetch 2.8 million records... this can take 5-10 seconds.")
        # Wait until the 'examplenew_info' element is present and populated with text
        wait.until(lambda d: d.find_element(By.ID, "examplenew_info").text.strip() != "")
        time.sleep(1) # Extra buffer for stability

        # --- DIRECT PAGE JUMP LOGIC START ---
        if page_num > 1:
            print(f"\n⌛ Fast-forwarding directly to Page {page_num}... Please wait.")
            try:
                # Use DataTables API to change page (DataTables is 0-indexed)
                # It's crucial we wait until the initial data is fully rendered before injecting this.
                driver.execute_script(
                    "$('#examplenew').dataTable().fnPageChange(arguments[0]);", 
                    page_num - 1
                )
                
                # Wait for the "Showing X to Y" text to update to ensure the jump actually finished
                def jump_completed(d):
                    info_text = d.find_element(By.ID, "examplenew_info").text
                    # Check if the first row number displayed matches our target page math
                    # Use "{:,}" to add commas because DataTables displays "Showing 149,951 to..."
                    expected_start = "{:,}".format((page_num - 1) * 50 + 1)
                    return expected_start in info_text

                wait.until(jump_completed)
                time.sleep(1)  # Extra second for table rows to visually attach to DOM
                print(f"🚀 Jumped successfully to Page {page_num}!")
            except Exception as e:
                print(f"⚠️ Could not jump directly (Timeout or Server error): {e}")
                return page_num, "ERROR"
        # --- DIRECT PAGE JUMP LOGIC END ---

        print(f"\n🚀 Starting Scraper from Page {page_num}...")

        # Step 5: Scrape all pages
        while True:
            # Retry loop for StaleElementReferenceException
            for attempt in range(3):
                try:
                    time.sleep(1)  # Give DataTables time to finish updating DOM
                    rows = driver.find_elements(By.XPATH, "//table[@id='examplenew']/tbody/tr")
                    page_data = []

                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        page_data.append([c.text.strip() for c in cols])

                    # Save directly to disk instantly
                    with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        for item in page_data:
                            if item:
                                writer.writerow(item)
                                total_collected_this_run += 1
                    
                    print(f"✅ Page {page_num} scraped. Records perfectly flushed to disk.")
                    break  # Success
                except StaleElementReferenceException:
                    if attempt == 2: raise
                    time.sleep(2)
                    continue

            # Try clicking Next button
            try:
                next_btn = driver.find_element(By.ID, "examplenew_next")
                if "disabled" in next_btn.get_attribute("class"):
                    print("🏁 Reached the last page!")
                    return page_num, "FINISHED"

                # Use JS click to avoid 'element not interactable' errors
                driver.execute_script("arguments[0].click();", next_btn)
                page_num += 1
                
                # Randomized delay between clicks
                time.sleep(random.uniform(0.5, 2))
            except Exception as e:
                print(f"⚠️ Could not click Next: {e}")
                return page_num, "ERROR"

    except UnexpectedAlertPresentException as e:
        print(f"\n⚠️ Server explicitly blocked connection (Rate Limit/Timeout).")
        print(f"   Alert text: {e.alert_text}")
        return page_num, "BLOCKED"
    except (WebDriverException, Exception) as e:
        print(f"\n⚠️ Scraping stopped unexpectedly on page {page_num}: {e}")
        return page_num, "ERROR"
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

if __name__ == "__main__":
    try:
        user_val = input("\n🔢 Enter the page number to START from (default: 1): ").strip()
        current_page = int(user_val) if user_val else 1
    except ValueError:
        print("⚠️ Invalid input, starting from page 1.")
        current_page = 1

    while True:
        last_page_attempted, status = run_scraper(current_page)
        
        if status == "FINISHED":
            print(f"\n✅ All pages successfully scraped up to {last_page_attempted}!")
            break
        elif status == "BLOCKED":
            wait_time = 30 # 30 seconds
            print(f"\n🛑 Scraper blocked at Page {last_page_attempted}. Waiting {wait_time} seconds before auto-restart...")
            current_page = last_page_attempted
            time.sleep(wait_time)
            print(f"\n♻️ Auto-restarting from Page {current_page}...")
        elif status == "ERROR":
            wait_time = 15 # 15 seconds
            print(f"\n❌ Error occurred at Page {last_page_attempted}. Retrying in {wait_time} seconds...")
            current_page = last_page_attempted
            time.sleep(wait_time)
            print(f"\n♻️ Retrying Page {current_page}...")
        else:
            print(f"\n❓ Unknown status '{status}' at page {last_page_attempted}. Exiting.")
            break

