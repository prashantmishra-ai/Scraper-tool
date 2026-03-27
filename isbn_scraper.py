from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import StaleElementReferenceException, UnexpectedAlertPresentException
import pandas as pd
import time
import random
import os
import csv

service = Service()
driver = webdriver.Firefox(service=service)

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
search_input.send_keys("1")

# Step 4: Click Search button
search_button = driver.find_element(By.ID, "btnSearchIsbnRptNew")
search_button.click()

time.sleep(3)

# ── Heavy Duty Configuration ─────────────────────────────────────────

# WE MUST USE CSV: Excel has a hard limit of 1,048,576 rows.
# 34k pages x 50 rows = 1.7 million rows, which will CRASH Excel completely!
output_csv = "/Users/innovativus/Downloads/isbn_full_data.csv"

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

print("🚀 Starting Heavy-Duty Scraping...")
print(f"💾 Data will continually save to {output_csv} to prevent memory crashes.")

# Step 5: Scrape all pages
try:
    while True:

        # Add a retry loop for each page to handle StaleElementReferenceException
        while True:
            try:
                time.sleep(2) # Give DataTables time to finish updating DOM
                
                rows = driver.find_elements(By.XPATH, "//table[@id='examplenew']/tbody/tr")
                page_data = []

                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    page_data.append([c.text.strip() for c in cols])

                # HEAVY DUTY UPGRADE: Save directly to disk instantly
                # This drops memory usage to 0 MB and prevents lost data on crashes!
                with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for item in page_data:
                        if item:
                            writer.writerow(item)
                            total_collected += 1

                break # Success, break out of retry loop
            except StaleElementReferenceException:
                # Table is refreshing. Wait and retry scraping this page.
                time.sleep(1)
                continue

        print(f"✅ Page {page_num} scraped. Records perfectly flushed to disk: {total_collected}")

        # Try clicking Next button
        try:
            next_btn = driver.find_element(By.ID, "examplenew_next")

            if "disabled" in next_btn.get_attribute("class"):
                print("🏁 Reached the last page!")
                break

            # Use JS click to avoid 'element not interactable' errors
            driver.execute_script("arguments[0].click();", next_btn)
            page_num += 1
            
            # CRITICAL TO AVOID ALERTS: Add 2-5 second delay between clicks
            # The government server WILL block you at 140 pages without this!
            delay = random.uniform(2, 5)
            time.sleep(delay)

        except Exception as e:
            print(f"⚠️ Could not click Next. Finished or interrupted: {e}")
            break

except UnexpectedAlertPresentException as e:
    print(f"\n⚠️ Server explicitly blocked connection (Rate Limit/Timeout).")
    print(f"   Alert text: {e.alert_text}")
    print(f"   Do not worry: ALL {total_collected} records were already safely saved to CSV!")
except Exception as e:
    print(f"\n⚠️ Scraping stopped unexpectedly: {e}")
    print(f"   Do not worry: ALL {total_collected} records were already safely saved to CSV!")

print(f"\n✅ Completely Finished! Result saved at: {output_csv}")
driver.quit()
