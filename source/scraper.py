# import libraries
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import time

# import os


NFR_LINK = 'https://hfr.health.gov.ng/facilities/hospitals-search?_token=' + \
           '4Wll44OzOr1kZWrvOzm7FGC1y3zCYbgGs99vHRSf&state_id=124&ward_id=0&facility_level_id=0&ownership_id=' + \
           '0&operational_status_id=1&registration_status_id=2&license_status_id=1&geo_codes=0&service_type=' + \
           '0&service_category_id=0&entries_per_page=20&page='

FEATURE_RULE_DETAIL = {
    # Identifiers
    'Facility_Code': {'tag': 'div', "id": "unique_id"},
    'State_Unique_ID': {'tag': 'div', "id": "state_unique_id"},
    'Registration_No': {'tag': 'div', "id": "registration_no"},
    'Facility_Name': {'tag': 'div', "id": "facility_name"},
    'Alternate_Name': {'tag': 'div', "id": "alt_facility_name"},
    'Start_Date': {'tag': 'div', "id": "start_date"},
    'Ownership': {'tag': 'div', "id": "ownership"},
    'Ownership_Type': {'tag': 'div', "id": "ownership_type"},
    'Facility_Level': {'tag': 'div', "id": "facility_level"},
    'Facility_Level_Option': {'tag': 'div', "id": "facility_level_option"},
    'Days_of_Operation': {'tag': 'div', "id": "operational_days"},
    'Hours_of_Operation': {'tag': 'div', "id": "operational_hours"},
    # Location
    'State': {'tag': 'div', "id": "state"},
    'LGA': {'tag': 'div', "id": "lga"},
    'Ward': {'tag': 'div', "id": "ward"},
    'Physical_Location': {'tag': 'div', "id": "physical_location"},
    'Postal_Address': {'tag': 'div', "id": "postal_address"},
    'Longitude': {'tag': 'div', "id": "longitude"},
    'Latitude': {'tag': 'div', "id": "latitude"},
    # Contacts
    'Phone_Number': {'tag': 'div', "id": "phone_number"},
    'Alternate_Number': {'tag': 'div', "id": "alternate_number"},
    'Email_Address': {'tag': 'div', "id": "email_address"},
    'Website': {'tag': 'div', "id": "website"},
    # Status
    'Operational_Status': {'tag': 'div', "id": "operation_status"},
    'Registration_Status': {'tag': 'div', "id": "registration_status"},
    'License_Status': {'tag': 'div', "id": "license_status"},
    # Services
    'Out_Patient_Services': {'tag': 'div', "id": "outpatient"},
    'In_Patient_Services': {'tag': 'div', "id": "inpatient"},
    'Medical_Services': {'tag': 'div', "id": "medical"},
    'Surgical_Services': {'tag': 'div', "id": "surgical"},
    'OG_Services': {'tag': 'div', "id": "gyn"},
    'Pediatrics_Services': {'tag': 'div', "id": "pediatrics"},
    'Dental_Services': {'tag': 'div', "id": "dental"},
    'SC_Services': {'tag': 'div', "id": "specialservice"},
    'Total_number_of_Beds': {'tag': 'div', "id": "beds"},
    'Onsite_Laboratory': {'tag': 'div', "id": "onsite_laboratory"},
    'Onsite_Imaging': {'tag': 'div', "id": "onsite_imaging"},
    'Onsite_Pharmacy': {'tag': 'div', "id": "onsite_pharmarcy"},
    'Mortuary_Services': {'tag': 'div', "id": "mortuary_services"},
    'Ambulance_Services': {'tag': 'div', "id": "ambulance_services"},
    # Personnel
    'Number_of_Doctors': {'tag': 'div', "id": "doctors"},
    'Number_of_Pharmacists': {'tag': 'div', "id": "pharmacists"},
    'Number_of_PT': {'tag': 'div', "id": "pharmacy_technicians"},
    'Number_of_Dentists': {'tag': 'div', "id": "dentist"},
    'Number_of_DT': {'tag': 'div', "id": "dental_technicians"},
    'Number_of_Nurses': {'tag': 'div', "id": "nurses"},
    'Number_of_Midwifes': {'tag': 'div', "id": "midwifes"},
    'Number_of_N/M': {'tag': 'div', "id": "nurse_midwife"},
    'Number_of_LT': {'tag': 'div', "id": "lab_technicians"},
    'Number_of_LS': {'tag': 'div', "id": "lab_scientists"},
    'HIM_Officers': {'tag': 'div', "id": "him_officers"},
    'Number_of_CHO': {'tag': 'div', "id": "community_health_officer"},
    'Number_of_CHEW': {'tag': 'div', "id": "community_extension_workers"},
    'Number_of_JCHEW': {'tag': 'div', "id": "jun_community_extension_worker"},
    'Number_of_EHO': {'tag': 'div', "id": "env_health_officers"},
    'Number_of_HA': {'tag': 'div', "id": "attendants"}
}


def chrome():
    """ A function to instantiate chrome driver

    :returns:
        driver - the driver object instantiated.
    """
    #  Headless mode
    chrome_option = Options()
    chrome_option.add_argument("--headless")

    # disabling unwanted messages printed while running with an headless browser
    chrome_option.add_argument("--log-level=3")
    browser = Chrome(options=chrome_option)
    return browser


def extract_facility_details(soup):
    """
    gets the details of each hospital/extract each feature value

    Parameter
    -------------
    soup: beautifulsoup
        This is the bs4 object containing the DOM of the website
    """
    # feature dictionary        
    features_dict = {}

    for col in FEATURE_RULE_DETAIL:
        if col in ["Medical_Services", "Surgical_Services", "OG_Services", "Pediatrics_Services",
                   "Dental_Services", "SC_Services"]:
            try:
                features_dict[col] = [i.text for i in soup.findChild(FEATURE_RULE_DETAIL[col]["tag"],
                                                                     {"id": FEATURE_RULE_DETAIL[col]['id']}).find_all(
                    "span")]
            except:
                features_dict[col] = np.NAN

        else:
            try:
                features_dict[col] = soup.find(FEATURE_RULE_DETAIL[col]["tag"],
                                               {"id": FEATURE_RULE_DETAIL[col]['id']}).text
            except:
                features_dict[col] = np.NAN

    return features_dict


def extract_soup_js_and_process_page_details(listing_url):
    """ 
    Extracts HTML from JS pages: open, wait, click, wait, extract

    Parameter
    ------------
    listing_url: String
        This is the website url

    Return
    -------------
    entries_list: list
        list of dictionaries for facility data entries
    """

    driver = chrome()
    # to hold features_dict
    dicts_list = []

    # number of listings in a page
    listings_per_page = 20

    time.sleep(2)
    # run till the website status is good
    while True:

        try:
            driver.get(listing_url)
            time.sleep(2)
            break
        except:
            continue

    for hosp_idx in range(listings_per_page):

        try:
            # open the view pop up
            driver.find_element(by=By.XPATH, value=f'//*[@id="hosp"]/tbody/tr[{hosp_idx + 1}]/td[9]/a/button').click()
            time.sleep(1)

            # click on services since it's the only one that still uses javascript to display
            driver.find_element(by=By.LINK_TEXT, value="Services").click()

            # parse the browser content into beautiful soup to extract all features
            view_btns = BeautifulSoup(driver.page_source, features="html.parser")

            # extract data
            row_dict = extract_facility_details(view_btns)

            dicts_list.append(row_dict)

            # click on Identifiers again so when the next hospital view is clicked, 
            # it opens to Identifier and the code logic is preserved
            time.sleep(1)
            driver.find_element(by=By.LINK_TEXT, value="Identifiers").click()

            # close the pop up
            time.sleep(1)
            driver.find_element(by=By.XPATH, value='//*[@id="view_details"]/div/div/div[2]/div[2]/button').click()
            time.sleep(2)

        except:
            view_btns = BeautifulSoup('', features='html.parser')

            # call
            row_dict = extract_facility_details(view_btns)
            dicts_list.append(row_dict)

    driver.quit()

    return dicts_list


class Scraper:
    def __init__(self, link=NFR_LINK, out_file="_"):
        self.link = link
        self.out_file = out_file
        # to hold the data entries
        self.entries_list = []

    def build_urls(self):
        """
        Builds links for all pages of the HFR website
        and also xpaths for view button of the facility listed

        """
        # number of pages which url is to be collected
        number_of_pages = 3
        # list to hold the urls
        url_list = []
        for i in range(number_of_pages):
            offset = str(i + 1)
            # build url
            url_pagination = self.link + f'{offset}#'
            url_list.append(url_pagination)
            # make global variable
            self.url_list = url_list

    def parallel_process(self):
        """
        Uses multithreading to retrieve all data
        
        Return
        -------------
        all_data: list
            list of dictionaries for facility data entries
        """
        with ThreadPoolExecutor() as exc:
            results = exc.map(extract_soup_js_and_process_page_details, self.url_list)
            for result in results:
                self.entries_list.extend(result)

    def run(self):
        """
        Performs the following:
        (1) run the other processes and extract the data
        (2) make the collected data dict into dataframe
        (3) save the extracted data into a csv file
        
        Return
        --------------
        df: DataFrame
            The DataFrame object of the scraped data
        
        """
        self.build_urls()

        # log
        t0 = time.time()  # get start time
        print("Scraping Data...(shouldn't take more than 2.2 minutes with good internet)")
        self.parallel_process()

        # make into DataFrame
        df = pd.DataFrame(self.entries_list)
        df.replace("", np.nan, inplace=True)

        # save
        print("Saving scraped data...")
        df.to_csv(self.out_file, index=False)
        print("Scraper Operations Completed after {} minutes!".format((time.time() - t0) / 60))
        print("Scraper output data saved as {}".format(self.out_file))

        return df


if __name__ == "__main__":
    new_extractor = Scraper(out_file='raw_hfr_data.csv')
    new_extractor.run()
