import requests
import time
import re
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime

from scraper.utils.data_models import ContactInfo
from scraper.utils.selenium_utils import init_selenium_driver
from selenium.webdriver.common.by import By

class ProgramWorker:
    def __init__(self, scraper):
        self.scraper = scraper
        
    def get_college_program_urls(self):
        """
        Scrapes the main navigation to get college and program URLs
        Returns a dictionary of college names to lists of program URLs
        """
        try:
            self.scraper.logger.info(f"Scraping college/program URLs from {self.scraper.base_url}")
            response = requests.get(self.scraper.base_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the colleges dropdown menu
            main_menu = soup.find('ul', class_='nav navbar-nav menu-main-menu') 
            if not main_menu:
                raise ValueError("Main menu not found")
            academics_li = main_menu.find_all('li', recursive=False)[2]  #Academics is the third option in the main menu
            if not academics_li or not academics_li.find('a', string='Academics'):
                raise ValueError("Academics menu not found")
            academics_menu = academics_li.find('ul', recursive=False)
            if not academics_menu:
                raise ValueError("Academics menu not found")
            colleges_li = academics_menu.find_all('li', recursive=False)[0]  #Colleges is the first option in the academics menu
            if not colleges_li or not colleges_li.find('a', string='Colleges'):
                raise ValueError("Colleges menu not found")
            colleges_menu = colleges_li.find('ul', recursive=False)
            if not colleges_menu:
                raise ValueError("Colleges menu not found")
            
            college_programs = {}
            for college_li in colleges_menu.find_all('li', recursive=False):
                program_menu = college_li.find('ul', recursive=False)
                college_name = college_li.find('a').text.strip()
                program_urls = []
                
                if program_menu:
                    for program_li in program_menu.find_all('li', recursive=False):
                        program_url = program_li.find('a')['href']
                        if program_url:
                            program_name = program_li.find('a').text.strip()
                            program_url = urllib.parse.urljoin(self.scraper.base_url, program_url)
                            # Add tuple of college name and program URL to queue instead of storing
                            self.scraper.program_queue.put((college_name, program_name, program_url))
                            program_urls.append(program_url)
                    college_programs[college_name] = program_urls
                    self.scraper.update_stats("college")

            total_urls = sum(len(programs) for programs in college_programs.values())
            self.scraper.logger.success(f"Found {total_urls} programs to process")
            return college_programs
            
        except Exception as e:
            self.scraper.logger.error(f"Error getting college/program URLs: {str(e)}")
            return {}
            
    def get_faculty_page(self, program_url, program_name):
        """Scrapes a program page to find the faculty directory link"""
        try:
            response = requests.get(program_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            faculty_links = soup.find_all('a', href=lambda href: href and 'faculty' in href.lower())
            if not faculty_links:
                raise ValueError("No faculty links found!")
            
            for link in faculty_links:
                if 'faculty profile' in link.text.lower():
                    url = urllib.parse.urljoin(self.scraper.base_url, link['href'])
                    
                    with self.scraper.faculty_url_lock:
                        if url in self.scraper.processed_faculty_urls:
                            # If we've seen this URL before but not for this program
                            if program_name not in self.scraper.processed_faculty_urls[url]:
                                self.scraper.processed_faculty_urls[url].add(program_name)
                                return url  # Process it again for the new program
                            else:
                                self.scraper.logger.info(f"Skipping duplicate faculty URL for {program_name}: {url}")
                                return None
                        else:
                            # First time seeing this URL
                            self.scraper.processed_faculty_urls[url] = {program_name}
                            return url
                    
            raise ValueError("No working faculty links found!")
            
        except Exception as e:
            self.scraper.logger.error(f"Error finding faculty page in {program_url}: {str(e)}")
            return None
            
    def process(self, thread_name):
        """Worker that processes program pages to find faculty directory links"""
        self.scraper.logger.info(f"Starting {thread_name}")
        while self.scraper.active and datetime.now() < self.scraper.end_time:
            try:
                item = self.scraper.program_queue.get(block=True)
                if item is None:
                    break 
                
                college_name, program_name, program_url = item
                self.scraper.update_stats("program")
                self.scraper.logger.info(f" {thread_name}: Scraping faculty directory link from {program_url}")
                faculty_url = self.get_faculty_page(program_url, program_name)
                if not faculty_url:
                    raise ValueError(f"No faculty URL found in {program_url}")
                self.scraper.update_stats("faculty_url_success", program_name)
                self.scraper.logger.success(f"Found faculty URL: {faculty_url}")
                self.scraper.directory_queue.put((college_name, program_name, faculty_url))
                self.scraper.program_queue.task_done()
            except Exception as e:
                if not e:
                    self.scraper.logger.error(f"Error in {thread_name}: could not connect to {program_url}")
                self.scraper.update_stats("faculty_url_failure", program_name)
                self.scraper.logger.error(f"Error in {thread_name}: {str(e)}")
        if self.scraper.program_queue.empty():
            self.scraper.logger.info(f"{thread_name}: Cleaning up and exiting program worker after processing all program pages.")
        else:
            self.scraper.logger.info(f"{thread_name}: Cleaning up and exiting program worker with {self.scraper.program_queue.qsize()} program pages remaining.")


class DirectoryWorker:
    def __init__(self, scraper):
        self.scraper = scraper
        
    def scrape_directory_page(self, url, college_name, program_name):
        """Scrapes the directory/list page and returns profile URLs"""
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            contacts = []
            worker_elements = []
            
            # Layout 1: Computer Programs with specific div IDs
            computer_programs = {
                "Computer Technology": "CT",
                "Information Technology": "IT",
                "Software Technology": "ST"
            }
            
            if program_name in computer_programs:
                program_id = computer_programs[program_name]
                start_div = soup.find('div', id=program_id)

                if start_div:
                    all_faculty_elements = []
                    current = start_div.find_next_sibling()
                    
                    while current:
                        if current.get('id') in computer_programs.values():
                            break
                            
                        if (current.name == 'div' and 
                            isinstance(current.get('class'), list) and 
                            'vc_row' in current.get('class') and 
                            'wpb_row' in current.get('class') and 
                            'vc_row-fluid' in current.get('class')):
                            
                            faculty_in_container = current.find_all(
                                'div',
                                class_=['wpb_column', 'vc_column_container', 'vc_col-sm-4']
                            )
                            all_faculty_elements.extend(faculty_in_container)
                        
                        current = current.find_next_sibling()
                    
                    worker_elements = all_faculty_elements

            # Layout 2: Direct class-based layout
            if not worker_elements:
                worker_elements = soup.find_all('div', class_=['wpb_column', 'vc_column_container', 'vc_col-sm-4'])

            # Layout 3: Alternative layout with div main
            if not worker_elements:
                worker_elements = soup.find_all('div', class_=["wpb_text_column", "wpb_content_element"])
       
            if not worker_elements:
                raise ValueError(f"No faculty members found for {program_name} in any layout!")

            # Process found elements
            for element in worker_elements:
                profile_link = element.find('a')
                if profile_link and profile_link.text.strip() != 'Faculty Profiles' and profile_link.text.strip() is not None:
                    if profile_link and profile_link.text.strip() != 'Faculty Profiles':
                        profile_url = profile_link.get('href', '').strip()
                        full_name = profile_link.text.strip()
                        
                        if not full_name or not profile_url:
                            continue

                        # Check if we should process this profile for this program
                        with self.scraper.faculty_url_lock:
                            if profile_url in self.scraper.processed_faculty_urls:
                                if program_name in self.scraper.processed_faculty_urls[profile_url]:
                                    continue  # Skip if already processed for this program
                                else:
                                    self.scraper.processed_faculty_urls[profile_url].add(program_name)
                            else:
                                self.scraper.processed_faculty_urls[profile_url] = {program_name}

                        contact = ContactInfo(
                            name=full_name,
                            office=college_name,
                            department=program_name,
                            profile_url=profile_url
                        )
                        contacts.append(contact)
                        self.scraper.update_stats("personnel_found", program_name)

            if not contacts:
                raise ValueError(f"No contacts found for {program_name}!")
            
            self.scraper.logger.success(f"Found {len(contacts)} contacts for {program_name}")
            return contacts

        except Exception as e:
            self.scraper.logger.error(f"Error parsing directory page {url} for {program_name}: {str(e)}")
            return []
            
    def process(self, thread_name):
        """Worker that processes directory pages"""
        self.scraper.logger.success(f"Starting {thread_name}")
        while self.scraper.active:
            try:
                item  = self.scraper.directory_queue.get(block=True)
                if item is None:
                    break
                college_name, program_name, url = item
                
                contacts = self.scrape_directory_page(url, college_name, program_name)
                self.scraper.logger.info(f" {thread_name}: Processing {url}")
                if not contacts:
                    raise ValueError(f"No contacts found in {url}")
                self.scraper.logger.success(f"Found {len(contacts)} contacts in {url}")
                for contact in contacts:
                    self.scraper.profile_queue.put(contact)
                self.scraper.directory_queue.task_done()
            except Exception as e:
                if not e:
                    self.scraper.logger.error(f"Error in {thread_name}: could not connect to {url}")
                self.scraper.logger.error(f"Error in {thread_name}: {str(e)}")
        if(self.scraper.directory_queue.empty()):
            self.scraper.logger.info(f"{thread_name}: Cleaning up and exiting directory worker after processing all faculty pages.")
        else:
            self.scraper.logger.info(f"{thread_name}: Cleaning up and exiting directory worker with {self.scraper.directory_queue.qsize()} faculty pages remaining")


class ProfileWorker:
    def __init__(self, scraper):
        self.scraper = scraper
        
    def scrape_profile_page(self, driver, contact):
        """Scrapes an individual profile page to get the email"""
        try:
            driver.get(contact.profile_url)
            time.sleep(2)
            try:
                """Regex solution to scrape the emails from the profile page."""
                page_text = driver.find_element(By.TAG_NAME, "body").text  # Get full visible text

                # Use regex to find emails
                email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                emails = re.findall(email_pattern, page_text)
                if len(emails) > 0:
                    contact.email = emails[0]
                return contact
            except Exception as e:
                self.scraper.logger.error(f"Could not parse email for {contact.profile_url}: {str(e)}")
                return contact
            
        except Exception as e:
            self.scraper.logger.error(f"Error parsing profile page {contact.profile_url}: {str(e)}")
            return contact
            
    def process(self, thread_name):
        self.scraper.logger.info(f"Starting {thread_name}")
        driver = init_selenium_driver()
        """Worker that processes individual profile pages"""
        while self.scraper.active:
            try:
                contact = self.scraper.profile_queue.get(block=True)
                if contact is None:
                    break
                updated_contact = self.scrape_profile_page(driver, contact)
                if not updated_contact.email:  # Only save if we found an email
                    raise ValueError(f"No email found in {updated_contact.profile_url}")
                
                self.scraper.result_queue.put(updated_contact)
                self.scraper.update_stats("complete_record", updated_contact.department)
                self.scraper.logger.success(f"{thread_name} Found email: {updated_contact.email}")
                self.scraper.profile_queue.task_done()
            except Exception as e:
                if not e:
                    self.scraper.logger.error(f"Error in {thread_name}: could not connect to {contact.profile_url}")
                self.scraper.update_stats("incomplete_record", contact.department)
                self.scraper.logger.error(f"Error in {thread_name}: {str(e)}")
        driver.quit()
        if(self.scraper.profile_queue.empty()):
            self.scraper.logger.info(f"{thread_name}: Cleaning up and exiting profile worker after processing all profiles")
        else:
            self.scraper.logger.info(f"{thread_name}: Cleaning up and exiting profile worker with {self.scraper.profile_queue.qsize()} profiles remaining")