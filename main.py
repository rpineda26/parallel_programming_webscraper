import threading
import csv
import time
import requests
import logging
import re
import urllib
import json
# import helper classes
from datetime import datetime, timedelta # for time-limit on execution
from tqdm import tqdm # for progress bars
from queue import Queue # for thread-safe data passing
from colorama import init, Fore, Style # for colored logging in the console
# library for web scraping
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.webdriver import WebDriver
#import types for clean function parameter definitions
from dataclasses import dataclass
from typing import List, Optional, Dict

init(autoreset=True)  # Initialize colorama
# Custom logger with colors
class ColoredLogger(logging.Logger):
    def __init__(self, name):
        super().__init__(name)
        
        # Create custom handler
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter())
        self.addHandler(handler)
        
    def success(self, msg, *args, **kwargs):
        """Add success level logging"""
        self.log(25, msg, *args, **kwargs)  # 25 is between INFO and WARNING

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        # Define color schemes
        colors = {
            'ERROR': Fore.RED,
            'WARNING': Fore.YELLOW,
            'INFO': Fore.WHITE,
            'SUCCESS': Fore.GREEN,
            'DEBUG': Fore.BLUE # Debug is not used as it doesn't show up if logging level is INFO
        }
        
        # Add timestamp and thread name to format
        msg = f"[{record.threadName}] {record.msg}"
        
        # Color code based on level
        level_name = record.levelname
        if level_name in colors:
            # Move cursor up above progress bars, print message, then restore cursor
            colored_msg = f"\033[2F\033[K{colors[level_name]}{msg}{Style.RESET_ALL}\033[2E"
            tqdm.write(colored_msg)
            return ""
        
        return msg

# Add success level to logging
logging.addLevelName(25, 'SUCCESS')
logging.setLoggerClass(ColoredLogger)

#Attributes of a record
@dataclass
class ContactInfo:
    email: Optional[str] = None
    name: Optional[str] = None
    office: Optional[str] = None
    department: Optional[str] = None
    profile_url: Optional[str] = None

"""
This function is used to initialize a selenium driver. Each thread for scraping profile pages will have its own driver.
The email data contained in the profile page is protected with obfuscation and requires JavaScript to render. Scraping
these pages through Selenium will allow us to access the email data after the page has fully loaded.
"""
def init_selenium_driver():
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3") 
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

@dataclass
class ScraperStatistics:
    base_url: str
    num_threads: int
    scrape_time_minutes: int
    colleges_count: int = 0
    programs_visited: int = 0
    total_pages_visited: int = 1  # Start at 1 to count base_url
    total_emails_recorded: int = 0
    programs_with_faculty_url: Dict[str, bool] = None  # Program name -> has working faculty URL
    programs_without_faculty_url: Dict[str, bool] = None  # Program name -> missing/failed faculty URL
    program_personnel_count: Dict[str, int] = None  # Program name faculty -> total personnel found
    program_complete_records: Dict[str, int] = None  # Program name faculty -> complete records
    program_incomplete_records: Dict[str, int] = None  # Program name faculty -> incomplete records

    def __post_init__(self):
        self.programs_with_faculty_url = {}
        self.programs_without_faculty_url = {}
        self.program_personnel_count = {}
        self.program_complete_records = {}
        self.program_incomplete_records = {}

    def to_dict(self):
        return {
            "base_url": self.base_url,
            "num_threads": self.num_threads,
            "scrape_time_minutes": self.scrape_time_minutes,
            "colleges_count": self.colleges_count,
            "programs_visited": self.programs_visited,
            "total_pages_visited": self.total_pages_visited,
            "total_emails_recorded": self.total_emails_recorded,
            "programs_with_faculty_url": dict(self.programs_with_faculty_url),
            "programs_without_faculty_url": dict(self.programs_without_faculty_url),
            "program_personnel_count": {f"{k} Faculty": v for k, v in self.program_personnel_count.items()},
            "program_complete_records": {f"{k} Faculty": v for k, v in self.program_complete_records.items()},
            "program_incomplete_records": {f"{k} Faculty": v for k, v in self.program_incomplete_records.items()}
        }

class NestedWebScraper:
    def __init__(self, url:str, num_threads: int, scrape_time_minutes: int):
        self.num_threads = num_threads
        self.scrape_time_minutes = scrape_time_minutes
        self.end_time = None
        self.program_queue = Queue()    # For list of program pages (generally for colleges)
        self.directory_queue = Queue()  # For list of faculty pages (generally for programs/some faculty pages are per college)
        self.profile_queue = Queue()    # For individual profile pages of each faculty members
        self.result_queue = Queue()
        self.processed_faculty_urls = {} # track processed faculty URLS to avoid duplicates
        self.faculty_url_lock = threading.Lock()
        self.csv_lock = threading.Lock()
        self.active = True
        self.base_url = url
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(ColoredFormatter())
            self.logger.addHandler(handler)

        self.stats = ScraperStatistics(url, num_threads, scrape_time_minutes)
        self.stats_lock = threading.Lock() 

    def update_stats(self, stat_type: str, program_name: str = None):
        """Thread-safe statistics update"""
        with self.stats_lock:
            if stat_type == "page_visit":
                self.stats.total_pages_visited += 1
            elif stat_type == "college":
                self.stats.colleges_count += 1
            elif stat_type == "program":
                self.stats.programs_visited += 1
                self.stats.total_pages_visited += 1  # Count program page visit
            elif stat_type == "faculty_url_success" and program_name:
                self.stats.programs_with_faculty_url[program_name] = True
                self.stats.total_pages_visited += 1
            elif stat_type == "faculty_url_failure" and program_name:
                self.stats.programs_without_faculty_url[program_name] = True
            elif stat_type == "personnel_found" and program_name:
                self.stats.program_personnel_count[program_name] = \
                    self.stats.program_personnel_count.get(program_name, 0) + 1
            elif stat_type == "complete_record" and program_name:
                self.stats.program_complete_records[program_name] = \
                    self.stats.program_complete_records.get(program_name, 0) + 1
                self.stats.total_emails_recorded += 1
            elif stat_type == "incomplete_record" and program_name:
                self.stats.program_incomplete_records[program_name] = \
                    self.stats.program_incomplete_records.get(program_name, 0) + 1
        #def function for getting list of college-program pairs from main url
    def get_college_program_urls(self) -> Dict[str, List[str]]:
        """
        Scrapes the main navigation to get college and program URLs
        Returns a dictionary of college names to lists of program URLs
        """
        try:
            self.logger.info(f"Scraping college/program URLs from {self.base_url}")
            response = requests.get(self.base_url)
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
                            program_url = urllib.parse.urljoin(self.base_url, program_url)
                            # Add tuple of college name and program URL to queue instead of storing
                            self.program_queue.put((college_name, program_name, program_url))
                            program_urls.append(program_url)
                    college_programs[college_name] = program_urls
                    self.update_stats("college")

            total_urls = sum(len(programs) for programs in college_programs.values())
            self.logger.success(f"Found {total_urls} programs to process")
            return college_programs
            
        except Exception as e:
            self.logger.error(f"Error getting college/program URLs: {str(e)}")
            return {}
    #def function for scraping the faculty url from the college-program pair
    def get_faculty_page(self, program_url: str, program_name: str) -> Optional[str]:
        """Scrapes a program page to find the faculty directory link"""
        try:
            response = requests.get(program_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            faculty_links = soup.find_all('a', href=lambda href: href and 'faculty' in href.lower())
            if not faculty_links:
                raise ValueError("No faculty links found!")
            
            for link in faculty_links:
                if 'faculty profile' in link.text.lower():
                    url = urllib.parse.urljoin(self.base_url, link['href'])
                    
                    with self.faculty_url_lock:
                        if url in self.processed_faculty_urls:
                            # If we've seen this URL before but not for this program
                            if program_name not in self.processed_faculty_urls[url]:
                                self.processed_faculty_urls[url].add(program_name)
                                return url  # Process it again for the new program
                            else:
                                self.logger.info(f"Skipping duplicate faculty URL for {program_name}: {url}")
                                return None
                        else:
                            # First time seeing this URL
                            self.processed_faculty_urls[url] = {program_name}
                            return url
                    
            raise ValueError("No working faculty links found!")
            
        except Exception as e:
            self.logger.error(f"Error finding faculty page in {program_url}: {str(e)}")
            return None
        
    def scrape_directory_page(self, url: str, college_name: str, program_name: str) -> List[ContactInfo]:
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
                        with self.faculty_url_lock:
                            if profile_url in self.processed_faculty_urls:
                                if program_name in self.processed_faculty_urls[profile_url]:
                                    continue  # Skip if already processed for this program
                                else:
                                    self.processed_faculty_urls[profile_url].add(program_name)
                            else:
                                self.processed_faculty_urls[profile_url] = {program_name}

                        contact = ContactInfo(
                            name=full_name,
                            office=college_name,
                            department=program_name,
                            profile_url=profile_url
                        )
                        contacts.append(contact)
                        self.update_stats("personnel_found", program_name)

            if not contacts:
                raise ValueError(f"No contacts found for {program_name}!")
            
            self.logger.success(f"Found {len(contacts)} contacts for {program_name}")
            return contacts

        except Exception as e:
            self.logger.error(f"Error parsing directory page {url} for {program_name}: {str(e)}")
            return []

    def scrape_profile_page(self, driver:WebDriver, contact: ContactInfo):
        """Scrapes an individual profile page to get the email"""
        try:
            driver.get(contact.profile_url)
            time.sleep(2)
            try:
                """ Deprecated solution for scraping the email (does not work properly on different layoutst of profile pages)"""
                ##########################################################################################
                # self.logger.info(f"Scraping {contact.profile_url} at {driver.title}")
                # email_element = driver.find_element(By.CSS_SELECTOR, "h2.wsite-content-title")

                # if not email_element:
                #     raise ValueError("Email element not found")
                # email_html = email_element.get_attribute("innerHTML")  # Get raw HTML inside the element
                # email_text = email_html.replace("<br>", "\n").strip()  # Convert <br> to newlines
                # lines = email_text.split("\n")  # Split into multiple lines

                # if len(lines) > 1:
                #     contact.email = lines[-1].strip()  # Get the last line (email)
                #########################################################################################
                """Regex solution to scrape the emails from the profile page."""
                page_text = driver.find_element(By.TAG_NAME, "body").text  # Get full visible text

                # Use regex to find emails
                email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                emails = re.findall(email_pattern, page_text)
                if len(emails) > 0:
                    contact.email = emails[0]
                return contact
            except Exception as e:
                self.logger.error(f"Could not parse email for {contact.profile_url}: {str(e)}")
                return contact
            
        except Exception as e:
            self.logger.error(f"Error parsing profile page {contact.profile_url}: {str(e)}")
            return contact
        
    def program_worker(self, thread_name:str):
        """Worker that processes program pages to find faculty directory links"""
        self.logger.info(f"Starting {thread_name}")
        while self.active and datetime.now() < self.end_time:
            try:
                item = self.program_queue.get(block=True)
                if item is None:
                    self.logger.info(f"{thread_name}: Cleaning up and exiting program worker")
                    break 
                
                college_name, program_name, program_url = item
                self.update_stats("program")
                self.logger.info(f" {thread_name}: Scraping faculty directory link from {program_url}")
                faculty_url = self.get_faculty_page(program_url, program_name)
                if not faculty_url:
                    raise ValueError(f"No faculty URL found in {program_url}")
                self.update_stats("faculty_url_success", program_name)
                self.logger.success(f"Found faculty URL: {faculty_url}")
                self.directory_queue.put((college_name, program_name, faculty_url))
                self.program_queue.task_done()
            except Exception as e:
                if not e:
                    self.logger.error(f"Error in {thread_name}: could not connect to {program_url}")
                self.update_stats("faculty_url_failure", program_name)
                self.logger.error(f"Error in {thread_name}: {str(e)}")
                
    def directory_worker(self, thread_name:str):
        """Worker that processes directory pages"""
        self.logger.success(f"Starting {thread_name}")
        while self.active and datetime.now() < self.end_time:
            try:
                college_name, program_name, url = self.directory_queue.get(block=True)
                if college_name is None or program_name is None or url is None:
                    self.logger.info(f"{thread_name}: Cleaning up and exiting directory worker")
                    break
                
                contacts = self.scrape_directory_page(url, college_name, program_name)
                self.logger.info(f" {thread_name}: Processing {url}")
                if not contacts:
                    raise ValueError(f"No contacts found in {url}")
                self.logger.success(f"Found {len(contacts)} contacts in {url}")
                for contact in contacts:
                    self.profile_queue.put(contact)
                self.directory_queue.task_done()
            except Exception as e:
                if not e:
                    self.logger.error(f"Error in {thread_name}: could not connect to {url}")
                self.logger.error(f"Error in {thread_name}: {str(e)}")
                

    def profile_worker(self, thread_name:str):
        self.logger.info(f"Starting {thread_name}")
        driver = init_selenium_driver()
        """Worker that processes individual profile pages"""
        while self.active and datetime.now() < self.end_time:
            try:
                contact = self.profile_queue.get(block=True)
                if contact is None:
                    self.logger.info(f"{thread_name}: Cleaning up and exiting profile worker")
                    break
                updated_contact = self.scrape_profile_page(driver, contact)
                if not updated_contact.email:  # Only save if we found an email
                    raise ValueError(f"No email found in {updated_contact.profile_url}")
                
                self.result_queue.put(updated_contact)
                self.update_stats("complete_record", updated_contact.department)
                self.logger.success(f"{thread_name} Found email: {updated_contact.email}")
                self.profile_queue.task_done()
            except Exception as e:
                if not e:
                    self.logger.error(f"Error in {thread_name}: could not connect to {contact.profile_url}")
                self.update_stats("incomplete_record", contact.department)
                self.logger.error(f"Error in {thread_name}: {str(e)}")
               

    def csv_worker(self):
        """Worker that writes results to CSV"""
        with open('contacts.csv', 'a', newline='') as csvfile:
            fieldnames = ['email', 'name', 'office', 'department', 'profile_url']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            csvfile.seek(0, 2)
            if csvfile.tell() == 0:
                writer.writeheader()
            
            while self.active or not self.result_queue.empty():
                try:
                    contact = self.result_queue.get( block=True)
                    if contact is None:
                        raise ValueError("No contact data found")
                        
                    self.logger.success(f"Processing {contact.profile_url} for {contact.department}")
                    with self.csv_lock:
                        writer.writerow(contact.__dict__)
                        csvfile.flush()
                        self.result_queue.task_done()
                except Exception as e:
                    self.logger.error(f"Error writing to CSV: {str(e)}")
                    
                    
    def stats_worker(self):
        """Worker that writes statistics to a JSON file"""
        while self.active or not self.result_queue.empty():
            try:
                with self.stats_lock:
                    with open('scraping_stats.json', 'w') as f:
                        json.dump(self.stats.to_dict(), f, indent=4)
                time.sleep(5)  # Update every 5 seconds
            except Exception as e:
                self.logger.error(f"Error writing statistics: {str(e)}")

    def run(self):
        self.end_time = datetime.now() + timedelta(minutes=self.scrape_time_minutes)
        college_programs = self.get_college_program_urls()
        
        all_threads = []
        
        # Create and start program page scraper threads
        program_threads = []
        for i in range(max(1, self.num_threads)):
            thread = threading.Thread(
                target=self.program_worker,
                args=(f"Program Thread_{i}",),
                name=f"Program Thread_{i}"
            )
            thread.daemon = True
            thread.start()
            program_threads.append(thread)
            all_threads.append(thread)

        # Create and start directory page scraper threads
        directory_threads = []
        for i in range(max(1, self.num_threads)):
            thread = threading.Thread(
                target=self.directory_worker,
                args=(f"Directory Thread_{i}",),
                name=f"Directory Thread_{i}"
            )
            thread.daemon = True
            thread.start()
            directory_threads.append(thread)
            all_threads.append(thread)
        
        # Create and start profile page scraper threads
        profile_threads = []
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self.profile_worker,
                args=(f"Profile Thread {i}",),
                name=f"Profile Thread_{i}"
            )
            thread.daemon = True
            thread.start()
            profile_threads.append(thread)
            all_threads.append(thread)
        
        # Create and start CSV writer thread
        csv_thread = threading.Thread(target=self.csv_worker)
        csv_thread.daemon = True
        csv_thread.start()
        all_threads.append(csv_thread)

        stats_thread = threading.Thread(target=self.stats_worker)
        stats_thread.daemon = True
        stats_thread.start()
        all_threads.append(stats_thread)

        print("\n\n")

        try:
            # Create time progress bar
            with tqdm(total=self.scrape_time_minutes * 60, desc="Time Remaining", unit="sec", 
                     ascii="*=", colour="magenta", position=1, 
                     bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as time_pbar:
                
                while (datetime.now() < self.end_time and self.active and 
                       (not all(q.empty() for q in [self.program_queue, self.directory_queue, self.profile_queue]) or
                        any(t.is_alive() for t in all_threads))):
                    time.sleep(1)
                    time_pbar.update(1)

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
        finally:
            # Clean shutdown
            self.active = False
            
            # Signal threads to stop
            for _ in range(len(program_threads)):
                self.program_queue.put(None)
            for _ in range(len(directory_threads)):
                self.directory_queue.put(None)
            for _ in range(len(profile_threads)):
                self.profile_queue.put(None)
            
            # Wait for all threads to complete with timeout
            for thread in all_threads:
                thread.join(timeout=5)
            
            self.result_queue.put(None)  # Signal CSV writer to stop
            
            self.logger.success("Scraping complete")

# Example usage
if __name__ == "__main__":
    num_threads = 8
    scrape_time_minutes = 3
    base_url = "https://www.dlsu.edu.ph/"
    print(f"{Fore.CYAN} Starting web scraper with {num_threads} threads for {scrape_time_minutes} minutes at {base_url}")
    scraper = NestedWebScraper(base_url, num_threads, scrape_time_minutes)
    scraper.run()