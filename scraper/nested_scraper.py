import threading
import csv
import json
import time
import logging
from datetime import datetime, timedelta
from queue import Queue
from tqdm import tqdm

from scraper.utils.data_models import ContactInfo, ScraperStatistics
from scraper.utils.logging_config import ColoredLogger, ColoredFormatter
from scraper.workers import ProgramWorker, DirectoryWorker, ProfileWorker

class NestedWebScraper:
    def __init__(self, url: str, num_threads: int, scrape_time_minutes: int):
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
        self.stopStats = False
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
                    contact = self.result_queue.get(block=True)
                    if contact is None:
                        break
                        
                    self.logger.success(f"Processing {contact.profile_url} for {contact.department}")
                    with self.csv_lock:
                        writer.writerow(contact.__dict__)
                        csvfile.flush()
                        self.result_queue.task_done()
                except Exception as e:
                    self.logger.error(f"Error writing to CSV: {str(e)}")
                    
    def stats_worker(self):
        """Worker that writes statistics once per session instead of every update cycle."""
        try:
            # while self.active or not self.result_queue.empty():
            #     time.sleep(5)  # Regular updates, but no file writing yet

            # Once the session ends, append stats to file
            with self.stats_lock:
                stats_file = "scraping_stats.json"

                # Try loading existing data
                try:
                    with open(stats_file, "r") as f:
                        existing_data = json.load(f)
                        if not isinstance(existing_data, list):
                            existing_data = [existing_data]  # Ensure it's a list
                except (FileNotFoundError, json.JSONDecodeError):
                    existing_data = []  # Start fresh if the file is missing or corrupted

                # Append only once per session
                existing_data.append(self.stats.to_dict())

                # Write updated data back to file
                with open(stats_file, "w") as f:
                    json.dump(existing_data, f, indent=4)

        except Exception as e:
            self.logger.error(f"Error writing statistics: {str(e)}")

    def run(self):
        self.end_time = datetime.now() + timedelta(minutes=self.scrape_time_minutes)
        
        program_worker = ProgramWorker(self)
        program_worker.get_college_program_urls()
        
        all_threads = []
        
        # Create and start program page scraper threads
        program_threads = []
        for i in range(max(1, self.num_threads)):
            thread = threading.Thread(
                target=program_worker.process,
                args=(f"Program Thread_{i}",),
                name=f"Program Thread_{i}"
            )
            thread.daemon = True
            thread.start()
            program_threads.append(thread)
            all_threads.append(thread)

        # Create and start directory page scraper threads
        directory_threads = []
        directory_worker = DirectoryWorker(self)
        for i in range(max(1, self.num_threads)):
            thread = threading.Thread(
                target=directory_worker.process,
                args=(f"Directory Thread_{i}",),
                name=f"Directory Thread_{i}"
            )
            thread.daemon = True
            thread.start()
            directory_threads.append(thread)
            all_threads.append(thread)
        
        # Create and start profile page scraper threads
        profile_threads = []
        profile_worker = ProfileWorker(self)
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=profile_worker.process,
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
            
            # Wait for all threads to complete with timeout
            for thread in all_threads:
                thread.join(timeout=5)
            
            #record stats
            self.stats_worker()
            
            self.logger.success("Scraping complete")