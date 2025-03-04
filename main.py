import argparse
from colorama import Fore
from scraper.nested_scraper import NestedWebScraper

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the web scraper.")
    parser.add_argument("--threads", type=int, default=24, help="Number of threads (default: 24)")
    parser.add_argument("--time", type=int, default=4, help="Scrape time in minutes (default: 4)")
    parser.add_argument("--url", type=str, default="https://www.dlsu.edu.ph/", help="Base URL (default: https://www.dlsu.edu.ph/)")
    
    args = parser.parse_args()
    
    num_threads = args.threads
    scrape_time_minutes = args.time
    base_url = args.url
    
    print(f"{Fore.CYAN}Starting web scraper with {num_threads} threads for {scrape_time_minutes} minutes at {base_url}")
    scraper = NestedWebScraper(base_url, num_threads, scrape_time_minutes)
    scraper.run()