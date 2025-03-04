from colorama import Fore
from scraper.nested_scraper import NestedWebScraper

if __name__ == "__main__":
    num_threads = 2
    scrape_time_minutes = 1
    base_url = "https://www.dlsu.edu.ph/"
    print(f"{Fore.CYAN} Starting web scraper with {num_threads} threads for {scrape_time_minutes} minutes at {base_url}")
    scraper = NestedWebScraper(base_url, num_threads, scrape_time_minutes)
    scraper.run()