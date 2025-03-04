from dataclasses import dataclass
from typing import List, Optional, Dict

#Attributes of a record
@dataclass
class ContactInfo:
    email: Optional[str] = None
    name: Optional[str] = None
    office: Optional[str] = None
    department: Optional[str] = None
    profile_url: Optional[str] = None

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