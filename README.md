# DraftKings Scraper

A modular Python scraper for collecting DraftKings fantasy sports data. Scrapes contests, draft groups, game types, payouts, player salaries, and contest entries into a PostgreSQL database.

**Version:** 2.0.1
**License:** MIT
**Python:** >=3.11

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Download Folder Structure](#download-folder-structure)
  - [Sports Configuration](#sports-configuration)
- [Project Structure](#project-structure)
- [Usage](#usage)
  - [Full Pipeline (Orchestrator)](#full-pipeline-orchestrator)
  - [Individual Scrapers](#individual-scrapers)
  - [Programmatic Usage](#programmatic-usage)
  - [Utility Functions](#utility-functions)
- [Database Tables](#database-tables)
- [Architecture](#architecture)
  - [Design Principles](#design-principles)
  - [Scraper Interface](#scraper-interface)
- [Versioning](#versioning)
- [License](#license)
- [Support](#support)

## Installation

```bash
# Clone the repository
git clone https://github.com/GGarrido28/draftkings-scraper.git
cd draftkings-scraper

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

- `requests` - HTTP requests
- `beautifulsoup4` - HTML parsing
- `marshmallow` - Data validation
- `pytz` - Timezone handling
- `python-dotenv` - Environment variables
- `selenium` - Browser automation (for contest entries)
- `webdriver-manager` - Chrome driver management
- `tqdm` - Progress bars
- `mg` - Custom database and logging utilities

## Configuration

### Environment Variables

Create a `.env` file in the project root for contest entries/history scrapers that require authentication:

```bash
# .env

# DraftKings credentials (required for contest entries/history scrapers)
DK_EMAIL=your_email@example.com
DK_PASSWORD=your_password
DK_USERNAME=YourDraftKingsUsername    # Your DK display name (for parsing head-to-head opponents)

# Download directories
DOWNLOAD_DIRECTORY=/path/to/browser/downloads    # Where Chrome downloads files
CSV_DIRECTORY=/path/to/csv/storage               # Where contest CSVs are organized

# Chrome path (optional, defaults to standard Windows install location)
CHROME_PATH=/path/to/chrome                      # Only needed for non-standard installs
```

### Download Folder Structure

The contest entries scraper organizes downloaded CSV files into the following structure:

```
CSV_DIRECTORY/
├── download/     # Temporary location for newly downloaded CSVs
├── import/       # Successfully imported CSVs are moved here
└── failed/       # CSVs that failed to import are moved here
```

The scraper:
1. Downloads contest standings CSV from DraftKings to `DOWNLOAD_DIRECTORY`
2. Moves the file to `CSV_DIRECTORY/download/`
3. Processes and imports the data to the database
4. Moves the file to `import/` on success or `failed/` on error

### Sports Configuration

Edit `draftkings_scraper/constants.py` to configure which sports are scraped:

```python
SPORTS_WITH_DB = [
    'MLB',
    'MMA',
    'GOLF',
    'CFB',
]
```

## Project Structure

```
draftkings-scraper/
├── orchestrator.py              # Main entry point - runs full pipeline
├── draftkings_scraper/
│   ├── __init__.py
│   ├── constants.py             # API URLs and sport mappings
│   ├── schemas/                 # Marshmallow validation schemas
│   │   ├── contest.py           # Contest entry schema
│   │   ├── contests.py          # Contests schema
│   │   ├── contest_history.py   # Contest history schema
│   │   ├── draft_groups.py      # Draft groups schema
│   │   ├── game_types.py        # Game types schema
│   │   ├── payout.py            # Payout schema
│   │   ├── player_salary.py     # Player salary schema
│   │   └── player_results.py    # Player results schema
│   ├── contests/                # Contests scraper
│   │   └── scraper.py
│   ├── game_types/              # Game types scraper
│   │   └── scraper.py
│   ├── draft_groups/            # Draft groups scraper
│   │   └── scraper.py
│   ├── payout/                  # Payout scraper
│   │   └── scraper.py
│   ├── player_salary/           # Player salary scraper
│   │   └── scraper.py
│   ├── contest_entries/         # Contest entries scraper (requires auth)
│   │   └── scraper.py
│   ├── contest_entry_history/   # Contest entry history scraper (requires auth)
│   │   └── scraper.py
│   └── utils/
│       ├── payout.py            # Real-time payout lookup utility
│       └── contest_adder.py     # Add single contest utility
└── sql_processing/              # Database utilities
    ├── postgres_process.py
    └── db_cleanup.py
```

## Usage

### Full Pipeline (Orchestrator)

The orchestrator runs all scrapers in the correct order, sharing lobby data to minimize API calls.

```bash
# Scrape a single sport
python orchestrator.py NFL

# Scrape all configured sports (from SPORTS_WITH_DB in constants.py)
python orchestrator.py --all

# Skip specific stages
python orchestrator.py MLB --skip-payouts --skip-player-salaries
```

**Pipeline Order:**
1. Fetch lobby data (shared across scrapers)
2. Scrape contests
3. Scrape game types
4. Scrape draft groups
5. Scrape payouts (for contest_ids from step 2)
6. Scrape player salaries (for draft_group_ids from step 4)

### Individual Scrapers

Each scraper can be run independently:

```bash
# Contests
python -m draftkings_scraper.contests.scraper NFL

# Update contest attributes (is_final, is_cancelled, start_time)
python -m draftkings_scraper.contests.scraper NFL --update-attributes

# Game Types
python -m draftkings_scraper.game_types.scraper MLB

# Draft Groups
python -m draftkings_scraper.draft_groups.scraper MMA

# Payouts (requires contest IDs)
python -m draftkings_scraper.payout.scraper NFL --contest-ids 123456,789012

# Payouts (by draft group ID - looks up contests from DB)
python -m draftkings_scraper.payout.scraper NFL --draft-group-id 12345

# Player Salaries (requires draft group IDs)
python -m draftkings_scraper.player_salary.scraper NFL --draft-group-ids 12345,67890

# Contest Entries (requires authentication)
python -m draftkings_scraper.contest_entries.scraper --contest-id 123456

# Contest Entry History (requires authentication)
# Downloads your full contest history CSV from DraftKings account
python -m draftkings_scraper.contest_entry_history.scraper

# Use existing CSV file (skip browser download)
python -m draftkings_scraper.contest_entry_history.scraper --skip-download

# Skip updating sport-specific databases
python -m draftkings_scraper.contest_entry_history.scraper --skip-sport-update

# Custom wait time for download (default: 120 seconds)
python -m draftkings_scraper.contest_entry_history.scraper --sleep-time 60
```

### Programmatic Usage

```python
from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.draft_groups import DraftGroupsScraper
from draftkings_scraper.payout import PayoutScraper
from draftkings_scraper.player_salary import PlayerSalaryScraper

# Scrape contests and get lobby data
contests_scraper = ContestsScraper(sport="NFL")
result = contests_scraper.scrape()
contests = result['contests']
lobby_data = result['lobby_data']

# Reuse lobby data for other scrapers
draft_groups_scraper = DraftGroupsScraper(sport="NFL")
draft_groups = draft_groups_scraper.scrape(lobby_data=lobby_data)

# Scrape payouts for specific contests
contest_ids = [c['contest_id'] for c in contests]
payout_scraper = PayoutScraper(sport="NFL")
payouts = payout_scraper.scrape(contest_ids=contest_ids)

# Scrape player salaries for draft groups
draft_group_ids = [dg['draft_group_id'] for dg in draft_groups]
salary_scraper = PlayerSalaryScraper(sport="NFL")
salaries = salary_scraper.scrape(draft_group_ids=draft_group_ids)

# Scrape contest entry history (requires DK_EMAIL, DK_PASSWORD, DK_USERNAME in .env)
from draftkings_scraper.contest_entry_history import ContestEntryHistoryScraper

history_scraper = ContestEntryHistoryScraper(sleep_time=120)
entries = history_scraper.scrape(
    skip_download=False,      # Set True to use existing CSV
    skip_sport_update=False   # Set True to skip sport-specific DB updates
)
```

### Utility Functions

#### Real-time Payout Lookup

Get payout information for a contest without writing to the database:

```python
from draftkings_scraper.utils import get_contest_payout

payout_info = get_contest_payout(contest_id=123456)
print(payout_info)
# {
#     'sport': 'nfl',
#     'contest_id': 123456,
#     'payouts': {'1': 10000, '2': 5000, '3': 2500, ...},
#     'cashing_index': 150,
#     'num_entries': 5000,
#     'max_entries': 10000,
#     'entry_fee': 25,
#     'is_locked': True
# }
```

#### Add Single Contest

Add a contest and all related data (draft group, payouts, player salaries) by contest ID:

```python
from draftkings_scraper.utils import ContestAdder

adder = ContestAdder()
result = adder.add_contest(contest_id=123456)
print(result)
# {
#     'contest_id': 123456,
#     'status': 'added',
#     'sport': 'nfl',
#     'draft_group_id': 78901,
#     'from_lobby': True
# }
```

Or via CLI:

```bash
python -m draftkings_scraper.utils.contest_adder 123456
```

## Data

The scrapers return the following tables in the `draftkings` schema:

| Table | Scraper | Description |
|-------|---------|-------------|
| `contests` | ContestsScraper | Contest metadata |
| `game_types` | GameTypesScraper | Game type definitions |
| `draft_groups` | DraftGroupsScraper | Draft group metadata |
| `payout` | PayoutScraper | Payout structures |
| `player_salary` | PlayerSalaryScraper | Player salaries per draft group |
| `contest_entries` | ContestEntriesScraper | Contest lineups (requires auth) |
| `contest_entry_history` | ContestEntryHistoryScraper | Historical entries (requires auth) |

## Architecture

### Design Principles

1. **Modular Structure**: Each scraper is self-contained in its own module with a consistent interface
2. **Shared Lobby Data**: The orchestrator fetches lobby data once and shares it across scrapers
3. **Schema Validation**: All data is validated using Marshmallow schemas before database insertion
4. **Retry Logic**: HTTP requests use retry strategies for resilience
5. **Centralized Constants**: API URLs and mappings are defined in `constants.py`

### Scraper Interface

All scrapers follow a consistent pattern:

```python
class SomeScraper:
    def __init__(self, sport: str):
        # Initialize connections, schemas, etc.
        pass

    def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        # Main entry point - returns validated data
        pass

    def _close_sql_connections(self):
        # Cleanup
        pass
```

## Versioning

This project uses semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR** - Breaking API changes
- **MINOR** - New features (backward compatible)
- **PATCH** - Bug fixes

Current version: **2.0.1**

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues, questions, or contributions:

- **GitHub Issues**: https://github.com/GGarrido28/draftkings-scraper/issues
- **Email**: gabriel.garrido28@gmail.com

---

**Note**: This package is not officially affiliated with or endorsed by DraftKings. Use responsibly and in accordance with DraftKings Terms of Service.