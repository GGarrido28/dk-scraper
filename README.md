# DraftKings Scraper

A modular Python scraper for collecting DraftKings fantasy sports data. Scrapes contests, draft groups, game types, game sets, payouts, player salaries, and contest entries - returning structured data for further processing.

**Version:** 3.1.0
**License:** MIT
**Python:** >=3.11

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Download Folder Structure](#download-folder-structure)
- [Project Structure](#project-structure)
- [Usage](#usage)
  - [Full Pipeline (Orchestrator)](#full-pipeline-orchestrator)
  - [Individual Scrapers](#individual-scrapers)
  - [Programmatic Usage](#programmatic-usage)
  - [Utility Functions](#utility-functions)
- [Data Structures](#data-structures)
- [Architecture](#architecture)
  - [Design Principles](#design-principles)
  - [Scraper Interface](#scraper-interface)
- [Versioning](#versioning)
- [License](#license)

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

## Configuration

### Environment Variables

Create a `.env` file in the project root (see `.env.example`):

```bash
# .env

# DraftKings credentials (required for contest entries/history scrapers)
DK_EMAIL=your_email@example.com
DK_PASSWORD=your_password
DK_USERNAME=YourDraftKingsUsername    # Your DK display name

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
└── download/     # Downloaded contest CSVs are stored here
```

The scraper:
1. Downloads contest standings CSV from DraftKings to `DOWNLOAD_DIRECTORY`
2. Moves the file to `CSV_DIRECTORY/download/`

## Project Structure

```
draftkings-scraper/
├── orchestrator.py              # Main entry point - runs full pipeline
├── draftkings_scraper/
│   ├── __init__.py
│   ├── constants.py             # API URLs
│   ├── http_handler.py          # HTTP client with retry logic
│   ├── schemas/                 # Marshmallow validation schemas
│   │   ├── contest.py           # Contest entry schema
│   │   ├── contests.py          # Contests schema
│   │   ├── contest_history.py   # Contest history schema
│   │   ├── draft_groups.py      # Draft groups schema
│   │   ├── game_sets.py         # Game sets, competitions, game styles schemas
│   │   ├── game_types.py        # Game types schema
│   │   ├── payout.py            # Payout schema
│   │   ├── player_salary.py     # Player salary schema
│   │   └── player_results.py    # Player results schema
│   ├── contests/                # Contests scraper
│   │   └── scraper.py
│   ├── game_types/              # Game types scraper
│   │   └── scraper.py
│   ├── game_sets/               # Game sets scraper (competitions, game styles)
│   │   └── scraper.py
│   ├── draft_groups/            # Draft groups scraper
│   │   └── scraper.py
│   ├── payout/                  # Payout scraper
│   │   └── scraper.py
│   ├── player_salary/           # Player salary scraper
│   │   └── scraper.py
│   ├── contest_entries/         # Contest entries downloader (requires auth)
│   │   └── scraper.py
│   ├── contest_entry_history/   # Contest entry history scraper (requires auth)
│   │   └── scraper.py
│   └── utils/
│       ├── helpers.py           # Utility functions
│       └── contest_adder.py     # Add single contest utility
```

## Usage

### Full Pipeline (Orchestrator)

The orchestrator runs all scrapers in the correct order, sharing lobby data to minimize API calls.

```bash
# Scrape a single sport
python orchestrator.py NFL

# Scrape multiple sports
python orchestrator.py --sports NFL,MLB,MMA

# Skip specific stages
python orchestrator.py MLB --skip-payouts --skip-player-salaries
```

**Pipeline Order:**
1. Fetch lobby data (shared across scrapers)
2. Scrape draft groups (filtered by game_type_ids and slate_types)
3. Scrape contests (filtered by draft_group_ids from step 2)
4. Scrape game types
5. Scrape game sets (competitions and game styles)
6. Scrape payouts (for contest_ids from step 3)
7. Scrape player salaries (for draft_group_ids from step 2)

### Individual Scrapers

Each scraper can be run independently:

```bash
# Contests
python -m draftkings_scraper.contests.scraper NFL

# Fetch contest attributes (is_final, is_cancelled, start_time)
python -m draftkings_scraper.contests.scraper NFL --fetch-attributes --contest-ids 123456,789012

# Game Types
python -m draftkings_scraper.game_types.scraper MLB

# Draft Groups
python -m draftkings_scraper.draft_groups.scraper MMA

# Game Sets (competitions and game styles)
python -m draftkings_scraper.game_sets.scraper CS
python -m draftkings_scraper.game_sets.scraper CS --tags Featured

# Payouts (requires contest IDs)
python -m draftkings_scraper.payout.scraper NFL --contest-ids 123456,789012

# Player Salaries (requires draft group IDs)
python -m draftkings_scraper.player_salary.scraper NFL --draft-group-ids 12345,67890

# Contest Entries CSV Download (requires authentication)
python -m draftkings_scraper.contest_entries.scraper --contest-ids 123456,789012

# Contest Entry History (requires authentication)
python -m draftkings_scraper.contest_entry_history.scraper
```

### Programmatic Usage

```python
from draftkings_scraper.contests import ContestsScraper
from draftkings_scraper.draft_groups import DraftGroupsScraper
from draftkings_scraper.game_sets import GameSetsScraper
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

# Scrape game sets (competitions and game styles)
game_sets_scraper = GameSetsScraper(sport="CS")
game_sets = game_sets_scraper.scrape(lobby_data=lobby_data)

for gs in game_sets:
    print(f"Game Set: {gs['contest_start_time_suffix']}")
    for comp in gs['competitions']:
        print(f"  {comp['away_team_name']} @ {comp['home_team_name']}")
```

#### Using the Orchestrator Programmatically

```python
from orchestrator import DraftKingsOrchestrator, run_all_sports

# Single sport
orchestrator = DraftKingsOrchestrator(sport="NFL")
result = orchestrator.run(
    skip_payouts=False,
    skip_player_salaries=False,
)

print(f"Contests: {len(result['contests'])}")
print(f"Draft Groups: {len(result['draft_groups'])}")
print(f"Game Sets: {len(result['game_sets'])}")
print(f"Payouts: {len(result['payouts'])}")
print(f"Player Salaries: {len(result['player_salaries'])}")

# Multiple sports
results = run_all_sports(
    sports=["NFL", "MLB", "MMA"],
    skip_payouts=True,
)

for sport, data in results.items():
    print(f"{sport}: {len(data['contests'])} contests")
```

### Utility Functions

#### Get Single Contest Data

Fetch a contest and all related data (draft group, payouts, player salaries) by contest ID:

```python
from draftkings_scraper.utils import ContestAdder

adder = ContestAdder()
result = adder.get_contest(contest_id=123456)
print(result)
# {
#     'contest_id': 123456,
#     'status': 'success',
#     'sport': 'nfl',
#     'contest': {...},
#     'draft_group': {...},
#     'payouts': [...],
#     'player_salaries': [...],
#     'draft_group_id': 78901,
#     'from_lobby': True
# }
```

Or via CLI:

```bash
python -m draftkings_scraper.utils.contest_adder 123456
```

## Data Structures

All scrapers return validated Python dictionaries. Example structures:

### Contest
```python
{
    'contest_id': 123456,
    'contest_name': 'NFL $1M Fantasy Football Millionaire',
    'entry_fee': 25,
    'max_entries': 10000,
    'draft_group_id': 78901,
    'start_time': datetime(2024, 1, 1, 13, 0, 0),
    'is_final': False,
    'is_cancelled': False,
    # ... additional fields
}
```

### Draft Group
```python
{
    'draft_group_id': 78901,
    'sport': 'NFL',
    'start_date': '2024-01-01T13:00:00',
    'game_count': 14,
    'game_type_id': 1,
    # ... additional fields
}
```

### Payout
```python
{
    'contest_id': 123456,
    'min_position': 1,
    'max_position': 1,
    'payout_one_type': 'Cash',
    'payout_one_value': 100000.0,
    # ... additional fields
}
```

### Player Salary
```python
{
    'draft_group_id': 78901,
    'player_id': 12345,
    'player_name': 'Patrick Mahomes',
    'roster_position': 'QB',
    'salary': 8000,
    'team': 'KC',
    # ... additional fields
}
```

### Game Set
```python
{
    'game_set_key': 'BE99BF41A73B693FD89309EACB9E81DA',
    'contest_start_time_suffix': ' (BLAST Bounty)',
    'tag': 'Featured',
    'competitions': [
        {
            'game_id': 6162971,
            'away_team_name': 'Liquid',
            'home_team_name': 'Falcons',
            'start_date': '2026-01-22T14:55:00.0000000Z',
            'status': 'Pre-Game',
            'sport': 'CS',
            # ... additional fields
        }
    ],
    'game_styles': [
        {
            'game_style_id': 84,
            'name': 'Classic',
            'description': 'Create a 6-player lineup...',
            # ... additional fields
        }
    ]
}
```

## Architecture

### Design Principles

1. **Modular Structure**: Each scraper is self-contained in its own module with a consistent interface
2. **Data-Only**: Scrapers return validated data - no database dependencies
3. **Shared Lobby Data**: The orchestrator fetches lobby data once and shares it across scrapers
4. **Schema Validation**: All data is validated using Marshmallow schemas
5. **Retry Logic**: HTTP requests use retry strategies for resilience
6. **Centralized Constants**: API URLs are defined in `constants.py`

### Scraper Interface

All scrapers follow a consistent pattern:

```python
class SomeScraper:
    def __init__(self, sport: str):
        # Initialize schemas, HTTP handler, etc.
        pass

    def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        # Main entry point - returns validated data
        pass
```

## Versioning

This project uses semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR** - Breaking API changes
- **MINOR** - New features (backward compatible)
- **PATCH** - Bug fixes

Current version: **3.1.0**

### Changelog

**3.1.0** - Added GameSetsScraper for scraping game sets with competitions and game styles.

**3.0.0** - Removed database dependencies. Scrapers now return data instead of inserting to database.

**2.0.1** - Previous version with PostgreSQL database integration.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Note**: This package is not officially affiliated with or endorsed by DraftKings. Use responsibly and in accordance with DraftKings Terms of Service.
