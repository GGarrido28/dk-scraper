import json
from typing import Dict, Any

from draftkings_scraper.constants import CONTEST_API_URL
from draftkings_scraper.http_handler import HTTPHandler

# Module-level HTTP handler for reuse
_http = HTTPHandler()


def get_contest_payout(contest_id: int) -> Dict[str, Any]:
    """
    Get payout information for a single contest (for real-time lookups).

    Args:
        contest_id: The DraftKings contest ID.

    Returns:
        dict: Contest payout information including:
            - sport: Sport code
            - contest_id: The contest ID
            - payouts: Dict mapping rank (str) to cash payout
            - cashing_index: Number of paid positions - 1
            - num_entries: Current number of entries
            - max_entries: Maximum allowed entries
            - entry_fee: Contest entry fee
            - is_locked: Always True (contest is locked)
    """
    url = CONTEST_API_URL % contest_id
    response = _http.get(url)
    data = json.loads(response.content)

    if not data:
        return {"contest_id": contest_id}

    contest_detail = data.get("contestDetail", {})
    payout_summary = contest_detail.get("payoutSummary", [])

    payouts = [
        {
            "contest_payout_id": f"{contest_id}|{row.get('minPosition')}|{row.get('maxPosition')}",
            "contest_id": contest_id,
            "min_position": row.get("minPosition"),
            "max_position": row.get("maxPosition"),
            "cash": sum([x.get("value", 0) for x in row.get("payoutDescriptions", [])]),
        }
        for row in payout_summary
    ]

    contest_payouts_ranks = {}
    for payout in payouts:
        for i in range(payout["min_position"], payout["max_position"] + 1):
            contest_payouts_ranks[str(i)] = payout["cash"]

    sport = contest_detail.get("sport", "").lower()

    return {
        "sport": sport,
        "contest_id": contest_id,
        "payouts": contest_payouts_ranks,
        "num_entries": contest_detail.get("entries", 0),
        "max_entries": contest_detail.get("maximumEntries", 0),
        "entry_fee": contest_detail.get("entryFee", 0),
    }
