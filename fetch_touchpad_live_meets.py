"""
Script to use the TouchPad Live REST API endpoints to collect all TouchPad Live meets for your team
"""

import json
import urllib.request
import urllib.parse
import argparse
import datetime
import time
import pprint as pp
import concurrent.futures

from typing import List, Dict, Tuple, Union
from statistics import mode, StatisticsError
from functools import wraps
from math import ceil

_DEFAULT_STATE = "VA"
_SEARCH_URL_TEMPLATE = (
    "https://www.touchpadlive.com/rest/touchpadlive/meets?offset={offset}&pattern={team}&state={state}&year={year}"
)
_MEET_URL_TEMPLATE = "https://www.touchpadlive.com/rest/touchpadlive/meets/{meet_id}"
_MEET_TEAMS_URL_TEMPLATE = "https://www.touchpadlive.com/rest/touchpadlive/meets/{meet_id}/teams"
_MEET_ATTENDEES_URL_TEMPLATE = "https://www.touchpadlive.com/rest/touchpadlive/meets/{meet_id}/attendees"


def _build_search_url(year=datetime.date.today().year, team="", state=_DEFAULT_STATE, offset=0):
    return _SEARCH_URL_TEMPLATE.format(**locals())


def infer_team_id(meets: List[Dict]):
    print("Determining your specific team ID from the team name you provided...")
    team_ids = []
    for meet in meets:
        meet_teams_url = _MEET_TEAMS_URL_TEMPLATE.format(meet_id=meet["id"])
        teams = get_json_from_url_with_retry(meet_teams_url)
        if not teams:
            continue  # must be teams in the meet to count it
        for team in teams:
            team_ids.append(team["teamID"])
    team_id = None
    try:
        team_id = mode(team_ids)
        print(
            f"Inferred that your team ID is {team_id}, beause that occurred most frequently in the meets found with your provided team name"
        )
    except StatisticsError:
        raise SystemExit(
            "Could not get a TouchPad Live Team ID from that Team Name. ",
            "Either try a more unique fragment of your team name, ",
            "or search the request/response payloads using browser tools ",
            "for a meet you know your team participated in to get the integer team ID, and pass it in with the --team-ids arg",
        )
    return team_id


def fetch_meets(
    team_name: str = "",
    state: str = _DEFAULT_STATE,
    years: List[int] = None,
):
    if years is None:
        years = [datetime.date.today().year]
    all_meets = []
    for year in years:
        print(f"\tFetching meets for {year}")
        meets_in_year = 0
        offset = 0
        url = _build_search_url(year=year, team=team_name, state=state, offset=offset)
        while True:
            data = get_json_from_url_with_retry(url)
            meets_in_year += len(data)
            if not data:
                break
            else:
                all_meets.extend(data)
                offset += 1  # offset is by page, not by meet items ¯\_(ツ)_/¯
                url = _build_search_url(year=year, team=team_name, state=state, offset=offset)
        print(f"\tFound {meets_in_year} meets from {year}")

    return all_meets


def filter_meets_by_team_ids(meets: List[Dict], team_ids: List[int]):
    """Filter out meets that do not include the given team IDs"""
    empty_meets = []
    not_our_team = []
    meet_count = len(meets)
    print(f"Found {meet_count} meets run in your state. Searching through them for your team with ID(s) {team_ids}...")

    def search_meet(meet):
        meet_id = meet["id"]
        meet_teams_url = _MEET_TEAMS_URL_TEMPLATE.format(meet_id=meet_id)
        teams_json = get_json_from_url_with_retry(meet_teams_url)
        if not teams_json:
            return meet_id, False
        teams_in_meet = [t["teamID"] for t in teams_json]
        if not any(int(our_team) in teams_in_meet for our_team in team_ids):
            return False, meet_id
        return False, False

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        searches = {executor.submit(search_meet, m): (i, m) for i, m in enumerate(meets)}
        finished = 0
        progress_updates = 0
        for search in concurrent.futures.as_completed(searches):
            (i, meet_to_search) = searches[search]

            # Update progress
            finished = ceil(100 * (i / meet_count))
            if divmod(finished, 10) == (progress_updates, 0):
                progress_updates += 1
                print(f"Finished searching through {finished}% of meets")

            # Check results to decide if this is a meet we keep
            empty_meet, not_us = search.result()
            if empty_meet:
                empty_meets.append(empty_meet)
            if not_us:
                not_our_team.append(not_us)

    to_remove = empty_meets + not_our_team
    print(f"Filtering out {len(to_remove)} of {meet_count} meets.")
    print(f"\t{len(empty_meets)} meets are empty.")
    print(f"\t{len(not_our_team)} meets do not contain your team(s) ({team_ids}).")
    our_meets = [m for m in meets if m["id"] not in to_remove]
    return our_meets


def retryable(
    exception_to_check: Union[Exception, Tuple[Exception]] = (Exception,),
    tries: int = 5,
    pause_before_try: float = 0.0,
    backoff: int = 1,
    backoff_factor: int = 2,
):
    """Decorator used to retry decorated function with exponential backoff.

    Args:
        exception_to_check: (Union[Exception, Tuple[Exception]]): the exception to check. may be a tuple of
            exceptions to check
        tries (int): number of times to try (not retry) before giving up
        pause_before_try (float): how long to pause in seconds before ANY and EVERY try
        backoff (int): how many seconds to backoff the first time a try fails
        backoff_factor (int): backoff multiplier e.g. value of 2 will double the delay
            each retry
    """

    def decorator(fn):
        @wraps(fn)
        def retry(*args, **kwargs):
            this_try = 1
            current_backoff = backoff
            while this_try < tries:
                if pause_before_try:
                    time.sleep(pause_before_try)
                try:
                    return fn(*args, **kwargs)  # attempt a (re)try if allowed more than one try
                except Exception as exc_val:
                    exc_type = type(exc_val)

                    if exc_type is not None:
                        print(f"Found error: {str(exc_type)}... checking if in {exception_to_check}")
                    else:
                        print("NONE exception")
                        return

                    if not issubclass(exc_type, exception_to_check):
                        print("OTHER exception")
                        return
                    if exc_type is not None and issubclass(exc_type, exception_to_check):
                        # Try again if you have tries left
                        if this_try < tries:  # do the retry
                            print(
                                f"Retrying in {pause_before_try + current_backoff:.2f} seconds..."
                                f" After receiving retryable Error: {str(exc_val)}"
                            )
                            this_try += 1
                            time.sleep(current_backoff)
                            current_backoff *= backoff_factor
                        else:
                            print("No more tries left :(")
                    if exc_type is not None and not issubclass(exc_type, exception_to_check):
                        print("NON-RETRYABLE exception" + str(exc_val))
                        raise exc_val

            # Last and unguarded call attempt, after all retries (if any) have been attempted
            return fn(*args, **kwargs)

        return retry  # wrapper --around--> wrapped function

    return decorator  # @retryable(arg[, ...]) decorator --delegating-to--> wrapper


@retryable()
def get_json_from_url_with_retry(url: str):
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())


def main(parser: argparse.ArgumentParser):
    # A single meet with ID=1: https://www.touchpadlive.com/rest/touchpadlive/meets/1

    # Search for Meets: https://www.touchpadlive.com/rest/touchpadlive/meets?offset=0&state=VA&year=2023

    args = parser.parse_args()
    if args.team:
        team = urllib.parse.quote_plus(args.team)
    state = args.state
    years = range(2012, datetime.date.today().year) if not args.year else [args.year]
    urls_only = args.print_urls_only

    team_ids = args.team_ids
    if not args.team_ids:
        print(f'Fetching meets that match your provided team name: "{team}"...')
        team_ids = [infer_team_id(fetch_meets(team, state, years))]

    print(f"Fetching all meets that occurred in the state of {state} from {min(years)} to {max(years)}")
    all_state_meets = fetch_meets(years=years)

    participated_meets = filter_meets_by_team_ids(all_state_meets, team_ids)
    meet_urls = []
    for pm in participated_meets:
        pm["url"] = f"http://www.touchpadlive.com/{pm['id']}"
        meet_urls.append(pm["url"])
    print(f"Found {len(participated_meets)} meets your team participated in:")
    if urls_only:
        [print(u) for u in meet_urls]
    else:
        pp.pprint(participated_meets)
    with open(f"{'_'.join([str(tid) for tid in team_ids])}_meets.json", "w") as meets_file:
        json.dump(participated_meets, meets_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TouchPad Live Meet Fetcher",
        description="Script to use the TouchPad Live REST API endpoints to collect all TouchPad Live meets for your team",
        epilog="",
    )

    team_args = parser.add_mutually_exclusive_group(required=True)
    team_args.add_argument(
        "-t", "--team", help="Search by this identifying part of a team name, that may show up in a " "meet " "title"
    )
    team_args.add_argument(
        "-i",
        "--team-ids",
        nargs="+",
        help="Provide the integer team ID, or space-separated list of team IDs, you want to collect meets for, if you know it.",
    )
    parser.add_argument(
        "-y",
        "--year",
        required=False,
        help="Narrow down to a specific years. Will use all years " "2012+ if not provided.",
    )
    parser.add_argument(
        "-s",
        "--state",
        default=_DEFAULT_STATE,
        help=f"Search only for meets in this state. " f"{_DEFAULT_STATE} used by default. This is " f"required.",
    )
    parser.add_argument(
        "-u",
        "--print-urls-only",
        action="store_true",
        default=False,
        help="Only list the Meet URL " "in the final results. " "Full JSON file is still " "saved to filesystem.",
    )

    main(parser)
