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
from typing import List, Dict, Tuple, Union
from statistics import mode, StatisticsError
from functools import wraps

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
    years: List[int] = [datetime.date.today().year],
):
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


def filter_meets_by_team_ids(team_ids: List[int]):
    """Filter out meets that do not include the given team IDs"""
    empty_meets = []
    not_our_team = []
    meet_count = len(all_state_meets)
    progress_updates = 0
    print(f"Found {meet_count} meets run in your state. Searching through them for your team with ID(s) {team_ids}...")
    for i, meet in enumerate(all_state_meets):
        # Update progress
        finished = 100 * (i / meet_count)
        if divmod(finished, 10) == (progress_updates, 0):
            progress_updates += 1
            print(f"Finished searching through {int(finished)}% of meets")

        meet_id = meet["id"]
        meet_teams_url = _MEET_TEAMS_URL_TEMPLATE.format(meet_id=meet_id)
        teams = get_json_from_url_with_retry(meet_teams_url)
        if not teams:
            empty_meets.append(meet_id)
            continue
        if not [team_id for team_id in team_ids if team_id in [t["teamID"] for t in teams]]:
            not_our_team.append(meet_id)

    to_remove = empty_meets + not_our_team
    print(f"Filtering out {len(to_remove)} of {meet_count} meets.")
    print(f"\t{len(empty_meets)} meets are empty.")
    print(f"\t{len(not_our_team)} meets do not contain your team(s) ({team_ids}).")
    participated_meets = [m for m in all_state_meets if m["id"] not in to_remove]
    return participated_meets


def retryable(
    exception_to_check: Union[Exception, Tuple[Exception]] = (Exception,),
    tries: int = 5,
    pause_before_try: float = 0.0,
    backoff: int = 1,
    backoff_factor: int = 2,
):
    """Decorator used to retry decorated function with exponential backoff.

    Args:
        exception_to_check  Union[Exception, Tuple[Exception]]): the exception to check. may be a tuple of
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TouchPad Live Meet Fetcher",
        description="Script to use the TouchPad Live REST API endpoints to collect all TouchPad Live meets for your team",
        epilog="",
    )

    team_args = parser.add_mutually_exclusive_group(required=True)
    team_args.add_argument("-t", "--team")
    team_args.add_argument("-i", "--team-ids", nargs="+")
    parser.add_argument("-y", "--year", required=False)
    parser.add_argument("-s", "--state", default=_DEFAULT_STATE)

    # A single meet with ID=1: https://www.touchpadlive.com/rest/touchpadlive/meets/1

    # Search for Meets: https://www.touchpadlive.com/rest/touchpadlive/meets?offset=0&state=VA&year=2023

    args = parser.parse_args()
    if args.team:
        team = urllib.parse.quote_plus(args.team)
    state = args.state
    years = range(2012, datetime.date.today().year) if not args.year else [args.year]

    team_ids = args.team_ids
    if not args.team_ids:
        print(f'Fetching meets that match your provided team name: "{team}"...')
        team_ids = [infer_team_id(fetch_meets(team, state, years))]

    print(f"Fetching all meets that occurred in the state of {state} from {min(years)} to {max(years)}")
    all_state_meets = fetch_meets(years=years)

    participated_meets = filter_meets_by_team_ids(team_ids)
    for pm in participated_meets:
        pm["url"] = f"http://www.touchpadlive.com/{pm['id']}"
    print(f"Found {len(participated_meets)} meets your team participated in:")
    pp.pprint(participated_meets)
    with open(f"{'_'.join([str(tid) for tid in team_ids])}_meets.json", "w") as meets_file:
        json.dump(participated_meets, meets_file)
