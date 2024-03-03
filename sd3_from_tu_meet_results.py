"""
Script to convert, read, or write Swimming Data Interchange Files (SDIF, .sd3) from a 
Team Unify and/or TouchPad source.

Required python packages need to be pip-installed and in the PYTHONPATH before running:
pip install sdif pandas openpyxl

Best explanation of the SDIF format: http://www.winswim.com/ftp/Standard%20Data%20Interchange%20Format.pdf
Less reable spec: https://www.usms.org/admin/sdifv3f.txt
"""

# TODO: check trim on swim codes that are not numbers
# TODO: could put "UO" at end as event time class code
# TODO: could look at putting NT in for prelim or seed times
# TODO: TU uses future for OD after C1 and event numer in relay event
# TODO: Move assumed values up as constants to promote easier re-use via config files; e.g. summer_swim_team=True, state, etc.


import sys
import re
import datetime
import json
import sdif
import pathlib
import pprint as pp
import pandas as pd
import numpy as np

from openpyxl.styles import Alignment
from decimal import Decimal
from typing import ClassVar, Optional, Callable, Literal, List, Tuple
from sdif.model_meta import model, spec
from sdif.fields import FieldType
from sdif.models import (
    FileCode,
    OrganizationCode,
    TimeT,
    TimeCode,
    Time,
)


t = FieldType

_INDIVIDUAL_OR_RELAY_FIELD = "individual_or_relay"

_SWIM_TEAM_INFO_PATH = pathlib.Path("swim_team_info.json")
_SWIM_TEAM_INFO_DEFAULT = {
    "swim_team": {
        "full_name": "An ODSL Swim Team",
        "abbreviation": "Swimmers",
        "local_swim_committee": "OD",
        "team_unify_team_code": "XXX",
        "swimtopia_team_code": "YYY",
        "address_line_1": "1 Main Street",
        "address_line_2": None,
        "city": "Ashburn",
        "state": "VA",
        "postal_code": "20148",
    }
}


def _get_swim_team_info():
    if _SWIM_TEAM_INFO_PATH.exists:
        with open(_SWIM_TEAM_INFO_PATH) as sti:
            return json.load(sti)
    else:
        return _SWIM_TEAM_INFO_DEFAULT


SWIM_TEAM_INFO = _get_swim_team_info()

FINALS_COURSE = sdif.models.CourseStatusCode.short_meters_hytek_nonstandard  # 'S'
IS_SUMMER_LEAGUE = True


@model(frozen=True, kw_only=True)
class MeetHostInfo:
    identifier: ClassVar[str] = "B2"
    contact_name: str = spec(start=12, len=20)
    country: Optional[str] = spec(start=118, len=3)
    contact_phone: str = spec(start=121, len=12, type=t.phone)


sdif.model_meta.REGISTERED_MODELS.pop("Z0")

# @model(frozen=True, kw_only=True)
# class TouchPadFileTerminator(FileTerminator):
#     notes = spec(start=14, len=30, type=Optional[str])  # override to make notes optional


@model(frozen=True, kw_only=True)
class TouchPadFileTerminator:
    """Identify the logical end of file for a file
    transmission.  Record statistics and swim
    statistics are listed for convenience.

    This record is mandatory in each file.  Each file ends with this
    record and each file has only one record of this type.  The first
    four fields are mandatory.  Additional fields provide for text
    and record counts.
    """

    identifier: ClassVar[str] = "Z0"
    organization: Optional[OrganizationCode] = spec(3, 1, m2=True)
    file_code: FileCode = spec(12, 2)
    notes: Optional[str] = spec(14, 30)
    # n_b_records: Optional[int] = spec(44, 3)
    # n_meets: Optional[int] = spec(47, 3)
    # n_c_records: Optional[int] = spec(50, 4)
    # n_teams: Optional[int] = spec(54, 4)
    # n_d_records: Optional[int] = spec(58, 6)
    # n_swimmers: Optional[int] = spec(64, 6)
    # n_e_records: Optional[int] = spec(70, 5)
    # n_f_records: Optional[int] = spec(75, 6)
    # n_g_records: Optional[int] = spec(81, 6)
    # batch_number: Optional[int] = spec(87, 5)
    # n_new_members: Optional[int] = spec(92, 3)
    # n_renew_members: Optional[int] = spec(95, 3)
    # n_member_changes: Optional[int] = spec(98, 3)
    # n_member_deletes: Optional[int] = spec(101, 3)


# Add new/missing models
# sdif.model_meta.REGISTERED_MODELS["B2"] = MeetHostInfo

# Override models for TouchPad-specific sd3 quirks
sdif.model_meta.REGISTERED_MODELS["Z0"] = TouchPadFileTerminator


def transform_event_age(event_age_raw: str):
    age_code = "UNOV"  # default to no age limits
    if "-" in event_age_raw:
        age_code = "".join([age.zfill(2) for age in event_age_raw.split("-")])
    elif "&" in event_age_raw:
        if "under" in event_age_raw.lower():
            age_code = "UN" + event_age_raw.split(" ")[0].zfill(2)
        elif "over" in event_age_raw.lower():
            age_code = "".join([age for age in event_age_raw.split(" ") if age.isdecimal()]).zfill(2) + "OV"
    if len(age_code) != 4:
        raise ValueError(
            f'Could not parse event age of "{event_age_raw}" into a 4 character SDIF age code. Resulted in: "{age_code}"'
        )
    return age_code


def transform_swim_time(swim_time_raw: str) -> TimeT:
    if swim_time_raw is None or swim_time_raw.strip() == "":
        return TimeCode.no_swim
    tc_match = [tc.name for tc in TimeCode if tc.value == swim_time_raw]
    if tc_match:
        return TimeCode[tc_match[0]]
    return Time.from_str(swim_time_raw)


def format_event_results_xls_dataframe(xls_path: pathlib.Path):
    """Parse common event results fields (for individual events and relays) from
    Team Unify meet results XLS file into a Pandas DataFrame
    """
    ######################################
    #### IndividualEvent (D0) Records ####
    ######################################
    # ✔ organization: Optional[OrganizationCode] = spec(3, 1)
    # x name: str = spec(12, 28, t.name_)
    # x ussn: Optional[str] = spec(40, 12, m2=True)
    # ✔ attached: Optional[AttachCode] = spec(52, 1)
    # ✔ citizen: Optional[str] = spec(53, 3)
    # x birthdate: Optional[date] = spec(56, 8, m2=True)
    # x age_or_class: Optional[str] = spec(64, 2)
    # ✔ sex: SexCode = spec(66, 1)
    # ✔ event_sex: Optional[EventSexCode] = spec(67, 1)
    # ✔ event_distance: Optional[int] = spec(68, 4)
    # ✔ stroke: Optional[StrokeCode] = spec(72, 1)
    # ✔ event_number: Optional[str] = spec(73, 4)
    # ✔ event_age: Optional[str] = spec(77, 4)
    # ✔ date_of_swim: Optional[date] = spec(81, 8)
    # - seed_time: Optional[Time] = spec(89, 8)
    # - seed_time_course: Optional[CourseStatusCode] = spec(97, 1)
    # - prelim_time: Optional[TimeT] = spec(98, 8, t.time)
    # - prelim_time_course: Optional[CourseStatusCode] = spec(106, 1)
    # - swim_off_time: Optional[TimeT] = spec(107, 8, t.time)
    # - swim_off_time_course: Optional[CourseStatusCode] = spec(115, 1)
    # ✔ finals_time: Optional[TimeT] = spec(116, 8, t.time)
    # ✔ finals_time_course: Optional[CourseStatusCode] = spec(124, 1)
    # - prelim_heat_number: Optional[int] = spec(125, 2)
    # - prelim_lane_number: Optional[int] = spec(127, 2)
    # - finals_heat_number: Optional[int] = spec(129, 2)
    # - finals_lane_number: Optional[int] = spec(131, 2)
    # - prelim_place_ranking: Optional[int] = spec(133, 3)
    # ✔ finals_place_ranking: Optional[int] = spec(136, 3)
    # ✔ points_scored_finals: Optional[Decimal] = spec(139, 4)
    # - event_time_class: Optional[str] = spec(143, 2)
    # - flight_status: Optional[str] = spec(145, 1)
    # - centipoints_scored_finals: Optional[int] = spec(151, 2)

    xls_df = pd.read_excel(xls_path, engine="openpyxl")
    xls_df = xls_df.replace({np.nan: None})
    xls_df["organization"] = sdif.models.OrganizationCode.uss
    xls_df["attached"] = sdif.models.AttachCode.attached
    xls_df["citizen"] = "USA"

    xls_df[
        [
            "event_sex_name",
            "event_age_title",
            "event_distance",
            "event_stroke_name",
        ]
    ] = xls_df["Event"].str.extract(
        pat=r"(Female|Male|Mixed) \((.*?)\) \n([0-9]+) (Free Relay|Free|Back|Breast|Fly|IM|MR)",
        expand=True,
    )
    xls_df["stroke"] = xls_df["event_stroke_name"].map(
        {
            "Free": sdif.models.StrokeCode.freestyle,
            "Back": sdif.models.StrokeCode.backstroke,
            "Breast": sdif.models.StrokeCode.breaststroke,
            "Fly": sdif.models.StrokeCode.butterfly,
            "IM": sdif.models.StrokeCode.im,
            "MR": sdif.models.StrokeCode.medley_relay,
            "Free Relay": sdif.models.StrokeCode.free_relay,
        }
    )

    xls_df["event_sex"] = xls_df["event_sex_name"].map(
        {
            "Male": sdif.models.EventSexCode.male,
            "Female": sdif.models.EventSexCode.female,
            "Mixed": sdif.models.EventSexCode.mixed,
        }
    )
    xls_df["event_age"] = xls_df["event_age_title"].map(lambda x: transform_event_age(x)).astype(str)

    # Have to assume sex of the event, since Team Unify exports don"t track that on the results.
    # May fail if it's a mixed event
    xls_df["sex"] = xls_df["event_sex"].map(
        {
            sdif.models.EventSexCode.male: sdif.models.SexCode.male,
            sdif.models.EventSexCode.female: sdif.models.SexCode.female,
            sdif.models.EventSexCode.mixed: None,
        }
    )
    xls_df["finals_time"] = xls_df["Finals"].map(lambda x: transform_swim_time(x))
    # TODO: May need to cross reference this against where it was actually swam,
    #  which could be yards
    xls_df["finals_time_course"] = FINALS_COURSE

    date_field = "Date of\nSport" if "Date of\nSport" in xls_df.columns else "Date"
    xls_df["date_of_swim"] = pd.to_datetime(xls_df[date_field]).dt.date
    xls_df[["lsc", "team_code_tu"]] = xls_df["LSC-Team"].str.split(pat="-", expand=True)
    xls_df["team_code4"] = xls_df["team_code_tu"].str[:4]
    xls_df["team_code5"] = xls_df["team_code_tu"].str[4:]
    xls_df["team_code"] = xls_df["lsc"] + xls_df["team_code4"]

    xls_df.rename(
        columns={
            "Pts" if "Pts" in xls_df.columns else "Points": "points_scored_finals",
            "Finals Pos": "finals_place_ranking",
        },
        inplace=True,
    )
    xls_df["points_scored_finals"] = xls_df["points_scored_finals"].apply(lambda x: Decimal(x) if x else None)
    return xls_df


def format_relay_event_results_xls_dataframe(xls_path, max_individual_event):
    """Parse common relay event results fields from
    Team Unify meet results relay XLS file into a Pandas DataFrame
    """
    xls_df = format_event_results_xls_dataframe(xls_path)
    xls_df[_INDIVIDUAL_OR_RELAY_FIELD] = "R"
    xls_df = sort_event_results_dataframe(xls_df, event_number_offset=max_individual_event)

    # Relay_team_name (12/1) is one alpha char, like 'A', 'B', 'C', or 'D'
    # It's followed by team_code (13/6), which should be the same as the team_code team name used in
    # record C1 (Team Id): team_code (12/6)
    # Team Codes follow the USS SDIF format:
    # 	 TEAM Code 006     LSC and Team code
    #    Supplied from USS Headquarters files upon request.
    #    Concatenation of two-character LSC code and four-character
    #    Team code, in that order (e.g., Colorado's FAST would be
    #    COFAST).  The code for Unattached should always be UN, and
    #    not any other abbreviation.  (Florida Gold's unattached
    #    would be FG  UN.)
    # If a team's code without the LSC part is more than 4 characters, and uses a 5th,
    # the 5th character falls into the optional team_code5 (150/1) single character field
    # Because it must fit in 6 characters for team_code, drop the LSC part.
    # e.g. for LSC-Team value of "OD-WWST", team_code should be just "ODWWST"
    xls_df["relay_team_name"] = xls_df["Relay\nTeam"].map(lambda x: x.split("\n")[0][-1])

    # Rename individual fields to match names used in RelayEvent
    xls_df.rename(
        columns={
            "points_scored_finals": "finals_points",
            "finals_place_ranking": "finals_place",
            "finals_time_course": "finals_course",
            "event_distance": "relay_distance",
            "date_of_swim": "swim_date",
        },
        inplace=True,
    )

    return xls_df


def sort_event_results_dataframe(
    xls_df: pd.DataFrame,
    event_field: str = "Event",
    stroke_field: str = "stroke",
    event_age_field: str = "event_age",
    event_sex_field: str = "event_sex",
    individual_or_relay_field: str = _INDIVIDUAL_OR_RELAY_FIELD,
    stroke_sort_val: Tuple[str, Callable] = (
        "stroke_sort_val",
        lambda x: x.value,
    ),
    event_sex_sort_val: Tuple[str, Callable] = (
        "event_sex_sort_val",
        lambda x: x.value,
    ),
    extra_sort_vals: List[Tuple[str, Callable]] = None,
    event_number_offset: int = 0,
):
    """Infer the event_number for each result by building an event-only DataFrame
    with events sorted in event_number order.
    """
    # Get Enum object values for sorting
    xls_df[stroke_sort_val[0]] = xls_df[stroke_field].map(stroke_sort_val[1])
    xls_df[event_sex_sort_val[0]] = xls_df[event_sex_field].map(event_sex_sort_val[1])

    # Get a sortable event age number
    xls_df["event_age_sortable"] = xls_df[event_age_field].str.replace("UN", "00").str.replace("OV", "99").astype(int)

    # Add another sort dimension to push the FR Open Relays last
    xls_df["is_open_free_relay"] = xls_df.apply(
        lambda x: x[event_age_field] == "UNOV" and x[stroke_field] == sdif.models.StrokeCode.free_relay,
        axis="columns",
    ).astype(bool)

    if extra_sort_vals:
        for sv in extra_sort_vals:
            xls_df[f"{sv[0]}_sort_val"] = xls_df[sv[0]].map(sv[1]())
    xls_df.sort_values(
        by=[
            "is_open_free_relay",
            individual_or_relay_field,
            stroke_sort_val[0],
            "event_age_sortable",
            event_sex_sort_val[0],
        ],
        ascending=[True, True, True, True, False],
        inplace=True,
    )
    events_df = xls_df[[event_field]].copy()
    events_df.drop_duplicates(inplace=True, ignore_index=True)
    events_df.reset_index()
    events_df.index = events_df.index + event_number_offset + 1
    events_df["event_number"] = events_df.index

    xls_df = xls_df.join(events_df.set_index(event_field), on=event_field)
    return xls_df


def format_individual_xls_dataframe(xls_path: pathlib.Path):
    ######################################
    #### IndividualEvent (D0) Records ####
    ######################################
    # x organization: Optional[OrganizationCode] = spec(3, 1)
    # ✔ name: str = spec(12, 28, t.name_)
    # ✔ ussn: Optional[str] = spec(40, 12, m2=True)
    # x attached: Optional[AttachCode] = spec(52, 1)
    # x citizen: Optional[str] = spec(53, 3)
    # ✔ birthdate: Optional[date] = spec(56, 8, m2=True)
    # ✔ age_or_class: Optional[str] = spec(64, 2)
    # x sex: SexCode = spec(66, 1)
    # x event_sex: Optional[EventSexCode] = spec(67, 1)
    # x event_distance: Optional[int] = spec(68, 4)
    # x stroke: Optional[StrokeCode] = spec(72, 1)
    # x event_number: Optional[str] = spec(73, 4)
    # x event_age: Optional[str] = spec(77, 4)
    # x date_of_swim: Optional[date] = spec(81, 8)
    # - seed_time: Optional[Time] = spec(89, 8)
    # - seed_time_course: Optional[CourseStatusCode] = spec(97, 1)
    # - prelim_time: Optional[TimeT] = spec(98, 8, t.time)
    # - prelim_time_course: Optional[CourseStatusCode] = spec(106, 1)
    # - swim_off_time: Optional[TimeT] = spec(107, 8, t.time)
    # - swim_off_time_course: Optional[CourseStatusCode] = spec(115, 1)
    # x finals_time: Optional[TimeT] = spec(116, 8, t.time)
    # x finals_time_course: Optional[CourseStatusCode] = spec(124, 1)
    # - prelim_heat_number: Optional[int] = spec(125, 2)
    # - prelim_lane_number: Optional[int] = spec(127, 2)
    # - finals_heat_number: Optional[int] = spec(129, 2)
    # - finals_lane_number: Optional[int] = spec(131, 2)
    # - prelim_place_ranking: Optional[int] = spec(133, 3)
    # x finals_place_ranking: Optional[int] = spec(136, 3)
    # x points_scored_finals: Optional[Decimal] = spec(139, 4)
    # - event_time_class: Optional[str] = spec(143, 2)
    # - flight_status: Optional[str] = spec(145, 1)
    # - centipoints_scored_finals: Optional[int] = spec(151, 2)

    xls_df = format_event_results_xls_dataframe(xls_path)
    xls_df[_INDIVIDUAL_OR_RELAY_FIELD] = "I"
    xls_df = sort_event_results_dataframe(xls_df)

    xls_df[["name", "uss_number_new"]] = xls_df["Athlete Name"].str.split(pat="\n", expand=True)

    # Keep the newer 14-character "new" uss_number around for building the IndividualInfo D3 records
    # But ensure it's at most 14 characters (some bad IDs in old meets)
    xls_df["uss_number_new"] = xls_df["uss_number_new"].str.slice(start=0, stop=14)  # must truncate, can only fit 14
    xls_df["ussn"] = xls_df["uss_number_new"].str.slice(start=0, stop=12)  # must truncate, can only fit 12

    # Get their age
    xls_df[["swimmer_age_at_date_of_swim", "swimmer_age_now"]] = xls_df["EventAge\nCurrent"].str.split(
        pat="\n", expand=True
    )
    xls_df["age_or_class"] = xls_df["swimmer_age_at_date_of_swim"]
    
    # Only derive a mmddyy birthday if you find six consecutive digits, fitting a birthday format, from the start of the USS#
    mdy_regex = r"^(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])(\d\d)"
    bday_df =xls_df["ussn"].str.extract(pat=mdy_regex, expand=False)
    xls_df["_birthdate_mmddyy"] = bday_df[0] + bday_df[1] + bday_df[2]  # concat mm + dd + yy from the regex extraction

    # If the USS Number is messed up and not containing a birthday, force it to Jan 1 of their birth year
    xls_df["_birthdate_mmddyy"] = xls_df.apply(
        lambda x: datetime.date(x["date_of_swim"].year - int(x["age_or_class"]), 1, 1).strftime("%m%d%y") if pd.isna(x["_birthdate_mmddyy"]) else x["_birthdate_mmddyy"],
        axis="columns"
    )

    xls_df["birthdate"] = pd.to_datetime(xls_df["_birthdate_mmddyy"], format="%m%d%y").dt.date

    xls_df[["last_name", "_first_and_middle_i"]] = xls_df["name"].str.rsplit(pat=", ", n=1, expand=True)
    xls_df[["first_name", "middle_initial"]] = xls_df["_first_and_middle_i"].str.rsplit(n=1, expand=True)

    xls_df.drop(
        [
            "_first_and_middle_i",
            "EventAge\nCurrent",
            "LSC-Team",
            "Date of\nSport",
            "_birthdate_mmddyy",
        ],
        axis="columns",
        inplace=True,
    )

    sd3_df = xls_df[
        [
            "organization",
            "name",
            "lsc",  # Keeping around to help partition records by team
            "team_code_tu",  # Keeping around to help partition records by team
            "uss_number_new",  # Keeping around for other SIDF records that need it
            "ussn",
            "attached",
            "citizen",
            "birthdate",
            "age_or_class",
            "sex",
            "event_sex",
            "event_distance",
            "stroke",
            "event_number",
            "event_age",
            "date_of_swim",
            "finals_time",
            "finals_time_course",
            "finals_place_ranking",
            "points_scored_finals",
        ]
    ].copy()

    missing_columns = [
        "seed_time",
        "seed_time_course",
        "prelim_time",
        "prelim_time_course",
        "swim_off_time",
        "swim_off_time_course",
        "prelim_heat_number",
        "prelim_lane_number",
        "finals_heat_number",
        "finals_lane_number",
        "prelim_place_ranking",
        "event_time_class",
        "flight_status",
        "centipoints_scored_finals",
    ]
    for mc in missing_columns:
        sd3_df[mc] = None

    return sd3_df


def format_relay_xls_dataframe(xls_path: pathlib.Path, max_individual_event: int = 1):
    #########################
    #### RelayEvent (E0) ####
    #########################
    # ✔ organization: Optional[OrganizationCode] = spec(3, 1, m2=True)
    # ✔ relay_team_name: str = spec(12, 1)
    # ✔ team_code: str = spec(13, 6)
    # n_f0_records: Optional[int] = spec(19, 2)
    # ✔ event_sex: EventSexCode = spec(21, 1)
    # ✔ relay_distance: int = spec(22, 4)
    # ✔ stroke: StrokeCode = spec(26, 1)
    # ✔ event_number: Optional[str] = spec(27, 4)
    # ✔ event_age: str = spec(31, 4)
    # total_athlete_age: Optional[int] = spec(35, 3, override_m1=True)
    # ✔ swim_date: Optional[date] = spec(38, 8)
    # seed_time: Optional[TimeT] = spec(46, 8)
    # seed_course: Optional[CourseStatusCode] = spec(54, 1)
    # prelim_time: Optional[TimeT] = spec(55, 8)
    # prelim_course: Optional[CourseStatusCode] = spec(63, 1)
    # swimoff_time: Optional[TimeT] = spec(64, 8)
    # swimoff_course: Optional[CourseStatusCode] = spec(72, 1)
    # ✔ finals_time: Optional[TimeT] = spec(73, 8)
    # ✔ finals_course: Optional[CourseStatusCode] = spec(81, 1)
    # prelim_heat: Optional[int] = spec(82, 2)
    # prelim_lane: Optional[int] = spec(84, 2)
    # finals_heat: Optional[int] = spec(86, 2)
    # finals_lane: Optional[int] = spec(88, 2)
    # prelim_place: Optional[int] = spec(90, 3)
    # ✔ finals_place: Optional[int] = spec(93, 3)
    # ✔ finals_points: Optional[Decimal] = spec(96, 4)
    # event_time_class_lower: Optional[EventTimeClassCode] = spec(100, 1)
    # event_time_class_upper: Optional[EventTimeClassCode] = spec(101, 1)

    ############################################################
    #### RelayName (F0) - Names of swimmers in a relay team ####
    ############################################################
    # ✔ organization: Optional[OrganizationCode] = spec(3, 1, m2=True)
    # ✔ team_code: str = spec(16, 6)
    # ✔ relay_team_name: Optional[str] = spec(22, 1)
    # x swimmer_name: str = spec(23, 28, t.name_)
    # x uss_number: Optional[str] = spec(51, 12)
    # x citizen: Optional[str] = spec(63, 3)
    # x birthdate: Optional[date] = spec(66, 8, m2=True)
    # x age_or_class: Optional[str] = spec(74, 2)
    # x sex: SexCode = spec(76, 1)
    # - prelim_order: Optional[OrderCode] = spec(77, 1, override_m1=True)
    # - swimoff_order: Optional[OrderCode] = spec(78, 1, override_m1=True)
    # x finals_order: OrderCode = spec(79, 1)
    # - leg_time: Optional[TimeT] = spec(80, 8)
    # x course: Optional[CourseStatusCode] = spec(88, 1)
    # - takeoff_time: Optional[Decimal] = spec(89, 4)
    # - uss_number_new: Optional[str] = spec(93, 14, t.ussnum, m2=True)
    # - preferred_first_name: Optional[str] = spec(107, 15)

    xls_df = format_relay_event_results_xls_dataframe(xls_path, max_individual_event)

    sd3_df = xls_df[
        [
            "organization",
            "team_code",
            "team_code_tu",  # Keeping around to allow partitioning DataFrame by team
            "relay_team_name",
            "event_sex",
            "relay_distance",
            "stroke",
            "event_number",
            "event_age",
            "swim_date",
            "finals_time",
            "finals_course",
            "finals_place",
            "finals_points",
        ]
    ].copy()

    missing_columns = [
        "n_f0_records",
        "total_athlete_age",
        "seed_time",
        "seed_course",
        "prelim_time",
        "prelim_course",
        "swimoff_time",
        "swimoff_course",
        "prelim_heat",
        "prelim_lane",
        "finals_heat",
        "finals_lane",
        "prelim_place",
        "event_time_class_lower",
        "event_time_class_upper",
    ]
    for mc in missing_columns:
        sd3_df[mc] = None

    return sd3_df


def format_swimmers_dataframe(xls_indiv_df: pd.DataFrame):
    """Consolidate individual events into a distinct DataFrame of swimmers"""
    swimmer_df = xls_indiv_df[
        [
            "lsc",
            "team_code_tu",
            "name",
            "ussn",
            "uss_number_new",
            "attached",
            "citizen",
            "birthdate",
            "age_or_class",
            "sex",
        ]
    ].copy()
    swimmer_df.drop_duplicates(inplace=True, ignore_index=True)
    swimmer_df.reset_index()
    return swimmer_df


def format_individual_info_dataframe(xls_indiv_df: pd.DataFrame):
    #############################
    #### IndividualInfo (D3) ####
    #############################
    # ✔ uss_number: Optional[str] = spec(3, 14, t.ussnum, m2=True)
    # - preferred_first_name: Optional[str] = spec(17, 15)
    # - ethnicity_1: Optional[EthnicityCode] = spec(32, 1)
    # - ethnicity_2: Optional[EthnicityCode] = spec(33, 1)
    # - junior_high: Optional[bool] = spec(34, 1)
    # - senior_high: Optional[bool] = spec(35, 1)
    # - ymca_ywca: Optional[bool] = spec(36, 1)
    # - college: Optional[bool] = spec(37, 1)
    # ✔ summer_league: Optional[bool] = spec(38, 1)
    # - masters: Optional[bool] = spec(39, 1)
    # - disabled_sports_org: Optional[bool] = spec(40, 1)
    # - water_polo: Optional[bool] = spec(41, 1)
    # - none: Optional[bool] = spec(42, 1)

    swimmers_df = format_swimmers_dataframe(xls_indiv_df)

    sd3_df = swimmers_df[["team_code_tu", "uss_number_new"]].copy()  # Keeping around to help split records by team
    sd3_df["summer_league"] = IS_SUMMER_LEAGUE

    sd3_df.rename(
        columns={
            "uss_number_new": "uss_number",
        },
        inplace=True,
    )

    missing_columns = [
        "preferred_first_name",
        "ethnicity_1",
        "ethnicity_2",
        "junior_high",
        "senior_high",
        "ymca_ywca",
        "college",
        "masters",
        "disabled_sports_org",
        "water_polo",
        "none",
    ]
    for mc in missing_columns:
        sd3_df[mc] = None

    return sd3_df


def format_relay_swimmers_xls_dataframe(
    xls_indiv_df: pd.DataFrame, xls_path: pathlib.Path, max_individual_event: int = 1
):
    ############################################################
    #### RelayName (F0) - Names of swimmers in a relay team ####
    ############################################################
    # x organization: Optional[OrganizationCode] = spec(3, 1, m2=True)
    # x team_code: str = spec(16, 6)
    # x relay_team_name: Optional[str] = spec(22, 1)
    # ✔ swimmer_name: str = spec(23, 28, t.name_)
    # ✔ uss_number: Optional[str] = spec(51, 12)
    # ✔ citizen: Optional[str] = spec(63, 3)
    # ✔ birthdate: Optional[date] = spec(66, 8, m2=True)
    # ✔ age_or_class: Optional[str] = spec(74, 2)
    # ✔ sex: SexCode = spec(76, 1)
    # - prelim_order: Optional[OrderCode] = spec(77, 1, override_m1=True)
    # - swimoff_order: Optional[OrderCode] = spec(78, 1, override_m1=True)
    # ✔ finals_order: OrderCode = spec(79, 1)
    # - leg_time: Optional[TimeT] = spec(80, 8)
    # ✔ course: Optional[CourseStatusCode] = spec(88, 1)
    # - takeoff_time: Optional[Decimal] = spec(89, 4)
    # ✔ uss_number_new: Optional[str] = spec(93, 14, t.ussnum, m2=True)
    # - preferred_first_name: Optional[str] = spec(107, 15)

    # Start with parsing+formatting the common relay event fields
    xls_df = format_relay_event_results_xls_dataframe(xls_path, max_individual_event).copy()

    # Transform the relay team text into a list of relay swimmer names
    xls_df["relay_swimmers"] = xls_df["Relay\nTeam"].str.replace(" »", "").str.split(pat="\n", expand=False).str[-4:]

    # Explode the embedded lists into their own Series that maintains the index of the original row they came from,
    # and join back with original DataFrame to create a product of the two
    xls_df = xls_df.join(xls_df["relay_swimmers"].explode(), lsuffix="_list", rsuffix="_name")

    # Set position (order) of each swimmer in their relay
    xls_df["finals_order"] = xls_df.apply(
        lambda x: sdif.models.OrderCode(str(x["relay_swimmers_list"].index(x["relay_swimmers_name"]) + 1)),
        axis="columns",
    )

    # Join relays-with-swimmer-names DataFrame back to individual swimmer events DataFrame to get swimmer details
    # This is the best we can do since the Relay Swimmers in the Team Unify Relay XLS download don't include any more
    # detail than their full names.
    # This may cause conflicts if two swimmers in the same meet and on the same team have the same full name
    swimmer_df = format_swimmers_dataframe(xls_indiv_df)
    xls_df = xls_df.join(
        swimmer_df.set_index(["name", "team_code_tu"]),
        on=["relay_swimmers_name", "team_code_tu"],
        rsuffix="_from_relay",
    )

    xls_df = xls_df.replace({np.nan: None})

    # Rename fields from IndividualEvent or RelayEvent formatting, to match RelayName field names
    xls_df.rename(
        columns={
            "finals_course": "course",
            "relay_swimmers_name": "swimmer_name",
            "ussn": "uss_number",
        },
        inplace=True,
    )

    sd3_df = xls_df[
        [
            "organization",
            "team_code",
            "team_code_tu",  # Keeping around to allow partitioning DataFrame by team
            "relay_team_name",
            "swimmer_name",
            "uss_number",
            "citizen",
            "birthdate",
            "age_or_class",
            "sex",
            "finals_order",
            "course",
            "uss_number_new",
            # From the RelayEvent -- Keeping these around to allow lookup of relay swimmers by relay event
            "event_number",
            "event_sex",
            "relay_distance",
            "stroke",
            "event_age",
        ]
    ].copy()

    missing_columns = [
        "prelim_order",
        "swimoff_order",
        "leg_time",
        "takeoff_time",
        "preferred_first_name",
    ]
    for mc in missing_columns:
        sd3_df[mc] = None

    return sd3_df


def build_sd3(individual_xls: pathlib.Path, relay_xls: pathlib.Path = None):
    """Compile SDIF records that are formatted/structured from the provided XLS files, and write them to a new .sd3 file

    NOTE: ORDER of the SDIF records in an .sd3 file is important
    - The only way to attribute a swam event from an individual or relay team to a Swim Team is the fact that those records
      (D0 and E0/F0) must FOLLOW the C1 Team ID record of the team those swims belong to.

    This function writes the record parsed from the XLS files in the required order.
    """
    print(f"Building .sd3 file from:\n\tIndividual Results File: {individual_xls}\n\tRelay Results File: {relay_xls}")

    base_name = re.sub(r"individual", "", str(individual_xls).rsplit(".")[0], flags=re.I)
    base_name = "results" if not base_name else base_name

    sd3_records = []

    lsc = SWIM_TEAM_INFO["swim_team"]["local_swim_committee"]
    our_team_code_tu = SWIM_TEAM_INFO["swim_team"]["team_unify_team_code"]

    # SDIF files only allow 4 character team_codes, preceded by a two-character LSC, for a total of 6
    # If a team code in a league has 5 characters, the 5th is stored in the "team_code5" field
    our_team_code4 = our_team_code_tu[:4]
    our_team_code5 = our_team_code_tu[4:]
    teams_in_meet = [
        t.split("-")[1]
        for t in pd.read_excel(individual_xls, engine="openpyxl")["LSC-Team"]
        .drop_duplicates(ignore_index=True)
        .to_list()
    ]

    # Parse XLS files and build-up the DataFrames
    xls_indiv_df = format_individual_xls_dataframe(individual_xls)
    max_individual_event = xls_indiv_df["event_number"].max()
    xls_relay_df = None
    xls_relay_swimmers_df = None
    if relay_xls:
        xls_relay_df = format_relay_xls_dataframe(relay_xls, max_individual_event)
        xls_relay_swimmers_df = format_relay_swimmers_xls_dataframe(
            xls_indiv_df=xls_indiv_df, xls_path=relay_xls, max_individual_event=max_individual_event
        )

    # A0
    a0 = sdif.models.FileDescription(
        organization=sdif.models.OrganizationCode.uss,
        sdif_version="V3",
        file_code=sdif.models.FileCode.meet_results,
        software_name="ODSL TU XLS to SD3",
        software_version="v0.0.0",
        contact_name="Keith Hickey",
        contact_phone="+15555551212",
        file_creation=datetime.date.today(),
        submitted_by_lsc=None,
    )

    # B1
    b1 = sdif.models.Meet(
        organization=sdif.models.OrganizationCode.uss,
        meet_name=base_name[0:30],
        meet_address_1=SWIM_TEAM_INFO["swim_team"]["address_line_1"],
        meet_address_2=None,
        meet_city=SWIM_TEAM_INFO["swim_team"]["city"],
        meet_state=SWIM_TEAM_INFO["swim_team"]["state"],
        postal_code=SWIM_TEAM_INFO["swim_team"]["postal_code"],
        country="USA",
        meet=sdif.models.MeetTypeCode.dual,
        meet_start=xls_indiv_df["date_of_swim"].min(),
        meet_end=xls_indiv_df["date_of_swim"].max(),
        pool_altitude_ft=0,
        # TODO: May need to cross reference this against where it was actually swam,
        #  which could be yards
        course=FINALS_COURSE,
    )

    # C1
    c1_us = sdif.models.TeamId(
        organization=sdif.models.OrganizationCode.uss,
        team_code=lsc + our_team_code4,
        name=SWIM_TEAM_INFO["swim_team"]["full_name"],
        abbreviation=SWIM_TEAM_INFO["swim_team"]["abbreviation"],
        address_1=SWIM_TEAM_INFO["swim_team"]["address_line_1"],
        address_2=None,
        city=SWIM_TEAM_INFO["swim_team"]["city"],
        state=SWIM_TEAM_INFO["swim_team"]["state"],
        postal_code=SWIM_TEAM_INFO["swim_team"]["postal_code"],
        country="USA",
        region=None,
        team_code5=our_team_code5,
    )

    # Append Meet setup records and our team's individual records
    sd3_records.extend([a0, b1, c1_us])

    # D0 and D3
    sd3_records.extend(generate_individual_records(our_team_code_tu, xls_indiv_df))

    # E0 and F0
    if relay_xls:
        sd3_records.extend(generate_relay_records(our_team_code_tu, xls_relay_df, xls_relay_swimmers_df))

    # Generate opposing teams' TeamId (C1) record, individual records (D0, D3), and relay records (E0, F0)
    for opp in teams_in_meet:
        if opp == our_team_code_tu:
            continue
        # See comment above about 4 and 5 character codes
        opp_team_code4 = opp[:4]
        opp_team_code5 = opp[4:]

        # C1
        c1_opp = sdif.models.TeamId(
            organization=sdif.models.OrganizationCode.uss,
            team_code=lsc + opp_team_code4,
            name=lsc + "-" + opp,
            abbreviation=None,
            address_1=None,
            address_2=None,
            city=None,
            state="VA",
            postal_code=None,
            country="USA",
            region=None,
            team_code5=opp_team_code5,
        )
        sd3_records.append(c1_opp)

        # D0 and D3
        sd3_records.extend(generate_individual_records(opp, xls_indiv_df))

        # E0 and F0
        if relay_xls:
            sd3_records.extend(generate_relay_records(opp, xls_relay_df, xls_relay_swimmers_df))

    # Z0
    z0 = TouchPadFileTerminator(
        organization=sdif.models.OrganizationCode.uss,
        file_code=sdif.models.FileCode.meet_results,
        notes=None,
    )
    sd3_records.append(z0)

    # Build the .sd3 file in the same directory
    sd3_file_name = f"{base_name}.sd3"
    with open(f"{base_name}.sd3", "w") as f:
        f.write(sdif.records.encode_records(sd3_records))

    print(f"Built SDIF file: {sd3_file_name}")

def generate_individual_records(team_code_tu: str, xls_indiv_df: pd.DataFrame):
    """Generate D0 IndividualEvent and singular D3 IndividualInfo records in the order required by SDIF files for the given team

    Args:
        xls_indiv_df (DataFrame): The pandas DataFrame of individual event results parsed from the XLS individual results file
        team_code_tu (str): The Team Unify code (max of 5 chars, and without LSC prepended) for the team for whose individuals
            you want to generate records
    """
    individual_key_col = "uss_number"

    # Filter meet individual results by given team
    xls_indiv_df_team = xls_indiv_df[xls_indiv_df.team_code_tu == team_code_tu].copy()

    # Format into IndividualInfo DataFrame
    ii_df = format_individual_info_dataframe(xls_indiv_df_team)
    ii_df.drop("team_code_tu", axis="columns", inplace=True)
    ii_df.set_index(individual_key_col, inplace=True)
    ii_df[individual_key_col] = ii_df.index
    ii_dict = ii_df.to_dict(orient="index")

    # Cleanup and sort by individuals' last name
    xls_indiv_df_team.drop(["lsc", "team_code_tu"], axis="columns", inplace=True)
    xls_indiv_df_team.sort_values(by=["name"], inplace=True)

    # d0s_and_d3s_us = []
    for d0_dict in xls_indiv_df_team.to_dict(orient="records"):
        d3_dict = ii_dict.pop(
            d0_dict.pop("uss_number_new"), None
        )  # pop ensures D3 records is only written once per individual
        yield sdif.models.IndividualEvent(**d0_dict)  # D0
        if d3_dict:
            yield sdif.models.IndividualInfo(**d3_dict)


def generate_relay_records(team_code_tu: str, xls_relay_df: pd.DataFrame, xls_relay_swimmers_df: pd.DataFrame):
    """Generate E0 RelayEvent and F0 RelayName records in the order required by SDIF files for the given team

    Args:
        team_code_tu (str): The Team Unify code (max of 5 chars, and without LSC prepended) for the team for whose individuals
            you want to generate records
        xls_relay_df (DataFrame): The pandas DataFrame of relay event results parsed from the XLS relay results file
        xls_relay_swimmers_df (DataFrame): The pandas DataFrame of swimmers in each relay event result parsed from the XLS relay results file
    """
    xls_relay_df_team = xls_relay_df[xls_relay_df.team_code_tu == team_code_tu].copy()
    xls_relay_df_team.drop("team_code_tu", axis="columns", inplace=True)
    xls_relay_swimmers_df_team = xls_relay_swimmers_df[xls_relay_swimmers_df.team_code_tu == team_code_tu].copy()
    xls_relay_swimmers_df_team.drop("team_code_tu", axis="columns", inplace=True)

    for e0_dict in xls_relay_df_team.to_dict(orient="records"):
        yield sdif.models.RelayEvent(**e0_dict)  # E0

        # Lookup swimmers in this relay event by event details
        swimmers_in_relay_df = xls_relay_swimmers_df_team[
            (xls_relay_swimmers_df_team.relay_team_name == e0_dict["relay_team_name"])
            & (xls_relay_swimmers_df_team.event_number == e0_dict["event_number"])
            & (xls_relay_swimmers_df_team.event_sex == e0_dict["event_sex"])
            & (xls_relay_swimmers_df_team.relay_distance == e0_dict["relay_distance"])
            & (xls_relay_swimmers_df_team.stroke == e0_dict["stroke"])
            & (xls_relay_swimmers_df_team.event_age == e0_dict["event_age"])
        ].copy()

        # Drop the fields kept around for RelayEvent lookup, that are not captured in the RelayName record
        swimmers_in_relay_df.drop(
            [
                "event_number",
                "event_sex",
                "relay_distance",
                "stroke",
                "event_age",
            ],
            axis="columns",
            inplace=True,
        )
        yield from (sdif.models.RelayName(**rs) for rs in swimmers_in_relay_df.to_dict(orient="records"))  # F0s


def parse_sd3(file_path: pathlib.Path):
    rtc_list = []
    with open(file_path, "rt") as sd3_file:
        print(f"Attempting to parse all lines of {file_path}...")
        for line in sd3_file.readlines():
            rtc = line[:2]
            if rtc not in rtc_list:
                print(f'Found new record type code "{rtc}". Parsing first line with this code')
            rtc_list.append(rtc)
            try:
                record_type = sdif.model_meta.REGISTERED_MODELS[rtc]
                rec = sdif.records.decode_record(record=line, record_type=record_type, strict=False)
            except Exception as e:
                print(f'Failed to parse line with record type code "{rtc}":\n\t{line}')
                print(str(e))
                # Swallow, and keep going
    print(pd.DataFrame(rtc_list).value_counts().to_string())


def print_sd3(file_path: pathlib.Path):
    with open(file_path, "rt") as sd3_file:
        for line in sd3_file.readlines():
            print(f"{line}")


def print_xls(
    file_path: pathlib.Path,
    second_file_path: pathlib.Path = None,
    format: Literal["raw", "individual", "relay_event", "relay_swimmers"] = "raw",
):
    if format == "raw":
        xls_df = pd.read_excel(file_path, engine="openpyxl")
    elif format == "individual":
        xls_df = format_individual_xls_dataframe(file_path)
    elif format == "relay_event":
        xls_df = format_relay_xls_dataframe(file_path)
    elif format == "relay_swimmers":
        xls_indiv_df = format_individual_xls_dataframe(file_path)
        xls_df = format_relay_swimmers_xls_dataframe(xls_indiv_df=xls_indiv_df, xls_path=second_file_path)
    else:
        raise ValueError(f"format = {format} is not valid")

    print(xls_df.dtypes)
    print(xls_df.to_string())
    print(xls_df.dtypes)


def concat_xls(base_name: str):
    files = [
        pathlib.Path(f"{base_name}_free.xls"),
        pathlib.Path(f"{base_name}_back.xls"),
        pathlib.Path(f"{base_name}_breast.xls"),
        pathlib.Path(f"{base_name}_fly.xls"),
        pathlib.Path(f"{base_name}_im.xls"),
    ]
    if all(not f.exists for f in files):
        print(f"No files found to combine given base path {base_name}")
        return

    concat_df = pd.concat((pd.read_excel(f, engine="openpyxl") for f in files if f.exists()), ignore_index=True)
    concat_path = pathlib.Path(f"{base_name}_concat.xls")
    with pd.ExcelWriter(concat_path, engine="openpyxl") as writer:  # defaults to openpyxl when writing .xls extension
        # Have to get into the guts of the writer to apply wrap style, so that newlines from the downloaded .xls
        # files are maintained after concatenation
        concat_df.to_excel(writer, sheet_name="Individual Results", index=False)
        worksheet = writer.sheets["Individual Results"]
        wrapped = Alignment(wrap_text=True)
        for row in worksheet.iter_rows():
            for cell in row:
                cell.alignment = wrapped
    print(f"Files combined into {concat_path}")


def main():
    if len(sys.argv) == 1:
        print("Use one of these subcommands:\n\tconcat\n\tprint\n\tparse\n\tbuild")
        return

    relay_only = False
    file_arg = 2 if sys.argv[1] in ("print", "parse", "build") else 1
    individual_xls = pathlib.Path(sys.argv[file_arg])
    relay_xls = None
    if len(sys.argv) == file_arg + 1 and "relay" in sys.argv[file_arg].lower():
        relay_only = True  # processing just a relay file
    elif len(sys.argv) == file_arg + 2 and sys.argv[file_arg + 1].rsplit(".")[-1].startswith("xls"):
        relay_xls = pathlib.Path(sys.argv[file_arg + 1])

    if sys.argv[1] == "concat":
        if len(sys.argv) < 3 or sys.argv[2].rsplit(".")[-1].startswith("xls"):
            print(
                "Provide just the base file name, with no extension, of the Excel files to combine. Prepending the path if not in the current directory.\n"
                "It is assumed each file will be named as <base_name>_free.xls, <base_name>_back.xls, <base_name>_breast.xls, <base_name>_fly.xls, "
                "<base_name>_im.xls"
            )
        base_name = sys.argv[2]
        concat_xls(base_name)
        return

    if sys.argv[1] == "print":
        if sys.argv[2].rsplit(".")[-1].startswith("xls"):
            print_xls(pathlib.Path(sys.argv[2]), format="raw")
        else:
            print_sd3(pathlib.Path(sys.argv[2]))
        return

    if sys.argv[1] == "parse":
        if sys.argv[2].rsplit(".")[-1].startswith("xls"):
            if relay_only:
                print_xls(pathlib.Path(sys.argv[2]), format="relay_event")
            elif relay_xls:  # "parse" relay XLS and passed in individual file ==> parse it for relay swimers
                print_xls(individual_xls, relay_xls, format="relay_swimmers")
            else:
                print_xls(pathlib.Path(sys.argv[2]), format="individual")
        else:
            parse_sd3(pathlib.Path(sys.argv[2]))
        return

    if sys.argv[1].endswith(".sd3"):
        parse_sd3(pathlib.Path(sys.argv[1]))
        return

    if sys.argv[1].rsplit(".")[-1].startswith("xls") or sys.argv[1] == "build":
        build_sd3(individual_xls, relay_xls)
        return


if __name__ == "__main__":
    main()
