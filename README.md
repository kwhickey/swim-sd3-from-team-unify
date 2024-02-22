swim-sd3-from-team-unify
====

Scripts and utilities to parse Team Unify and TouchPad meet results and convert to .sd3 format for import elsewhere

## `sd3_from_tu_meet_results.py`

_Script to convert, read, or write Swimming Data Interchange Files (SDIF, .sd3) from a 
Team Unify and/or TouchPad source._

This script can be used to import swim meet results from Team Unify to SwimTopia.

### Quickstart

1. Log into your Team Unify site
2. In your profile, go to "Back Office"
3. Left-nav menu, choose Events & Competitions > Meet Results
4. Open your meet
5. Change the "Stroke" filter to one stroke, and "Search"
6. Export to Excel (XLS)
7. Repeat Search and Export for each stroke type, and save the file with the stroke name in lowercase at the end, e.g. `meet_free.xls`, `meet_back.xls`, ..., `meet_im.xls`. Save them alongside the `sd3_from_tu_meet_results.py` file
    - _The reason for this is that Team Unify only lets you download 400 results at a time, so you must filter to get less than that, so results aren't truncated._
    - ***Make sure each file has less than 400 results in it!***. Add more search filters if needed.
8. Go on to the "Relays" tab of the meet, Search and Export as well into `meet_relay.xls`
8. Open a shell in this directory
9. `pip install sdif pandas openpyxl`
10. `python3 sd3_from_tu_meet_results.py concat meet`, changing `meet` to whatever the base name of your saved XLS files is before the `_stroke.xls`
12. `python3 sd3_from_tu_meet_results.py build meet_concat.xls meet_relay.xls`

Take the generated `.sd3` file and import it into a SwimTopia meet.

#### _Tips_:

- Best to have imported the Roster to SwimTopia, in the Season of this meet, before importing the meet
- There's a bug in SwimTopia import such that auto-creating Events on import doesn't work. Create the standard 64 events manually, use a SwimTopia team or league template, or find a `.ev3` for uploading (like the [one in this repo](event_template_dual_64.ev3)).

## `fetch_touchpad_live_meets.py`

_**Script to pull meet info for a team or set of teams from TouchPad Live via their REST API.**_

Can be expanded to get meet entry and result details, however results do not have the USA Swimming Number for swimmers, so not readily useful for building an SDIF .sd3 results file.

This script can be used to import swim meet results from Team Unify to SwimTopia.

### Quickstart

For help: 
```shell
python3 fetch_touchpad_live_meets.py -h
```

Get meets for team name like "Willowsford North":
```shell
python3 fetch_touchpad_live_meets.py --team "Willowsford North"
```

Results are stored in `<team_id>_meets.json`

Example console output:

```
〉python3 fetch_touchpad_live_meets.py -i 5721
Fetching all meets that occurred in the state of VA from 2012 to 2023
	Fetching meets for 2012
	Found 0 meets from 2012
	Fetching meets for 2013
	Found 109 meets from 2013
	Fetching meets for 2014
	Found 119 meets from 2014
	Fetching meets for 2015
	Found 187 meets from 2015
	Fetching meets for 2016
	Found 155 meets from 2016
	Fetching meets for 2017
	Found 165 meets from 2017
	Fetching meets for 2018
	Found 162 meets from 2018
	Fetching meets for 2019
	Found 180 meets from 2019
	Fetching meets for 2020
	Found 9 meets from 2020
	Fetching meets for 2021
	Found 167 meets from 2021
	Fetching meets for 2022
	Found 175 meets from 2022
	Fetching meets for 2023
	Found 192 meets from 2023
Found 1620 meets run in your state. Searching through them for your team with ID(s) ['5721']...
Finished searching through 0% of meets
Finished searching through 10% of meets
Finished searching through 20% of meets
Finished searching through 30% of meets
Finished searching through 40% of meets
Finished searching through 50% of meets
Finished searching through 60% of meets
Finished searching through 70% of meets
Finished searching through 80% of meets
Finished searching through 90% of meets
Finished searching through 100% of meets
Filtering out 1542 of 1620 meets.
	250 meets are empty.
	1292 meets do not contain your team(s) (['5721']).
Found 78 meets your team participated in:
[{'children': [],
  'city': 'South Riding',
  'id': 4283,
  'name': '2014 ODSL All-Star Meet',
  'startDate': '2014-08-02',
  'state': 'VA',
  'url': 'http://www.touchpadlive.com/4283'},
 {'children': [],
  'city': 'Leesburg',
  'id': 4281,
  'name': '2014 ODSL Divisional Meet',
  'startDate': '2014-07-26',
  'state': 'VA',
  'url': 'http://www.touchpadlive.com/4281'},
 {'children': [],
  'city': 'Leesburg',
  'id': 3543,
  'name': '2014 Willowsford at Tavistock',
  'startDate': '2014-07-16',
  'state': 'VA',
  'url': 'http://www.touchpadlive.com/3543'},
 {'children': [],
  'city': 'Leesburg',
  'id': 3581,
  'name': '2014 Woodlea Watermocs vs Willowsford Waves',
  'startDate': '2014-06-25',
  'state': 'VA',
  'url': 'http://www.touchpadlive.com/3581'},
 {'children': [],
  'city': 'Leesburg',
  'id': 3578,
  'name': 'Willowsford Farms at Evergreen Meadows',
  'startDate': '2014-06-18',
  'state': 'VA',
  'url': 'http://www.touchpadlive.com/3578'},
  ...
```