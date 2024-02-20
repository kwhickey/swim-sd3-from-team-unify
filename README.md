swim-sd3-from-team-unify
====

Scripts and utilities to parse Team Unify and TouchPad meet results and convert to .sd3 format for import elsewhere

# `sd3_from_tu_meet_results.py`

_Script to convert, read, or write Swimming Data Interchange Files (SDIF, .sd3) from a 
Team Unify and/or TouchPad source._

This script can be used to import swim meet results from Team Unify to SwimTopia.

## Quickstart

1. Log into your Team Unify site
2. In your profile, go to "Back Office"
3. Left-nav menu, choose Events & Competitions > Meet Results
4. Open your meet
5. Change the "Stroke" filter to one stroke, and "Search"
6. Export to Excel (XLS)
7. Repeat Search and Export for each stroke type, and save the file with the stroke name in lowercase at the end, e.g. `meet_free.xls`, `meet_back.xls`, ..., `meet_im.xls`. Save them alongside the `sd3_from_tu_meet_results.py` file
    - _The reason for this is that Team Unify only lets you download 400 results at a time, so you must filter to get less than that, so results aren't truncated._
    - ***Make sure each file has less than 400 results in it***
8. Go on to the 
8. Open a shell in this directory
9. `pip install sdif pandas openpyxl`
10. `python3 sd3_from_tu_meet_results.py concat meet`, changing `meet` to whatever the base name of your saved XLS files is before the `_stroke.xls`
11. `python3 sd3_from_tu_meet_results.py concat meet`
12. `python3 sd3_from_tu_meet_results.py build meet_concat.xls meet_relay.xls`

Take the generated `.sd3` file and import it into a SwimTopia meet.

_Tips_:

- Best to have imported the Roster to SwimTopia, in the Season of this meet, before importing the meet
- There's a bug in SwimTopia import such that auto-creating Events on import doesn't work. Create the standard 64 events manually, use a SwimTopia team or league template, or find a `.ev3` for uploading (like the one in this repo).
