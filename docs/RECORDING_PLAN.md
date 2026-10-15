# Recording the demo

The submission needs a recorded demo, not a live one. This file covers how to record it.
What to say at each step is in [`DEMO_SCRIPT.md`](DEMO_SCRIPT.md), Part 3.

Target length: 12-15 minutes. Every command was rerun from a clean start on 2026-10-15;
the whole sequence takes about 10 minutes of machine time.

## 1. Setup (15 minutes before recording)

1. Close everything heavy: browsers with many tabs, IDEs, other Docker containers.
   Spark, Kafka, HDFS and ClickHouse together need most of 16 GB.
2. Turn on Do Not Disturb so notifications do not appear in the recording.
3. Stop the Mac from sleeping during the recording: `caffeinate -d &`
4. Terminal: one window, about 140x40 characters, font size 16-18, light or dark theme
   with good contrast. Clear the scrollback (Cmd+K).
5. Warm up, so nothing in the recording waits on downloads or cold starts:

   ```bash
   cd ~/Documents/labs/streaming-lakehouse-cdc-iceberg
   bash scripts/demo.sh 0
   ```

   Step 0 is a setup step. Run it before recording, not on camera.
6. Do a 30-second test recording with the microphone and play it back. Check the volume
   and that the terminal text is readable at 1080p.

## 2. Recording tool

- **macOS:** Cmd+Shift+5 -> "Record Selected Portion" (select the terminal window) ->
  Options -> Microphone: built-in or headset. Saves a `.mov` to the Desktop.
- **Alternatives:** OBS Studio (free, all platforms), or a Google Meet / Zoom call with
  screen share and cloud recording. A call works well if the three of you are in
  different places: each person shares the same terminal in turn and narrates.

## 3. Recording order

Record in **segments**, one per speaker. A mistake then costs one segment, not the whole
take. Stop the recording between segments and restart it for the next one. Segments
must be recorded in this order, because steps 2-6 use the live table from step 1.

| Segment | Speaker | Commands | Show on screen | Approx. length |
|---|---|---|---|---|
| A | Vivek | (no command) | architecture diagram from `README.md` section 1, opening lines | 1.5 min |
| B | Vivek | `demo.sh 1`, `demo.sh 2`, `demo.sh 3` | producer + ingest, live counts, one cancelled order | 3 min |
| C | Tejasri | `demo.sh 4`, `demo.sh 5`, `demo.sh 6` | snapshots, compaction + `e1_planning.png`, rollback | 4.5 min |
| D | Sunoj | `demo.sh 7`, `demo.sh 8` | kill -9 + verifier, three engines agree, `e3_trials.png` | 4 min |
| E | Vivek | `demo.sh 9` | E2 crossover table + figures, closing | 2 min |

After segment E: `bash scripts/demo.sh stop`.

Between segments B and C, do not wait long: the producer stops after 100,000 events
(about 30 minutes at 50 events per second).

Tips:

- Type nothing on camera except `bash scripts/demo.sh N`. Have the commands ready in a
  note to copy from.
- Some steps run quietly for 20-60 seconds (Spark startup). Keep talking through it, or
  cut the wait in editing.
- When a step opens a figure (steps 5 and 9), arrange the image window next to the
  terminal before you point at it.

## 4. If something goes wrong

| Problem | What to do |
|---|---|
| A step errors or hangs | Stop that segment's recording. Check section 9 of `README.md`. If steps 2-6 broke, restart from `demo.sh 0` and `demo.sh 1`, then re-record from segment B. Steps 7, 8 and 9 can each be rerun on their own. |
| Step 8 ClickHouse query hangs | Ctrl-C, rerun `bash scripts/demo.sh 8`. |
| Step 3 shows a different starting status | Expected. It picks any order that is not already CANCELLED. Read out the status it prints. |
| Numbers differ from the screenshots in `docs/screenshots/` | Expected. The data is random per run. Read out the numbers on screen, not the ones in the script. The E1/E2/E3 figures and step 9 come from the committed CSVs and do not change. |

## 5. Editing and export

1. Join segments A-E in order. QuickTime: open A, then Edit -> Add Clip to End for each
   of the others. iMovie, Clipchamp or Shotcut work too.
2. Trim long silent waits for Spark startup, if any. Do not cut the output that proves
   something: the `passed True` lines, the before/after tables, the three-engine result.
3. Export as MP4, 1080p. 15 minutes at 1080p is roughly 200-500 MB.
4. Name it like `2023BCS0175_0178_0166_streaming_lakehouse_demo.mp4` and watch it once
   from start to finish before submitting.

## 6. Final checklist

- [ ] All three names and roll numbers visible (every step prints the team banner)
- [ ] Every speaker audible
- [ ] Terminal text readable at full screen
- [ ] Steps 1-9 all shown, each with its output
- [ ] No notifications, passwords or unrelated windows in frame
- [ ] Video plays from start to finish
