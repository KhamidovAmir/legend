#!/usr/bin/env python3
"""Prints a Markdown table of line coverage per module from JaCoCo CSV reports (used in CI job summaries)."""
import csv
import glob

print("| module | line coverage |\n|---|---|")
total_missed = total_covered = 0
for path in sorted(glob.glob("**/target/site/jacoco/jacoco.csv", recursive=True)):
    missed = covered = 0
    for row in csv.DictReader(open(path)):
        missed += int(row["LINE_MISSED"])
        covered += int(row["LINE_COVERED"])
    total_missed += missed
    total_covered += covered
    module = path.split("/target/")[0].split("/")[-1]
    print(f"| {module} | {covered / (missed + covered) * 100:.1f}% |")
if total_missed + total_covered:
    # the regex of GitLab's "coverage:" keyword picks this line up
    print(f"\nTOTAL line coverage: {total_covered / (total_missed + total_covered) * 100:.1f}%")
