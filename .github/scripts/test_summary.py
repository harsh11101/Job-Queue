"""Summarize Gradle's JUnit XML results for a GitHub Actions run.

Writes a Markdown table to stdout (CI appends it to $GITHUB_STEP_SUMMARY, shown on the
run's Summary page) and a one-line result to stderr, which appears in the step log.
"""
import glob
import sys
import xml.etree.ElementTree as ET

rows, failures = [], []
totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
for path in sorted(glob.glob("build/test-results/test/*.xml")):
    suite = ET.parse(path).getroot()
    name = suite.get("name").rsplit(".", 1)[-1]
    counts = {key: int(suite.get(key, 0)) for key in totals}
    for key in totals:
        totals[key] += counts[key]
    rows.append((name, counts, float(suite.get("time", 0))))
    for case in suite.iter("testcase"):
        problem = case.find("failure")
        if problem is None:
            problem = case.find("error")
        if problem is not None:
            text = (problem.get("message") or "").strip()
            failures.append((name, case.get("name"), text.splitlines()[0][:200] if text else ""))

print("## Test results\n")
if not rows:
    headline = "No test results were found: the tests did not run."
    print(headline)
else:
    failed = totals["failures"] + totals["errors"]
    headline = f"{totals['tests']} tests, {'all passed' if failed == 0 else f'{failed} failed'}, {totals['skipped']} skipped"
    print(f"**{headline}**\n")
    print("| Suite | Tests | Failed | Skipped | Time (s) |")
    print("| --- | ---: | ---: | ---: | ---: |")
    for name, counts, seconds in rows:
        print(f"| {name} | {counts['tests']} | {counts['failures'] + counts['errors']} | {counts['skipped']} | {seconds:.1f} |")
    if failures:
        print("\n### Failures\n")
        for suite_name, test, message in failures:
            print(f"- `{suite_name}.{test}`: {message}")

print(f"Test results: {headline} (details on the run's Summary page)", file=sys.stderr)
