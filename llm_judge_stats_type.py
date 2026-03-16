from openai import OpenAI
import json
import os
import re
import argparse
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

RUBRIC_PROMPT = """
You are an expert statistician and data analyst. You will be given one or more JSON files that contain analytic results from statistical analyses.

Here is the JSON file: 
$$JSON_FILE$$ 

Your task is to:
Identify the statistical test(s) used in each JSON file (e.g., "two-sample t-test", "linear regression", "Poisson regression", "survival analysis", "time series analysis", etc.).
If a JSON file contains more than one test, list all tests separately.
When multiple JSON files are provided, combine the results into a single summary that shows:
- Each statistical test type found
- The number of times it appears across all files
- An example JSON filename or identifier where it was found.
Instructions for interpreting the data:
- If the JSON includes model output (e.g., coefficients, p-values, hazard ratios), use that context to infer the statistical test.
- If the test type is not explicitly stated, make a best guess based on the structure of the results (e.g., “lm” in R output → linear regression; “coxph” → survival analysis).
- Output the final answer in two parts:
    - Per-file breakdown (file name → list of test types).
    - Combined summary table (test type → frequency → example file).

Example Input (2 JSON files):
 file1.json: contains a regression summary with coefficients and R².
 file2.json: contains a t-test output with t-statistic, degrees of freedom, and p-value.

 Example Output:
 Per-file breakdown:
file1.json → linear regression
file2.json → two-sample t-test

Combined summary:
Example 1 
Test TypeCountExample FileLinear Regression1file1.jsonTwo-Sample t-Test1file2.json
| Test Type         | Count | Example File |
| ----------------- | ----- | ------------ |
| Linear Regression | 1     | file1.json   |
| Two-Sample t-Test | 1     | file2.json   |

Example 2 
| Test Type                  | Count (files) | Example File          |
| -------------------------- | ------------- | --------------------  |
| Linear Regression          | 5             | json_file_name_1.json |
| Two-Sample t-Test          | 3             | json_file_name_2.json |
| Survival Analysis (Cox PH) | 1             | json_file_name_3.json |
| Unknown                    | 2             | json_file_name_4.json |
"""

def extract_first_json_block(text: str):
    """
    Return the first valid JSON object or array in `text`.
    Uses stdlib JSON decoder with forward scanning.
    """
    decoder = json.JSONDecoder()
    idx = 0
    text = text.lstrip()
    while idx < len(text):
        try:
            obj, end = decoder.raw_decode(text[idx:])
            return obj
        except json.JSONDecodeError:
            idx += 1
    raise ValueError("No valid JSON found in response")

def get_statistical_tests_from_json(json_content, filename):
    """Call an LLM-based agent to extract statistical test(s) from a JSON file."""
    prompt = RUBRIC_PROMPT.replace("$$JSON_FILE$$", json_content)
    client = OpenAI()
    response = client.responses.create(model="gpt-4.1", input=prompt, temperature=0.0)
    # Try to extract the per-file breakdown from the response
    return response.output_text.strip()

def process_directory(json_dir):
    """
    For each immediate subdirectory of json_dir, process all JSON files directly inside it.
    Skip any sub-subdirectories (including failed/timeout).
    Returns a dict: {subdir/filename: result}
    """
    results = {}
    batch_dirs = [entry for entry in os.scandir(json_dir) if entry.is_dir()]
    if not batch_dirs:
        print(f"⚠️ No batch subdirectories found in directory: {json_dir}")
    for batch_entry in batch_dirs:
        batch_name = batch_entry.name
        batch_path = batch_entry.path
        # Skip special subdirectories
        if batch_name in {"failed", "timeout", "no_matching_concept"}:
            continue
        json_found = False
        for fname in os.listdir(batch_path):
            fpath = os.path.join(batch_path, fname)
            if os.path.isfile(fpath) and fname.endswith('.json'):
                json_found = True
                print(f"Processing {fname} in {batch_path}...")
                with open(fpath, "r", encoding="utf-8") as f:
                    json_content = f.read()
                try:
                    result = get_statistical_tests_from_json(json_content, fname)
                    results[f"{batch_name}/{fname}"] = result
                except Exception as e:
                    print(f"❌ Error processing {fname}: {e}")
        if not json_found:
            print(f"⚠️ No JSON files found in batch directory: {batch_path}")
    return results

def summarize_results(per_file_results, output_file=None):
    """
    Print per-file breakdown and a combined summary table.
    If output_file is provided, also save the summary to that file.
    """
    lines = []
    lines.append("\nPer-file breakdown:")
    for fname, result in per_file_results.items():
        lines.append(f"{fname} → {result}")

    # Attempt to extract test types for summary
    test_type_counts = {}
    example_files = {}
    for fname, result in per_file_results.items():
        result_lines = result.splitlines()
        for line in result_lines:
            m = re.match(r".*→\s*(.+)", line)
            if m:
                test_types = [t.strip() for t in m.group(1).split(",")]
                for t in test_types:
                    if t:
                        test_type_counts[t] = test_type_counts.get(t, 0) + 1
                        if t not in example_files:
                            example_files[t] = fname

    lines.append("\nCombined summary:")
    lines.append("| Test Type | Count | Example File |")
    lines.append("| --------- | ----- | ------------ |")
    for t, count in test_type_counts.items():
        lines.append(f"| {t} | {count} | {example_files[t]} |")

    output_text = "\n".join(lines)
    print(output_text)
    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(output_text)
        print(f"\n✅ Summary saved to {output_file}")

def main(json_dir, output_file=None):
    per_file_results = process_directory(json_dir)
    summarize_results(per_file_results, output_file=output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract statistical test types from JSON files in subdirectories.")
    parser.add_argument("--json-dir", type=str, required=True, help="Directory containing subdirectories with JSON files.")
    parser.add_argument("--output-file", type=str, required=False, help="File to save the summary output.")
    args = parser.parse_args()
    main(args.json_dir, args.output_file)