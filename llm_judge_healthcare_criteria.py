### this script evaluates the performance of research findings using a predefined healthcare reporting checklist

from openai import OpenAI
import json
import os
import pandas as pd
import re
import argparse
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed



checklist = """   ## Healthcare Reporting Checklist 
Title and abstract
1a. Indicate the study's design with a commonly used term in the title or the abstract
1b. Provide in the abstract an informative and balanced summary of what was done and what was found

Introduction
2. Background/rationale: Explain the scientific background and rationale for the investigation being reported
3. Objectives: State specific objectives, including any prespecified hypotheses

Methods
4. Study design: Present key elements of study design early in the paper
5. Setting: Describe the setting, locations, and relevant dates, including periods of recruitment, exposure, follow-up, and data collection

Participants:
6a. Cohort study—Give the eligibility criteria, and the sources and methods of selection of participants. Describe methods of follow-up
Case-control study—Give the eligibility criteria, and the sources and methods of case ascertainment and control selection. Give the rationale for the choice of cases and controls
Cross-sectional study—Give the eligibility criteria, and the sources and methods of selection of participants
6b. Cohort study—For matched studies, give matching criteria and number of exposed and unexposed
Case-control study—For matched studies, give matching criteria and the number of controls per case
7. Variables: Clearly define all outcomes, exposures, predictors, potential confounders, and effect modifiers. Give diagnostic criteria, if applicable
8. Data sources/measurement: For each variable of interest, give sources of data and details of methods of assessment (measurement). Describe comparability of assessment methods if there is more than one group
9. Bias: Describe any efforts to address potential sources of bias
10. Study size: Explain how the study size was arrived at
11. Quantitative variables: Explain how quantitative variables were handled in the analyses. If applicable, describe which groupings were chosen and why

Statistical methods:
12a. Describe all statistical methods, including those used to control for confounding
12b. Describe any methods used to examine subgroups and interactions
12c. Explain how missing data were addressed
12d. Cohort study—If applicable, explain how loss to follow-up was addressed
Case-control study—If applicable, explain how matching of cases and controls was addressed
Cross-sectional study—If applicable, describe analytical methods taking account of sampling strategy
12e. Describe any sensitivity analyses

Results
Participants:
13a. Report numbers of individuals at each stage of study—e.g. numbers potentially eligible, examined for eligibility, confirmed eligible, included in the study, completing follow-up, and analysed
13b. Give reasons for non-participation at each stage
13c. Consider use of a flow diagram

Descriptive data:
14a. Give characteristics of study participants (e.g. demographic, clinical, social) and information on exposures and potential confounders
14b.Indicate number of participants with missing data for each variable of interest
14c. Cohort study—Summarise follow-up time (e.g., average and total amount)

15. Outcome data:
Cohort study—Report numbers of outcome events or summary measures over time
Case-control study—Report numbers in each exposure category, or summary measures of exposure
Cross-sectional study—Report numbers of outcome events or summary measures

Main results:
16a. Give unadjusted estimates and, if applicable, confounder-adjusted estimates and their precision (e.g., 95% confidence interval). Make clear which confounders were adjusted for and why they were included
16b. Report category boundaries when continuous variables were categorized
16c. If relevant, consider translating estimates of relative risk into absolute risk for a meaningful time period
17. Other analyses: Report other analyses done—e.g. analyses of subgroups and interactions, and sensitivity analyses

Discussion
18. Key results: Summarise key results with reference to study objectives
19. Limitations: Discuss limitations of the study, taking into account sources of potential bias or imprecision. Discuss both direction and magnitude of any potential bias
20. Interpretation: Give a cautious overall interpretation of results considering objectives, limitations, multiplicity of analyses, results from similar studies, and other relevant evidence
21. Generalisability: Discuss the generalisability (external validity) of the study results

Other information
22. Funding: Give the source of funding and the role of the funders for the present study and, if applicable, for the original study on which the present article is based

Give information separately for cases and controls in case-control studies and, if applicable, for exposed and unexposed groups in cohort and cross-sectional studies.
""".strip()

CHECKLIST_SUMMARY = {
    '1a': '1a Study design in title/abstract',
    '1b': '1b Informative abstract summary',
    '2': '2 Scientific background/rationale',
    '3': '3 Specific objectives/hypotheses',
    '4': '4 Study design elements',
    '5': '5 Setting/locations/dates',
    '6a': '6a Participant selection criteria',
    '6b': '6b Matching criteria (if applicable)',
    '7': '7 Variable definitions',
    '8': '8 Data sources/measurement',
    '9': '9 Bias mitigation efforts',
    '10': '10 Study size justification',
    '11': '11 Quantitative variable handling',
    '12a': '12a Statistical methods',
    '12b': '12b Subgroup/interaction methods',
    '12c': '12c Missing data handling',
    '12d': '12d Follow-up/matching methods',
    '12e': '12e Sensitivity analyses',
    '13a': '13a Participant flow numbers',
    '13b': '13b Non-participation reasons',
    '13c': '13c Flow diagram usage',
    '14a': '14a Participant characteristics',
    '14b': '14b Missing data reporting',
    '14c': '14c Follow-up time summary',
    '15': '15 Outcome data reporting',
    '16a': '16a Unadjusted/adjusted estimates',
    '16b': '16b Category boundaries',
    '16c': '16c Relative to absolute risk',
    '17': '17 Other analyses',
    '18': '18 Key results summary',
    '19': '19 Study limitations',
    '20': '20 Results interpretation',
    '21': '21 Generalizability',
    '22': '22 Funding information'
}

RUBRIC_PROMPT = """
You are an expert clinical researcher, and you are evaluating a research finding based on the following checklist.

This is the checklist you are grading the research finding against:
$$CHECKLIST$$

Look at the research finding and for each item in the checklist, assign either +1 or 0. Output your grading in a JSON format.

The JSON format would look like:
```json
[
  {
    "item": "1a",
    "score": 0 | 1,
    "justification": "string"
  }, ...
]
```
- "item" refers to the item in the checklist.
- "score" is either 0 or 1, where 1 means the item is satisfied and 0 means it is not.
- "justification" is a string that explains why you assigned that score.

The research finding you are evaluating is:
$$RESEARCH_FINDING$$

Please output your final grading:
""".replace(
    "$$CHECKLIST$$", checklist
).strip()


def grade_research_finding(research_finding, directory, job_id, uuid, question, batch):
    """Grade a research finding and return the result."""
    try:
        # Don't grade files from special directories
        if any(x in directory for x in ['/failed', '/timeout', '/no_matching_concept']):
            print(f"⏩ Skipping grading for file in special directory: {directory}")
            return None
            
        # Ensure checklist is a string
        if isinstance(checklist, list):
            checklist_str = "\n".join(checklist)
        else:
            checklist_str = str(checklist)

        # Replace placeholders in the prompt
        prompt = RUBRIC_PROMPT.replace("$$CHECKLIST$$", checklist_str).replace(
            "$$RESEARCH_FINDING$$", research_finding
        )

        # Call the LLM API (replace with actual API call if needed)
        client = OpenAI()
        response = client.responses.create(model="gpt-4.1", input=prompt, temperature=0.5)
        try:
            grading = extract_first_json_block(response.output_text.replace("```json", "").replace("```", "").strip())
        except Exception as e:
            print(f"❌ Error parsing grading JSON for job_id {job_id}: {e}. Skipping this file.")
            return None  # Skip grading for this file

        # Map scores using item numbers directly as keys
        scores_dict = {item["item"]: item["score"] for item in grading}

        # Clean up directory path: keep only the relevant part after the batch name
        path_parts = directory.split("/")
        try:
            reports_index = path_parts.index("reports")
            batch_name = path_parts[reports_index + 1]
        except ValueError:
            # If "reports" not in path, use the batch parameter passed to the function
            batch_name = batch
        
        # Return the result
        return {
            "job_id": job_id,
            "uuid": uuid,
            "question": question,
            "batchID": batch,  # Using batchID consistently
            **scores_dict,
        }
    except Exception as e:
        print(f"❌ Error grading research finding: {e}")
        return None


def extract_first_json_block(text: str) -> str:
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


REQUIRED_ITEMS_ORDERED = list(CHECKLIST_SUMMARY.keys())

def sort_key(item):
    match = re.match(r"(\d+)([a-z]?)", item)
    return (int(match.group(1)), match.group(2))

def is_bottom_directory(dir_path):
    """Check if a directory is a bottom-level directory (contains JSON files, not subdirectories with JSON files)"""
    if not os.path.exists(dir_path):
        return False
    
    # Skip the special subdirectories - they should not be processed as separate batches
    if os.path.basename(dir_path) in ["failed", "timeout", "no_matching_concept"]:
        return False
        
    has_json_files = any(f.endswith('.json') for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f)))
    
    # Check if any subdirectories contain JSON files (excluding special directories)
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isdir(item_path) and item not in ["failed", "timeout", "no_matching_concept"]:
            if any(f.endswith('.json') for f in os.listdir(item_path) if os.path.isfile(os.path.join(item_path, f))):
                return False  # This is not a bottom directory
    
    return has_json_files

def process_single_directory(json_dir, output_dir, base_dir):
    """Process a single directory and return records data"""
    print(f"Processing directory: {json_dir}")
    print(f"Output directory: {output_dir}")
    global args
    if "/reports/" in json_dir:
        # Normalise json_dir to a generic local root for evaluation outputs
        args.json_dir = os.getcwd()
        path_parts = json_dir.split("/")
        try:
            reports_index = path_parts.index("reports")
            timestamp_dir = None
            for part in path_parts[reports_index+1:]:
                if re.match(r"\d{8}_\d{6}", part):
                    timestamp_dir = part
                    break
            if timestamp_dir:
                output_dir = os.path.join(os.getcwd(), "llm_judge_eval_summary", timestamp_dir)
        except ValueError:
            # If "reports" not in path, continue with the provided output_dir
            pass

    records = []
    json_files = [f for f in os.listdir(json_dir) if f.endswith(".json")]
    total_files = len(json_files)
    graded_count = 0
    existing_count = 0
    skipped_count = 0
    excluded_count = 0
    print(f"Found {total_files} JSON files to process")

    # Use ThreadPoolExecutor for concurrent grading
    def process_file(fname):
        # Use absolute path for input file
        fpath = os.path.abspath(os.path.join(json_dir, fname))
        if not os.path.exists(fpath):
            print(f"⚠️ Warning: File not found: {fpath}")
            return fname, None, None, None, None, None

        # Extract metadata
        uuid, question, job_id, batch, date = extract_metadata(json_dir, fname)
        
        # Check if already graded - look for existing _graded.json file
        output_fname = os.path.splitext(fname)[0] + "_graded.json"
        output_fpath = os.path.join(output_dir, output_fname)
        
        if os.path.exists(output_fpath):
            print(f"📝 Loading existing grading for {fname}")
            try:
                with open(output_fpath, "r", encoding="utf-8") as f:
                    existing_graded_data = json.load(f)
                    # Return the existing grading data
                    return fname, existing_graded_data, uuid, question, job_id, batch, date
            except Exception as e:
                print(f"⚠️ Error loading existing grading for {fname}: {e}. Re-grading...")
                # Continue to re-grade if we can't load existing data

        # Skip grading for files in failed, timeout, or no_matching_concept directories
        special_dirs = {"failed", "timeout", "no_matching_concept"}
        if any(sd in os.path.normpath(fpath).split(os.sep) for sd in special_dirs):
            print(f"⏩ Skipping grading for file in special directory: {fpath}")
            return fname, None, uuid, question, job_id, batch, date

        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
            research_finding = str(data.get("report", ""))
        grading = grade_research_finding(research_finding, os.path.dirname(fpath), job_id, uuid, question, batch)
        
        # If grading succeeded, save the graded JSON file
        if grading is not None:
            # Save only the grading results, no metadata
            graded_data = grading
            
            # Save to output directory with _graded suffix
            output_fname = os.path.splitext(fname)[0] + "_graded.json"
            output_fpath = os.path.join(output_dir, output_fname)
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Write the graded JSON file
            with open(output_fpath, "w", encoding="utf-8") as out_f:
                json.dump(graded_data, out_f, indent=2, ensure_ascii=False)
            print(f"✅ Saved graded JSON: {output_fpath}")
        
        # If grading is None (failed), do not generate a JSON output file
        return fname, grading, uuid, question, job_id, batch, date

    import random
    futures = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for fname in json_files:
            time.sleep(random.randint(5, 10))  # Add a 5-10 second lag between job submissions
            futures.append(executor.submit(process_file, fname))

        for future in as_completed(futures):
            try:
                fname, grading, uuid, question, job_id, batch, date = future.result()
                if grading:
                    # Only generate record if grading succeeded
                    record = {
                        "batchID": batch,  # Using batchID consistently
                        "date": date,  # Add the date from timestamp
                        "job_id": job_id,
                        "uuid": uuid,
                        "question": question,
                    }
                    # Check if this was existing grading (loaded from file) or new grading (from API)
                    # Old format had nested structure, new format is direct grading data
                    if isinstance(grading, dict) and 'grading' in grading:
                        # Old format with nested grading structure
                        scores = grading['grading']
                        existing_count += 1
                    elif isinstance(grading, dict) and any(key in grading for key in CHECKLIST_SUMMARY.keys()):
                        # New format with direct grading data (existing file)
                        scores = grading
                        existing_count += 1
                    else:
                        # New grading from API
                        scores = grading
                        graded_count += 1
                    for item_key in CHECKLIST_SUMMARY.keys():
                        record[item_key] = scores.get(item_key, None)
                    records.append(record)
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"❌ Error processing file {fname}: {e}")
                skipped_count += 1

    # Print final summary for this directory
    print(f"\n📊 Directory Summary:")
    print(f"   Total files found: {total_files + excluded_count}")
    print(f"   Files excluded (in special directories): {excluded_count}")
    print(f"   Files eligible for grading: {total_files}")
    print(f"   Files newly graded: {graded_count}")
    print(f"   Files with existing grading: {existing_count}")
    print(f"   Files skipped (failed grading): {skipped_count}")
    print(f"   Total records processed: {len(records)}")

    return records

def extract_metadata(json_dir, fname):
    """Extracts UUID, question, status, step times, step values, image, job_id, batch and date from a JSON file."""
    fpath = os.path.join(json_dir, fname)
    
    # Extract date from directory path
    path_parts = os.path.normpath(json_dir).split(os.sep)
    date = None
    for part in path_parts:
        if re.match(r"\d{8}_\d{6}", part):  # Match timestamp format
            date = part[:8]  # Extract YYYYMMDD part
            break
            
    with open(fpath, "r") as f:
        data = json.load(f)
        uuid = data.get("uuid", "")
        # Handle question which could be either string or list
        full_question = data.get("question", "")
        if isinstance(full_question, list):
            # If it's a list, take the second element if available
            question = full_question[1] if len(full_question) > 1 else full_question[0] if full_question else ""
        else:
            # If it's a string, split by newline and take second element if available
            question = full_question.split("\n")[1] if full_question and len(full_question.split("\n")) > 1 else full_question
        job_id = os.path.splitext(fname)[0]
        batch = os.path.basename(os.path.dirname(fpath))
    return uuid, question, job_id, batch, date

def merge_with_check_batch(llm_df, check_batch_excel, output_file):
    """Merge LLM evaluation output with check_batch_success_rate output."""
    # Load the "All Batches Combined" sheet from the check_batch_success_rate Excel file
    check_batch_df = pd.read_excel(check_batch_excel, sheet_name="All_Batches_Combined")

    print(f"\nDEBUG: LLM DataFrame columns: {llm_df.columns.tolist()}")
    print(f"DEBUG: Check Batch DataFrame columns: {check_batch_df.columns.tolist()}")
    # Print first few rows to inspect job_id columns
    print("LLM DataFrame head:")
    print(llm_df.head())
    print("Check Batch DataFrame head:")
    print(check_batch_df.head())

    # Merge on job_id only, outer join
    merged_df = pd.merge(
        llm_df,
        check_batch_df,
        on=["job_id"],
        how="outer",
        indicator=True
    )

    print(f"\nDEBUG: Merged DataFrame shape: {merged_df.shape}")
    print(f"DEBUG: Merged DataFrame columns: {merged_df.columns.tolist()}")
    print(f"DEBUG: Merge indicator counts:\n{merged_df['_merge'].value_counts()}")
    print("DEBUG: Showing first 10 rows of the merge indicator column:")
    print(merged_df[["job_id", "_merge"]].head(10))


    # For failed cases (where LLM data is missing), adopt question, uuid, etc. from check_batch columns
    # Fill question_x, uuid_x, batchID, date, etc. from their _y counterparts if _x is missing
    fill_columns = [
        ("uuid_x", "uuid_y"),
        ("question_x", "question_y"),
        ("batchID", "batch"),
        ("date", "date"),
    ]
    for col_x, col_y in fill_columns:
        if col_x in merged_df.columns and col_y in merged_df.columns:
            merged_df[col_x] = merged_df[col_x].combine_first(merged_df[col_y])

    # For failed/timeout/no matching concept cases (right_only), set batchID to directory value and fill in date from directory string if missing
    if "batchID" in merged_df.columns and "directory" in merged_df.columns and "_merge" in merged_df.columns:
        mask_special = merged_df["_merge"] == "right_only"
        merged_df.loc[mask_special, "batchID"] = merged_df.loc[mask_special, "directory"]
        # Fill in date for failed/timeout/no matching concept cases
        if "date" in merged_df.columns and "directory" in merged_df.columns:
            def extract_date_from_directory(directory):
                if pd.isna(directory):
                    return None
                m = re.match(r"(\d{8})_", str(directory))
                if m:
                    return m.group(1)
                return None
            missing_date_mask = mask_special & (merged_df["date"].isna() | (merged_df["date"].astype(str).str.strip() == ""))
            merged_df.loc[missing_date_mask, "date"] = merged_df.loc[missing_date_mask, "directory"].apply(extract_date_from_directory)

    # For right_only (check_batch only), adopt error column from check_batch
    if "error_y" in merged_df.columns and "_merge" in merged_df.columns:
        mask_right_only = merged_df["_merge"] == "right_only"
        merged_df.loc[mask_right_only, "error"] = merged_df.loc[mask_right_only, "error_y"]
    # For both, prefer error_x if present, else error_y
    if "error_y" in merged_df.columns and "error_x" in merged_df.columns:
        merged_df["error"] = merged_df["error_y"].combine_first(merged_df["error_x"])
    # Drop error_x and error_y columns to keep only 'error'
    for col in ["error_x", "error_y"]:
        if col in merged_df.columns:
            merged_df = merged_df.drop(columns=[col])

    # User-specified column mapping (EXACT column names with spaces, no extra underscores)
    column_mapping = {
        'date': 'date',
        'batch': 'batch',
        'batchID': 'batch_ID',
        'error': 'error',
        'job_id': 'job_ID_run',
        'job_ID_eval': 'job_ID_eval',
        'uuid_x': 'question_ID',
        'question_x': 'question',
        'status': 'status',
        'step_1_seconds': 'step_1_seconds',
        'step_2_seconds': 'step_2_seconds',
        'step_3_seconds': 'step_3_seconds',
        'step_4_seconds': 'step_4_seconds',
        'job_total': 'total_time',
        '1a': 'equator_1a_study design in title/abstract',
        '1b': 'equator_1b_informative abstract summary',
        '2': 'equator_2_scientific background/rationale',
        '3': 'equator_3_specific objectives/hypotheses',
        '4': 'equator_4_study design elements',
        '5': 'equator_5_setting/locations/dates',
        '6a': 'equator_6a_participant selection criteria',
        '6b': 'equator_6b_matching criteria (if applicable)',
        '7': 'equator_7_variable definitions',
        '8': 'equator_8_data sources/measurement',
        '9': 'equator_9_bias mitigation efforts',
        '10': 'equator_10_study size justification',
        '11': 'equator_11_quantitative variable handling',
        '12a': 'equator_12a_statistical methods',
        '12b': 'equator_12b_subgroup/interaction methods',
        '12c': 'equator_12c_missing data handling',
        '12d': 'equator_12d_follow-up/matching methods',
        '12e': 'equator_12e_sensitivity analyses',
        '13a': 'equator_13a_participant flow numbers',
        '13b': 'equator_13b_non-participation reasons',
        '13c': 'equator_13c_flow diagram usage',
        '14a': 'equator_14a_participant characteristics',
        '14b': 'equator_14b_missing data reporting',
        '14c': 'equator_14c_follow-up time summary',
        '15': 'equator_15_outcome data reporting',
        '16a': 'equator_16a_unadjusted/adjusted estimates',
        '16b': 'equator_16b_category boundaries',
        '16c': 'equator_16c_relative to absolute risk',
        '17': 'equator_17_other analyses',
        '18': 'equator_18_key results summary',
        '19': 'equator_19_study limitations',
        '20': 'equator_20_results interpretation',
        '21': 'equator_21_generalizability',
        '22': 'equator_22_funding information',
        'step_1': 'step_1',
        'step_2': 'step_2',
        'step_3': 'step_3',
        'step_4': 'step_4',
        'step_5': 'step_5',
        'img_base64_1': 'img_base64'
    }

    # Only keep columns that are mapped and in the exact_expected_columns list
    merged_df = merged_df.rename(columns=column_mapping)
    # Remove any columns not in the mapping or not in the exact_expected_columns
    keep_cols = set(column_mapping.values())
    merged_df = merged_df[[col for col in merged_df.columns if col in keep_cols]]
    # Columns to drop
    drop_cols = ['directory', 'uuid_y', 'question_y', '_merge']
    merged_df = merged_df.rename(columns=column_mapping)
    for col in drop_cols:
        if col in merged_df.columns:
            merged_df = merged_df.drop(columns=[col])

    # Reorder columns to match the exact order provided by the user
    exact_expected_columns = [
        'date', 'batch', 'batch_ID', 'job_ID_run', 'job_ID_eval', 'question_ID', 'question', 'status',
        'step_1_seconds', 'step_2_seconds', 'step_3_seconds', 'step_4_seconds', 'total_time',
        'equator_1a_study design in title/abstract', 'equator_1b_informative abstract summary',
        'equator_2_scientific background/rationale', 'equator_3_specific objectives/hypotheses',
        'equator_4_study design elements', 'equator_5_setting/locations/dates',
        'equator_6a_participant selection criteria', 'equator_6b_matching criteria (if applicable)',
        'equator_7_variable definitions', 'equator_8_data sources/measurement',
        'equator_9_bias mitigation efforts', 'equator_10_study size justification',
        'equator_11_quantitative variable handling', 'equator_12a_statistical methods',
        'equator_12b_subgroup/interaction methods', 'equator_12c_missing data handling',
        'equator_12d_follow-up/matching methods', 'equator_12e_sensitivity analyses',
        'equator_13a_participant flow numbers', 'equator_13b_non-participation reasons',
        'equator_13c_flow diagram usage', 'equator_14a_participant characteristics',
        'equator_14b_missing data reporting', 'equator_14c_follow-up time summary',
        'equator_15_outcome data reporting', 'equator_16a_unadjusted/adjusted estimates',
        'equator_16b_category boundaries', 'equator_16c_relative to absolute risk',
        'equator_17_other analyses', 'equator_18_key results summary',
        'equator_19_study limitations', 'equator_20_results interpretation',
        'equator_21_generalizability', 'equator_22_funding information',
        'step_1', 'step_2', 'step_3', 'step_4', 'step_5', 'img_base64'
    ]
    # Only keep columns that exist in the DataFrame, in the specified order
    final_columns = [col for col in exact_expected_columns if col in merged_df.columns]
    merged_df = merged_df.reindex(columns=final_columns + [col for col in merged_df.columns if col not in final_columns])

    # Print merged column names before saving
    print("Merged DataFrame columns before saving to Excel:")
    print(list(merged_df.columns))

    # Split into HVq and PUq based on question_ID
    hv_mask = merged_df['question_ID'].astype(str).str.contains('HV', na=False)
    pu_mask = merged_df['question_ID'].astype(str).str.contains('PU', na=False)
    hv_df = merged_df[hv_mask].sort_values('question_ID')
    pu_df = merged_df[pu_mask].sort_values('question_ID')

    # Build output filenames: save in batch directory with batch name as prefix
    file_dir = os.path.dirname(output_file)
    batch_name = os.path.basename(file_dir)
    
    # Extract base filename without extension for the final prefix
    base_filename = os.path.basename(output_file)
    if base_filename.lower().endswith('.xlsx'):
        base_prefix = base_filename[:-5]
    else:
        base_prefix = base_filename
    
    # Use batch name as the prefix for output files, save in the batch directory
    hv_out = os.path.join(file_dir, f"{batch_name}_HV.xlsx")
    pu_out = os.path.join(file_dir, f"{batch_name}_PU.xlsx")

    # Save each DataFrame to its own Excel file
    with pd.ExcelWriter(hv_out, engine='openpyxl') as writer:
        hv_df.to_excel(writer, index=False, sheet_name='All Batches Combined')
    print(f"✅ Merged Excel file saved: {hv_out} (sheet: All Batches Combined)")

    with pd.ExcelWriter(pu_out, engine='openpyxl') as writer:
        pu_df.to_excel(writer, index=False, sheet_name='All Batches Combined')
    print(f"✅ Merged Excel file saved: {pu_out} (sheet: All Batches Combined)")

    # Also save as CSV files with the same naming convention
    hv_csv = os.path.join(file_dir, f"{batch_name}_HV.csv")
    pu_csv = os.path.join(file_dir, f"{batch_name}_PU.csv")
    
    hv_df.to_csv(hv_csv, index=False)
    print(f"✅ Merged CSV file saved: {hv_csv}")
    
    pu_df.to_csv(pu_csv, index=False)
    print(f"✅ Merged CSV file saved: {pu_csv}")

def main(json_dir, check_batch_excel, output_file):
    """Main function to process directories, merge results, and generate Excel output."""
    # Create base output directory in run_tests
output_base_dir = os.path.join(os.getcwd(), "llm_judge_eval_summary")
    os.makedirs(output_base_dir, exist_ok=True)

    # Extract only the batch ID from the path
    path_parts = json_dir.split("/")
    
    # Try to find reports index, if not found use the last directory name
    try:
        reports_index = path_parts.index("reports")
        main_batch_name = path_parts[reports_index + 1]
    except ValueError:
        # If "reports" not in path, use the last directory name as batch name
        main_batch_name = os.path.basename(json_dir.rstrip('/'))
        print(f"ℹ️  'reports' not found in path, using directory basename: {main_batch_name}")

    all_records = []
    special_dirs = {"failed", "timeout", "no_matching_concept"}
    flat_output_dir = os.path.join(output_base_dir, main_batch_name)
    os.makedirs(flat_output_dir, exist_ok=True)
    
    # Process each subdirectory that contains JSON files
    processed_dirs = set()
    for root, dirs, files in os.walk(json_dir):
        # Skip grading files in special subdirectories
        if os.path.basename(root) in special_dirs:
            continue
        
        # Only process directories that have JSON files and haven't been processed yet
        json_files = [f for f in files if f.endswith('.json')]
        if json_files and root not in processed_dirs:
            print(f"\n🔍 Processing directory: {root}")
            
            # Process first to get records, then create output directory only if needed
            records = process_single_directory(root, flat_output_dir, json_dir)
            
            # Only create subdirectory and save files if we have records
            if records:
                # Create output subdir matching the input structure only when needed
                rel_path = os.path.relpath(root, json_dir)
                if rel_path == '.':
                    output_subdir = flat_output_dir
                else:
                    output_subdir = os.path.join(flat_output_dir, rel_path)
                os.makedirs(output_subdir, exist_ok=True)
                print(f"� Created output directory: {output_subdir}")
                
                all_records.extend(records)
            else:
                print(f"⚠️ No records generated for directory: {root}")
                
            processed_dirs.add(root)

    if all_records:
        llm_df = pd.DataFrame(all_records)
        print("\nSkipping column name standardization. Columns in LLM DataFrame:")
        print(sorted(llm_df.columns.tolist()))
        # Determine output Excel file name
        # Always save the Excel file inside the batch directory
        if output_file:
            if output_file.lower().endswith('.xlsx'):
                excel_file = os.path.join(flat_output_dir, os.path.basename(output_file))
            else:
                excel_file = os.path.join(flat_output_dir, os.path.basename(output_file) + ".xlsx")
        else:
            excel_file = os.path.join(flat_output_dir, f"{main_batch_name}_summary.xlsx")
        merge_with_check_batch(llm_df, check_batch_excel, excel_file)
    else:
        print("⚠️ No records to process.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate LLM outputs, merge with check_batch_success_rate, and generate Excel.")
    parser.add_argument("--json-dir", type=str, required=True, help="Directory containing JSON files.")
    parser.add_argument("--check-batch-excel", type=str, required=True, help="Path to check_batch_success_rate Excel file.")
    parser.add_argument("--output-path", type=str, required=True, help="Path to save the merged Excel file.")
    args = parser.parse_args()

    main(args.json_dir, args.check_batch_excel, args.output_path)