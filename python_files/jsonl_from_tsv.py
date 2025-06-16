import json
import tqdm
from collections import Counter
import pandas as pd
import numpy as np
import re  # Import the regular expression module


def convert_tsv_to_jsonl(input_paths, output_path, filter_na=True):
    """
    Converts TSV files containing comment data and annotations into a JSONL file.
    It processes comments by thread (sdid), assigns unique author numbers per thread,
    performs majority voting on 'constructiveclass' labels, and outputs structured JSONL.

    Args:
        input_paths (list): A list of paths to the input TSV files.
        output_path (str): The path to the output JSONL file.
    """
    label_mapping = {"Constructive": 1, "Not constructive": 0}

    # Stores annotations for each thread (sdid) for majority voting
    all_thread_annotations = {}
    # Stores comment data (commentindex, text, author) for each thread (sdid)
    thread_comment_data = {}

    # Counters for different majority vote outcomes (for summary statistics)
    skipped_all_na_count = 0
    skipped_tie_count = 0
    tie_na_constructive_count = 0
    na_top_other_exists_resolved_count = 0
    valid_majority_resolved_count = 0
    na_exists_count = 0
    max_author_number_total = -1  # Track the maximum author number across all threads

    # Track all unique raw labels encountered during file reading for debugging
    all_raw_labels_encountered = set()

    for input_path in input_paths:
        print(f"Reading {input_path} and normalizing 'NaN' and 'NA' to 'NA' string...")

        # Read the TSV file.
        # `keep_default_na=False` prevents pandas from interpreting 'NA' or 'NaN' (string) as np.nan.
        # `na_values=[]` ensures no specific values are converted to NaN from a list.
        # Any actual empty cells will still be read as np.nan.
        df_temp = pd.read_csv(
            input_path,
            sep="\t",
            keep_default_na=False,
            na_values=[],
        )

        # Store all unique raw labels encountered
        all_raw_labels_encountered.update(df_temp["constructiveclass"].unique())

        # Normalize ALL forms of "no label" (np.nan, string 'NaN') to the string 'NA'.
        # This ensures consistent handling of missing annotations.
        df_temp["constructiveclass"] = df_temp["constructiveclass"].replace(
            {np.nan: "NA", "NaN": "NA"}
        )

        # Iterate over each row in the DataFrame to extract and store data
        for line_num, row in enumerate(df_temp.itertuples(index=False)):
            if line_num % 1000 == 0:
                print(f"Processing line {line_num} from {input_path}...")

            try:
                sdid = row.sdid
                commentindex = int(row.commentindex)
                raw_text = row.text
                author = row.guid
                constructiveclass = (
                    row.constructiveclass
                )  # Already normalized to 'NA', 'Constructive', or 'Not constructive'

                # --- Apply text normalization immediately upon reading ---
                # 1. Replace escaped double quotes \" with "
                normalized_text = re.sub(r'\\"|"', "", raw_text)
                # 2. Replace multiple spaces/tabs/newlines with a single space and strip leading/trailing whitespace
                normalized_text = re.sub(r"\s+", " ", normalized_text).strip()
                # --- End of text normalization ---
                # Store annotations for majority voting
                if sdid not in all_thread_annotations:
                    all_thread_annotations[sdid] = []
                all_thread_annotations[sdid].append(constructiveclass)

                # Store comment details for reconstructing the thread text later
                if sdid not in thread_comment_data:
                    thread_comment_data[sdid] = []
                thread_comment_data[sdid].append(
                    {
                        "commentindex": commentindex,
                        "text": normalized_text,
                        "author": author,
                    }
                )

            except AttributeError as e:
                print(
                    f"Error accessing attribute in row {line_num} from {input_path}: {row}. "
                    f"Ensure 'sdid', 'commentindex', 'text', 'author', and 'constructiveclass' columns exist. Error: {e}"
                )
                continue
            except ValueError as e:
                print(
                    f"Error converting value in row {line_num} from {input_path}: {row}. Error: {e}"
                )
                continue
            except IndexError as e:
                print(
                    f"Error parsing line {line_num} in {input_path}: {row}. Error: {e}"
                )
                continue

    print("\n--- Raw labels encountered before normalization (for debugging) ---")
    print(all_raw_labels_encountered)
    print(
        "\nFinished reading TSV files. Now performing majority vote and writing to JSONL..."
    )

    with open(output_path, "w", encoding="utf-8") as outfile:
        for sdid, annotations in tqdm.tqdm(
            all_thread_annotations.items(), desc="Processing threads and writing JSONL"
        ):
            annotation_counts = Counter(annotations)

            # Skip threads if 'NA' annotations are present.
            # This is a strict rule to ensure only clearly labeled threads are processed.
            if "NA" in annotation_counts:
                na_exists_count += 1
                if filter_na:
                    continue

            # Filter out 'NA' to find actual labels ('Constructive', 'Not constructive')
            non_na_annotations_dict = {
                k: v for k, v in annotation_counts.items() if k != "NA"
            }

            final_constructive_class = None
            thread_label = None  # Initialize thread_label

            # If no non-NA annotations exist (meaning all were 'NA', though we already filtered this case)
            if not non_na_annotations_dict:
                skipped_all_na_count += 1
                continue

            # Get the most common non-NA label(s) for tie-breaking logic
            most_common_non_na = Counter(non_na_annotations_dict).most_common(2)

            # Get the overall most common label(s) including 'NA' (though 'NA' should be filtered out by now)
            overall_most_common = annotation_counts.most_common(2)

            # Check for specific ties involving 'NA' (should be mostly handled by the initial 'NA' check)
            is_tie_between_na_and_constructive = False
            if len(overall_most_common) == 2:
                top_label_1, top_count_1 = overall_most_common[0]
                top_label_2, top_count_2 = overall_most_common[1]

                if top_count_1 == top_count_2:
                    if ("NA" in [top_label_1, top_label_2]) and (
                        ("Constructive" in [top_label_1, top_label_2])
                        or ("Not constructive" in [top_label_1, top_label_2])
                    ):
                        is_tie_between_na_and_constructive = True

            if is_tie_between_na_and_constructive:
                # This case should ideally be caught by the initial `if "NA" in annotation_counts`
                tie_na_constructive_count += 1
                continue
            elif (
                len(most_common_non_na) > 1
                and most_common_non_na[0][1] == most_common_non_na[1][1]
            ):
                # Case: Tie among non-'NA' labels (e.g., equal votes for 'Constructive' and 'Not constructive')
                skipped_tie_count += 1
                continue

            # At this point, there's at least one non-NA annotation, and no ties.
            candidate_label = most_common_non_na[0][
                0
            ]  # The clear most common non-NA label

            # Now, check the overall most common taking 'NA' into account (again, 'NA' should be absent)
            overall_most_common_label = overall_most_common[0][0]

            if overall_most_common_label == "NA":
                # This path should ideally not be taken if the initial 'NA' check is strict.
                # If it is, it means 'NA' was the most common overall, but a non-'NA' label (candidate_label) exists.
                # We prioritize the non-'NA' label in this scenario.
                final_constructive_class = candidate_label
                na_top_other_exists_resolved_count += 1
            else:
                # This is the primary path: A non-'NA' label is the clear majority.
                final_constructive_class = candidate_label
                valid_majority_resolved_count += 1

            # Map the final string label to its integer representation
            thread_label = label_mapping.get(final_constructive_class)

            if thread_label is None:
                # This should ideally not happen if `final_constructive_class` is always 'Constructive' or 'Not constructive'
                print(
                    f"Warning: Unexpected final label '{final_constructive_class}' for sdid {sdid}. Skipping."
                )
                continue

            # Reconstruct the thread text using stored comment data and assign author numbers
            comments_for_thread = thread_comment_data.get(sdid)
            if not comments_for_thread:
                print(f"Warning: No comment data found for sdid {sdid}. Skipping.")
                continue

            # Convert to a set of tuples to ensure uniqueness, then back to a list.
            # Before adding to the set, normalize the text to handle formatting inconsistencies.
            unique_comments_tuples = set()
            for c in comments_for_thread:
                unique_comments_tuples.add((c["commentindex"], c["text"], c["author"]))

            # Sort the unique comments by commentindex
            sorted_comments = sorted(list(unique_comments_tuples))

            full_thread_text_parts = []
            author_to_number_map = (
                {}
            )  # Maps author names to unique numbers for the current thread
            current_author_number = 0  # Counter for assigning new author numbers
            max_author_number_for_thread = (
                -1
            )  # Tracks the highest assigned author number

            for comment_index, comment_text, author_name in sorted_comments:
                if author_name not in author_to_number_map:
                    # Assign a new unique number to this author within this thread
                    author_to_number_map[author_name] = current_author_number
                    current_author_number += 1

                assigned_author_number = author_to_number_map[author_name]
                # Update the maximum author number encountered in this thread
                max_author_number_for_thread = max(
                    max_author_number_for_thread, assigned_author_number
                )

                # Construct the comment token with the assigned author number
                full_thread_text_parts.append(
                    f"[author{assigned_author_number}] {comment_text}"
                )

            # Prepare the data for the JSONL output
            thread_data = {
                "sdid": sdid,
                "text": " ".join(full_thread_text_parts),
                "label": thread_label,
            }
            max_author_number_total = max(
                max_author_number_total, max_author_number_for_thread
            )
            outfile.write(json.dumps(thread_data) + "\n")

    print("\n--- Majority Voting Summary ---")
    print(f"Total threads with 'NA' annotations (skipped): {na_exists_count}")
    print(f"Threads skipped (all annotations were 'NA'): {skipped_all_na_count}")
    print(f"Threads skipped (tie in non-'NA' labels only): {skipped_tie_count}")
    print(
        f"Threads skipped (tie between 'NA' and a constructive label): {tie_na_constructive_count}"
    )
    print(
        f"Threads resolved (NA was overall top, but non-NA label existed): {na_top_other_exists_resolved_count}"
    )
    print(
        f"Threads resolved (clear non-'NA' majority): {valid_majority_resolved_count}"
    )
    print(
        f"Total threads attempted to resolve: {na_exists_count + skipped_all_na_count + skipped_tie_count + tie_na_constructive_count + na_top_other_exists_resolved_count + valid_majority_resolved_count}"
    )
    print(f"Max author number across all threads: {max_author_number_total}")
    print(f"Processed data written to {output_path}")

    # Clear memory to free up resources
    del all_thread_annotations
    del thread_comment_data
    del df_temp
