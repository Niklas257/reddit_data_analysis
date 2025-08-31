#!/usr/bin/env python3
"""
Script to parse SLURM output file and extract performance metrics into JSON format.
"""

import json
import re
from typing import Dict, Any


def parse_slurm_output(file_path: str) -> Dict[str, Any]:
    """Parse the SLURM output file and extract performance metrics."""

    with open(file_path, "r") as f:
        content = f.read()

    # Initialize the data structure
    data = {
        "curriculum_iteration_metadata": {
            "iteration_boundaries": {
                "epoch_start_indices": [],
                "num_iterations_completed": 0,
            },
            "pseudo_labeling_stats": [],
            "confidence_thresholds_used": [],
            "training_data_sizes": [],
        },
        "curriculum_epoch_metrics": {
            "train_losses": [],
            "train_accuracies": [],
            "val_losses_from_train_epoch": [],
            "val_accuracies_from_train_epoch": [],
            "val_precision_scores_from_train_epoch": [],
            "val_recall_scores_from_train_epoch": [],
            "val_f1_scores_from_train_epoch": [],
            "val_accuracies_ynacc": [],
            "val_f1s_ynacc": [],
            "val_accuracies_iac": [],
            "val_f1s_iac": [],
            "val_accuracies_reddit": [],
            "val_f1s_reddit": [],
        },
        "final_training_parameters": {
            "model_name": "Qwen/Qwen3-Embedding-0.6B",
            "use_qlora": True,
            "quantization": 4,
            "lora_rank": 16,
            "lora_alpha": 32,
        },
    }

    # Parse curriculum iterations and pseudo-labeling stats
    iteration_pattern = r"--- Curriculum Iteration (\d+)(?:/\d+)? ---"
    pseudo_label_pattern = r"Teacher pseudo-labeling unlabeled Reddit data with confidence threshold: ([\d\.]+)"
    selected_samples_pattern = (
        r"Selected (\d+) high-confidence pseudo-labeled samples for training"
    )
    label_dist_pattern = r"Final pseudo-labeled data label distribution:\s*\{([^}]+)\}"

    # Find all pseudo-labeling sections by splitting content into iteration sections
    iteration_sections = re.split(r"--- Curriculum Iteration \d+", content)[
        1:
    ]  # Skip first empty section

    pseudo_labeling_stats = []
    confidence_thresholds = []

    for i, section in enumerate(iteration_sections):
        # Skip iteration 0 (initial supervised training) - it has no pseudo-labeling
        if i == 0:
            continue

        # Look for pseudo-labeling data in this section
        conf_match = re.search(pseudo_label_pattern, section)
        if conf_match:
            confidence = float(conf_match.group(1))
            confidence_thresholds.append(confidence)

            # Find number of selected samples
            selected_match = re.search(selected_samples_pattern, section)
            num_samples = int(selected_match.group(1)) if selected_match else 0

            # Find label distribution
            dist_match = re.search(label_dist_pattern, section)
            label_distribution = {}
            if dist_match:
                dist_str = dist_match.group(1)
                # Parse the distribution string, e.g., "0: 1" or "0: 500, 1: 400"
                for item in dist_str.split(","):
                    if ":" in item:
                        key, value = item.strip().split(":")
                        label_distribution[key.strip()] = int(value.strip())

            pseudo_stat = {
                "iteration": i,  # iteration number (1-based for pseudo-labeling)
                "confidence_threshold": confidence,
                "num_pseudo_labels": num_samples,
                "label_distribution": label_distribution,
                "avg_confidence": confidence,  # Approximation
                "min_confidence": confidence,  # Approximation
                "max_confidence": confidence,  # Approximation
            }
            pseudo_labeling_stats.append(pseudo_stat)

    data["curriculum_iteration_metadata"][
        "pseudo_labeling_stats"
    ] = pseudo_labeling_stats
    data["curriculum_iteration_metadata"][
        "confidence_thresholds_used"
    ] = confidence_thresholds

    # Parse training metrics from each epoch
    train_metrics_pattern = r"(\d+)/\d+ \| ([\d\.]+)\s+\| ([\d\.]+)\s+\| ([\d\.]+)\s+\| ([\d\.]+)\s+\|\s+([\d\.]+)\s+\| ([\d\.]+)\s+\| ([\d\.]+)\s+\|"

    # Development set patterns
    ynacc_pattern = r"YNACC Dev Loss: ([\d\.]+), Acc: ([\d\.]+), Prec: ([\d\.]+), Rec: ([\d\.]+), F1: ([\d\.]+)"
    iac_pattern = r"IAC Dev Loss: ([\d\.]+), Acc: ([\d\.]+), Prec: ([\d\.]+), Rec: ([\d\.]+), F1: ([\d\.]+)"
    reddit_pattern = r"Reddit Dev Loss: ([\d\.]+), Acc: ([\d\.]+), Prec: ([\d\.]+), Rec: ([\d\.]+), F1: ([\d\.]+)"

    # Find all training metrics
    train_metrics = re.findall(train_metrics_pattern, content)
    ynacc_metrics = re.findall(ynacc_pattern, content)
    iac_metrics = re.findall(iac_pattern, content)
    reddit_metrics = re.findall(reddit_pattern, content)

    # Extract epoch boundaries by analyzing the structure dynamically
    lines = content.split("\n")
    epoch_boundaries = [0]  # Start with epoch 0
    current_epoch = 0

    # Count epochs in each iteration dynamically
    iterations_info = {}
    in_iteration = None
    epoch_count_in_iteration = 0

    for line in lines:
        # Check for any curriculum iteration start
        iter_match = re.search(r"--- Curriculum Iteration (\d+)", line)
        if iter_match:
            iteration_num = int(iter_match.group(1))

            # If we were already in an iteration, save its epoch count
            if in_iteration is not None:
                iterations_info[in_iteration] = epoch_count_in_iteration
                current_epoch += epoch_count_in_iteration
                epoch_boundaries.append(current_epoch)

            # Start new iteration
            in_iteration = iteration_num
            epoch_count_in_iteration = 0

        # Count training epochs
        elif (
            "Initial Supervised Training Epoch" in line
            or "Student Training Epoch" in line
        ) and "Train Loss" not in line:  # Exclude the header line
            if in_iteration is not None:
                epoch_count_in_iteration += 1

    # Add the final iteration's epoch count if we have one
    if in_iteration is not None:
        iterations_info[in_iteration] = epoch_count_in_iteration
        current_epoch += epoch_count_in_iteration
        epoch_boundaries.append(current_epoch)

    data["curriculum_iteration_metadata"]["iteration_boundaries"][
        "epoch_start_indices"
    ] = epoch_boundaries
    data["curriculum_iteration_metadata"]["iteration_boundaries"][
        "num_iterations_completed"
    ] = len(iterations_info)

    # Process training metrics
    for (
        step,
        train_loss,
        train_acc,
        val_loss,
        val_acc,
        val_prec,
        val_rec,
        val_f1,
    ) in train_metrics:
        data["curriculum_epoch_metrics"]["train_losses"].append(float(train_loss))
        data["curriculum_epoch_metrics"]["train_accuracies"].append(float(train_acc))
        data["curriculum_epoch_metrics"]["val_losses_from_train_epoch"].append(
            float(val_loss)
        )
        data["curriculum_epoch_metrics"]["val_accuracies_from_train_epoch"].append(
            float(val_acc)
        )
        data["curriculum_epoch_metrics"][
            "val_precision_scores_from_train_epoch"
        ].append(float(val_prec))
        data["curriculum_epoch_metrics"]["val_recall_scores_from_train_epoch"].append(
            float(val_rec)
        )
        data["curriculum_epoch_metrics"]["val_f1_scores_from_train_epoch"].append(
            float(val_f1)
        )

    # Process development set metrics
    for loss, acc, prec, rec, f1 in ynacc_metrics:
        data["curriculum_epoch_metrics"]["val_accuracies_ynacc"].append(float(acc))
        data["curriculum_epoch_metrics"]["val_f1s_ynacc"].append(float(f1))

    for loss, acc, prec, rec, f1 in iac_metrics:
        data["curriculum_epoch_metrics"]["val_accuracies_iac"].append(float(acc))
        data["curriculum_epoch_metrics"]["val_f1s_iac"].append(float(f1))

    for loss, acc, prec, rec, f1 in reddit_metrics:
        data["curriculum_epoch_metrics"]["val_accuracies_reddit"].append(float(acc))
        data["curriculum_epoch_metrics"]["val_f1s_reddit"].append(float(f1))

    # Add training data sizes (approximation based on pseudo-labeling)
    initial_size = 935  # Approximate based on supervised training
    training_sizes = [initial_size]

    for stat in data["curriculum_iteration_metadata"]["pseudo_labeling_stats"]:
        # Add pseudo-labeled data to previous size
        new_size = training_sizes[-1] + stat["num_pseudo_labels"]
        training_sizes.append(new_size)

    data["curriculum_iteration_metadata"]["training_data_sizes"] = training_sizes

    return data


def main():
    """Main function to parse SLURM output and create JSON file."""

    input_file = "/home/niklas/reddit_project/training_data/modernBERT_focal.out"
    output_file = "/home/niklas/reddit_project/training_data/performance_metrics_modernBERT_focal.json"

    print("Parsing SLURM output file...")
    data = parse_slurm_output(input_file)

    print("Creating JSON file...")
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Performance metrics extracted and saved to: {output_file}")

    # Print summary
    print("\nSummary:")
    print(f"- Total epochs: {len(data['curriculum_epoch_metrics']['train_losses'])}")
    print(
        f"- Curriculum iterations: {data['curriculum_iteration_metadata']['iteration_boundaries']['num_iterations_completed']}"
    )
    print(
        f"- Pseudo-labeling iterations: {len(data['curriculum_iteration_metadata']['pseudo_labeling_stats'])}"
    )
    print(
        f"- Confidence thresholds used: {data['curriculum_iteration_metadata']['confidence_thresholds_used']}"
    )


if __name__ == "__main__":
    main()
