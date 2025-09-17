#!/usr/bin/env python3
"""
Script to analyze Reddit annotation statistics by subreddit.
Generates statistics including TP, FP, TN, FN, accuracy, precision, recall, F1,
and fraction of constructive threads for each subreddit.
"""

import json
import pandas as pd
from collections import defaultdict
import os
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


def load_jsonl(file_path):
    """Load JSONL file and return list of dictionaries."""
    data = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line.strip()))
        print(f"Loaded {len(data)} samples from {file_path}")
    except FileNotFoundError:
        print(f"Warning: File {file_path} not found")
    return data


def calculate_confusion_matrix_components(y_true, y_pred):
    """Calculate TP, FP, TN, FN from true and predicted labels."""
    # Convert to binary if needed
    y_true = [int(label) for label in y_true]
    y_pred = [int(pred) for pred in y_pred]

    tp = sum(1 for true, pred in zip(y_true, y_pred) if true == 1 and pred == 1)
    fp = sum(1 for true, pred in zip(y_true, y_pred) if true == 0 and pred == 1)
    tn = sum(1 for true, pred in zip(y_true, y_pred) if true == 0 and pred == 0)
    fn = sum(1 for true, pred in zip(y_true, y_pred) if true == 1 and pred == 0)

    return tp, fp, tn, fn


def calculate_metrics(y_true, y_pred):
    """Calculate accuracy, precision, recall, and F1 score."""
    if len(y_true) == 0:
        return 0.0, 0.0, 0.0, 0.0

    # Convert to binary if needed
    y_true = [int(label) for label in y_true]
    y_pred = [int(pred) for pred in y_pred]

    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, average="binary", zero_division=0)
    recall = recall_score(y_true, y_pred, average="binary", zero_division=0)
    f1 = f1_score(y_true, y_pred, average="binary", zero_division=0)

    return accuracy, precision, recall, f1


def analyze_subreddit_stats(file_path, mode_name):
    """Analyze statistics for a single file."""
    data = load_jsonl(file_path)

    if not data:
        return {}

    # Group data by subreddit
    subreddit_data = defaultdict(list)

    for item in data:
        subreddit = item.get("subreddit", "unknown")
        subreddit_data[subreddit].append(item)

    # Calculate statistics for each subreddit
    subreddit_stats = {}

    for subreddit, items in subreddit_data.items():
        # Extract labels and predictions
        true_labels = [item["label"] for item in items]
        predictions = [item["prediction"] for item in items]

        # Calculate confusion matrix components
        tp, fp, tn, fn = calculate_confusion_matrix_components(true_labels, predictions)

        # Calculate metrics
        accuracy, precision, recall, f1 = calculate_metrics(true_labels, predictions)

        # Calculate fraction of constructive threads (label == 1)
        total_threads = len(true_labels)
        constructive_threads = sum(1 for label in true_labels if label == 1)
        fraction_constructive = (
            constructive_threads / total_threads if total_threads > 0 else 0.0
        )

        # Store statistics
        subreddit_stats[subreddit] = {
            "mode": mode_name,
            "total_samples": total_threads,
            "constructive_threads": constructive_threads,
            "non_constructive_threads": total_threads - constructive_threads,
            "fraction_constructive": fraction_constructive,
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }

    return subreddit_stats


def create_summary_table(stats_with_thinking, stats_without_thinking):
    """Create a comprehensive summary table comparing both modes."""

    # Get all unique subreddits
    all_subreddits = set(stats_with_thinking.keys()) | set(
        stats_without_thinking.keys()
    )

    summary_data = []

    for subreddit in sorted(all_subreddits):
        # With thinking stats
        with_thinking = stats_with_thinking.get(subreddit, {})
        without_thinking = stats_without_thinking.get(subreddit, {})

        # Create row for with thinking
        if with_thinking:
            row_with = {
                "subreddit": subreddit,
                "mode": "with_thinking",
                "total_samples": with_thinking["total_samples"],
                "constructive_threads": with_thinking["constructive_threads"],
                "fraction_constructive": with_thinking["fraction_constructive"],
                "tp": with_thinking["tp"],
                "fp": with_thinking["fp"],
                "tn": with_thinking["tn"],
                "fn": with_thinking["fn"],
                "accuracy": with_thinking["accuracy"],
                "precision": with_thinking["precision"],
                "recall": with_thinking["recall"],
                "f1": with_thinking["f1"],
            }
            summary_data.append(row_with)

        # Create row for without thinking
        if without_thinking:
            row_without = {
                "subreddit": subreddit,
                "mode": "without_thinking",
                "total_samples": without_thinking["total_samples"],
                "constructive_threads": without_thinking["constructive_threads"],
                "fraction_constructive": without_thinking["fraction_constructive"],
                "tp": without_thinking["tp"],
                "fp": without_thinking["fp"],
                "tn": without_thinking["tn"],
                "fn": without_thinking["fn"],
                "accuracy": without_thinking["accuracy"],
                "precision": without_thinking["precision"],
                "recall": without_thinking["recall"],
                "f1": without_thinking["f1"],
            }
            summary_data.append(row_without)

    return pd.DataFrame(summary_data)


def print_detailed_stats(stats, mode_name):
    """Print detailed statistics for a specific mode."""
    print(f"\n{'='*80}")
    print(f"DETAILED STATISTICS - {mode_name.upper()}")
    print(f"{'='*80}")

    # Sort subreddits by total samples (descending)
    sorted_subreddits = sorted(
        stats.items(), key=lambda x: x[1]["total_samples"], reverse=True
    )

    for subreddit, stat in sorted_subreddits:
        print(f"\nSubreddit: {subreddit}")
        print(f"  Total samples: {stat['total_samples']}")
        print(f"  Constructive threads: {stat['constructive_threads']}")
        print(f"  Non-constructive threads: {stat['non_constructive_threads']}")
        print(f"  Fraction constructive: {stat['fraction_constructive']:.3f}")
        print("  Confusion Matrix:")
        print(f"    TP: {stat['tp']:3d}  |  FP: {stat['fp']:3d}")
        print(f"    FN: {stat['fn']:3d}  |  TN: {stat['tn']:3d}")
        print("  Metrics:")
        print(f"    Accuracy:  {stat['accuracy']:.3f}")
        print(f"    Precision: {stat['precision']:.3f}")
        print(f"    Recall:    {stat['recall']:.3f}")
        print(f"    F1-score:  {stat['f1']:.3f}")


def print_comparison_summary(stats_with_thinking, stats_without_thinking):
    """Print a comparison summary between the two modes."""
    print(f"\n{'='*80}")
    print("COMPARISON SUMMARY")
    print(f"{'='*80}")

    # Get all unique subreddits
    all_subreddits = set(stats_with_thinking.keys()) | set(
        stats_without_thinking.keys()
    )

    print(
        f"{'Subreddit':<20} {'Samples':<8} {'Frac_Constr':<12} {'Acc_WithThink':<14} {'Acc_NoThink':<14} {'F1_WithThink':<13} {'F1_NoThink':<13}"
    )
    print("-" * 110)

    for subreddit in sorted(
        all_subreddits,
        key=lambda x: stats_with_thinking.get(x, stats_without_thinking.get(x, {})).get(
            "total_samples", 0
        ),
        reverse=True,
    ):
        with_thinking = stats_with_thinking.get(subreddit, {})
        without_thinking = stats_without_thinking.get(subreddit, {})

        # Get basic info (should be same for both modes)
        total_samples = with_thinking.get(
            "total_samples", without_thinking.get("total_samples", 0)
        )
        fraction_constructive = with_thinking.get(
            "fraction_constructive", without_thinking.get("fraction_constructive", 0.0)
        )

        acc_with = with_thinking.get("accuracy", float("nan"))
        acc_without = without_thinking.get("accuracy", float("nan"))
        f1_with = with_thinking.get("f1", float("nan"))
        f1_without = without_thinking.get("f1", float("nan"))

        print(
            f"{subreddit:<20} {total_samples:<8} {fraction_constructive:<12.3f} {acc_with:<14.3f} {acc_without:<14.3f} {f1_with:<13.3f} {f1_without:<13.3f}"
        )


def main():
    """Main function to run the analysis."""
    print("Reddit Subreddit Statistics Analysis")
    print("=" * 50)

    model = "qwen_1.7B_inst"  # Change model name as needed
    # File paths
    base_dir = f"../training_data/{model}"
    file_with_thinking = os.path.join(
        base_dir, "inst_annotated_data_reddit_with_thinking.jsonl"
    )
    file_without_thinking = os.path.join(
        base_dir, "inst_annotated_data_reddit_without_thinking.jsonl"
    )

    # Analyze both files
    print("Analyzing statistics for both thinking modes...")

    stats_with_thinking = analyze_subreddit_stats(file_with_thinking, "with_thinking")
    stats_without_thinking = analyze_subreddit_stats(
        file_without_thinking, "without_thinking"
    )

    # Print detailed statistics for each mode
    print_detailed_stats(stats_with_thinking, "WITH THINKING")
    print_detailed_stats(stats_without_thinking, "WITHOUT THINKING")

    # Print comparison summary
    print_comparison_summary(stats_with_thinking, stats_without_thinking)

    # Create and save summary table
    summary_df = create_summary_table(stats_with_thinking, stats_without_thinking)

    # Save to CSV
    output_file = (
        "../training_data/reddit_subreddit_statistics.csv"
    )
    summary_df.to_csv(output_file, index=False)
    print(f"\nDetailed statistics saved to: {output_file}")

    # Save to JSON for programmatic access
    json_output = {
        "with_thinking": stats_with_thinking,
        "without_thinking": stats_without_thinking,
        "summary": {
            "total_subreddits": len(
                set(stats_with_thinking.keys()) | set(stats_without_thinking.keys())
            ),
            "total_samples_with_thinking": sum(
                stat["total_samples"] for stat in stats_with_thinking.values()
            ),
            "total_samples_without_thinking": sum(
                stat["total_samples"] for stat in stats_without_thinking.values()
            ),
        },
    }

    json_output_file = (
        "../training_data/reddit_subreddit_statistics.json"
    )
    with open(json_output_file, "w") as f:
        json.dump(json_output, f, indent=4)

    print(f"JSON statistics saved to: {json_output_file}")

    # Print overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")
    print(
        f"Total unique subreddits: {len(set(stats_with_thinking.keys()) | set(stats_without_thinking.keys()))}"
    )
    print(
        f"Total samples (with thinking): {sum(stat['total_samples'] for stat in stats_with_thinking.values())}"
    )
    print(
        f"Total samples (without thinking): {sum(stat['total_samples'] for stat in stats_without_thinking.values())}"
    )

    # Calculate overall metrics for each mode
    if stats_with_thinking:
        overall_acc_with = sum(
            stat["accuracy"] * stat["total_samples"]
            for stat in stats_with_thinking.values()
        ) / sum(stat["total_samples"] for stat in stats_with_thinking.values())
        overall_f1_with = sum(
            stat["f1"] * stat["total_samples"] for stat in stats_with_thinking.values()
        ) / sum(stat["total_samples"] for stat in stats_with_thinking.values())
        print(f"Overall accuracy (with thinking): {overall_acc_with:.3f}")
        print(f"Overall F1 (with thinking): {overall_f1_with:.3f}")

    if stats_without_thinking:
        overall_acc_without = sum(
            stat["accuracy"] * stat["total_samples"]
            for stat in stats_without_thinking.values()
        ) / sum(stat["total_samples"] for stat in stats_without_thinking.values())
        overall_f1_without = sum(
            stat["f1"] * stat["total_samples"]
            for stat in stats_without_thinking.values()
        ) / sum(stat["total_samples"] for stat in stats_without_thinking.values())
        print(f"Overall accuracy (without thinking): {overall_acc_without:.3f}")
        print(f"Overall F1 (without thinking): {overall_f1_without:.3f}")


if __name__ == "__main__":
    main()
