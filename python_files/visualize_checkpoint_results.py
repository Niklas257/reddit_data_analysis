"""
Visualization script for checkpoint test results.
Creates comprehensive plots showing model performance across checkpoints, datasets, and thresholds.
"""

import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import argparse

# Set style for better-looking plots
plt.style.use("seaborn-v0_8")
sns.set_palette("husl")


def load_results(json_file):
    """Load checkpoint test results from JSON file."""
    with open(json_file, "r") as f:
        return json.load(f)


def extract_epoch_from_checkpoint(checkpoint_name):
    """Extract epoch number from checkpoint name for sorting."""
    try:
        # Handle format like 'best_model_iter_X_epoch_Y'
        parts = checkpoint_name.split("_")
        for i, part in enumerate(parts):
            if part == "epoch" and i + 1 < len(parts):
                return int(parts[i + 1])
    except (ValueError, IndexError):
        pass
    return 0


def extract_iteration_from_checkpoint(checkpoint_name):
    """Extract iteration number from checkpoint name."""
    try:
        # Handle format like 'best_model_iter_X_epoch_Y'
        parts = checkpoint_name.split("_")
        for i, part in enumerate(parts):
            if part == "iter" and i + 1 < len(parts):
                return int(parts[i + 1])
    except (ValueError, IndexError):
        pass
    return 0


def get_checkpoint_label(checkpoint_name):
    """Get a readable label for checkpoint (e.g., 'it0 e1')."""
    iteration = extract_iteration_from_checkpoint(checkpoint_name)
    epoch = extract_epoch_from_checkpoint(checkpoint_name)
    return f"it{iteration} e{epoch}"


def create_confusion_matrix_grids(data, output_dir):
    """Create confusion matrix grids for each checkpoint with datasets in rows and thresholds in columns."""

    # Get all datasets and checkpoints
    all_datasets = set()
    all_checkpoints = []

    for checkpoint, datasets in data.items():
        all_checkpoints.append(checkpoint)
        all_datasets.update(datasets.keys())

    # Sort checkpoints by iteration and epoch
    all_checkpoints.sort(
        key=lambda x: (
            extract_iteration_from_checkpoint(x),
            extract_epoch_from_checkpoint(x),
        )
    )
    all_datasets = sorted(list(all_datasets))

    # Create one plot per checkpoint
    for checkpoint in all_checkpoints:
        checkpoint_label = get_checkpoint_label(checkpoint)

        # Get all thresholds for this checkpoint
        all_thresholds = set()
        for dataset in all_datasets:
            if dataset in data[checkpoint]:
                for threshold_key, metrics in data[checkpoint][dataset].items():
                    if "threshold" in metrics:
                        all_thresholds.add(metrics["threshold"])

        all_thresholds = sorted(list(all_thresholds))

        if not all_thresholds:
            continue

        # Create grid: 3 datasets (rows) × 4 thresholds (columns)
        rows = len(all_datasets)
        cols = len(all_thresholds)

        fig, axes = plt.subplots(rows, cols, figsize=(4 * cols, 3 * rows))

        # Handle different grid sizes
        if rows == 1 and cols == 1:
            axes = [[axes]]
        elif rows == 1:
            axes = [axes]
        elif cols == 1:
            axes = [[ax] for ax in axes]

        fig.suptitle(
            f"Confusion Matrices - {checkpoint_label}", fontsize=16, fontweight="bold"
        )

        for row_idx, dataset in enumerate(all_datasets):
            for col_idx, threshold in enumerate(all_thresholds):
                ax = axes[row_idx][col_idx]

                # Find metrics for this dataset and threshold
                metrics = None
                if dataset in data[checkpoint]:
                    for threshold_key, threshold_metrics in data[checkpoint][
                        dataset
                    ].items():
                        if (
                            "threshold" in threshold_metrics
                            and abs(threshold_metrics["threshold"] - threshold) < 0.01
                        ):
                            required_keys = [
                                "tp",
                                "fp",
                                "tn",
                                "fn",
                                "accuracy",
                                "precision",
                                "recall",
                                "f1",
                                "avg_loss",
                            ]
                            if all(key in threshold_metrics for key in required_keys):
                                metrics = threshold_metrics
                                break

                if metrics:
                    cm = np.array(
                        [[metrics["tn"], metrics["fp"]], [metrics["fn"], metrics["tp"]]]
                    )

                    sns.heatmap(
                        cm,
                        annot=True,
                        fmt="d",
                        cmap="Blues",
                        ax=ax,
                        xticklabels=["Neg", "Pos"],
                        yticklabels=["Neg", "Pos"],
                        cbar=False,
                    )

                    # Compact metrics display - 2 lines
                    title_text = f"{dataset.upper()} | T:{threshold:.1f}, avg_loss:{metrics['avg_loss']:.3f}\n"
                    title_text += f'Acc:{metrics["accuracy"]:.3f} Pre:{metrics["precision"]:.3f} Rec:{metrics["recall"]:.3f} F1:{metrics["f1"]:.3f}'
                    ax.set_title(title_text, fontsize=12)
                else:
                    ax.text(
                        0.5,
                        0.5,
                        "No Data",
                        ha="center",
                        va="center",
                        transform=ax.transAxes,
                        fontsize=10,
                    )
                    ax.set_title(
                        f"{dataset.upper()} | T:{threshold:.1f}\nNo Data", fontsize=10
                    )
                    ax.set_xticks([])
                    ax.set_yticks([])

        plt.tight_layout()
        filename = f"confusion_matrices_{checkpoint}.png"
        plt.savefig(os.path.join(output_dir, filename), dpi=300, bbox_inches="tight")
        plt.close()


def create_loss_plots_by_checkpoint(data, output_dir):
    """Create loss plots for each dataset with iteration-epoch labels."""
    # Group data by checkpoint and sort by iteration/epoch
    checkpoint_data = {}
    for checkpoint, datasets in data.items():
        iteration = extract_iteration_from_checkpoint(checkpoint)
        epoch = extract_epoch_from_checkpoint(checkpoint)
        checkpoint_label = get_checkpoint_label(checkpoint)
        checkpoint_data[checkpoint] = {
            "iteration": iteration,
            "epoch": epoch,
            "label": checkpoint_label,
            "datasets": datasets,
        }

    # Sort checkpoints by iteration then epoch
    sorted_checkpoints = sorted(
        checkpoint_data.items(), key=lambda x: (x[1]["iteration"], x[1]["epoch"])
    )

    # Create separate plots for each dataset
    datasets = set()
    for _, checkpoint_info in sorted_checkpoints:
        datasets.update(checkpoint_info["datasets"].keys())

    for dataset in datasets:
        fig, ax = plt.subplots(figsize=(15, 8))

        # Pre-process data for this dataset to group by threshold
        threshold_data = {}  # {0.5: {0: loss, 1: loss}, 0.6: ...}

        for checkpoint_idx, (_, checkpoint_info) in enumerate(sorted_checkpoints):
            if dataset in checkpoint_info["datasets"]:
                for key, metrics in checkpoint_info["datasets"][dataset].items():
                    if "threshold" in metrics and "avg_loss" in metrics:
                        t = round(metrics["threshold"], 1)
                        if t not in threshold_data:
                            threshold_data[t] = {}
                        threshold_data[t][checkpoint_idx] = metrics["avg_loss"]

        all_thresholds = sorted(threshold_data.keys())
        print(f"Dataset {dataset}: Found thresholds {all_thresholds}")

        if not all_thresholds:
            print(f"No threshold data found for dataset {dataset}")
            plt.close()
            continue

        colors = plt.cm.tab10(np.linspace(0, 1, len(all_thresholds)))

        # For each threshold, plot the collected data
        for threshold_idx, threshold in enumerate(all_thresholds):
            data_points = threshold_data.get(threshold, {})
            if data_points:
                # Get all available checkpoint indices for this threshold
                x_positions = sorted(data_points.keys())
                losses = [data_points[x] for x in x_positions]

                print(
                    f"  Threshold {threshold}: Found {len(losses)} data points at positions {x_positions}: {losses}"
                )

                # Only plot if we have data points
                if len(losses) > 0:
                    # Use different styles for single points vs lines
                    if len(losses) == 1:
                        # For single points, use larger markers and no line
                        ax.scatter(
                            x_positions,
                            losses,
                            marker="o",
                            s=100,  # larger marker size
                            label=f"Threshold {threshold:.1f}",
                            color=colors[threshold_idx],
                            alpha=0.8,
                            edgecolors="black",
                            linewidths=1,
                        )
                    else:
                        # For multiple points, use lines with markers
                        ax.plot(
                            x_positions,
                            losses,
                            marker="o",
                            linewidth=2,
                            markersize=8,
                            label=f"Threshold {threshold:.1f}",
                            color=colors[threshold_idx],
                            alpha=0.8,
                        )
            else:
                print(f"  Threshold {threshold}: No data points found")

        # Set up the plot with all checkpoint labels
        all_labels = [info["label"] for _, info in sorted_checkpoints]
        if all_labels:
            ax.set_xticks(range(len(all_labels)))
            ax.set_xticklabels(all_labels, rotation=45, ha="right")

        ax.set_xlabel("Checkpoint (Iteration-Epoch)")
        ax.set_ylabel("Average Loss")
        ax.set_title(f"Loss Over Training - {dataset.upper()} Dataset")

        # Only show legend if we have plotted lines
        if ax.get_lines():
            ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        filename = f"loss_over_training_{dataset}.png"
        plt.savefig(os.path.join(output_dir, filename), dpi=300, bbox_inches="tight")
        plt.close()

        print(f"Saved plot for {dataset} with {len(ax.get_lines())} lines")


def main():
    parser = argparse.ArgumentParser(description="Visualize checkpoint test results")
    parser.add_argument(
        "--json_file",
        type=str,
        default="../training_data/checkpoint_test_results_qwen_focal_qlora.json",
        help="Path to JSON results file",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="../training_data/checkpoint_visualizations",
        help="Output directory for plots",
    )

    args = parser.parse_args()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Load data
    print(f"Loading results from {args.json_file}...")
    data = load_results(args.json_file)

    print(f"Found {len(data)} checkpoints")
    print(f"Datasets: {list(next(iter(data.values())).keys())}")

    # Create visualizations
    print("Creating confusion matrix grids for each checkpoint...")
    create_confusion_matrix_grids(data, args.output_dir)

    print("Creating loss plots by checkpoint...")
    create_loss_plots_by_checkpoint(data, args.output_dir)

    print(f"\nAll visualizations saved to: {args.output_dir}")
    print("\nGenerated files:")
    for file in os.listdir(args.output_dir):
        print(f"  - {file}")


if __name__ == "__main__":
    main()
