#!/usr/bin/env python3
"""
Script to visualize the performance metrics from modernBERT training.
Creates plots for:
1. Training loss over epochs
2. Combined validation metrics (accuracy, precision, recall, F1)
3. Pseudo-labeled data distributions at different confidence thresholds
"""

import json
import matplotlib.pyplot as plt
from pathlib import Path

# Try to import seaborn, fall back to matplotlib if not available
try:
    import seaborn as sns

    sns.set_palette("husl")
except ImportError:
    print("Seaborn not available, using matplotlib defaults")

# Use clean white background
plt.style.use("default")


def load_metrics(file_path):
    """Load the performance metrics from JSON file."""
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def add_iteration_axis(ax, data, x_limit):
    """Add iteration labels above the main plot axis."""
    if "curriculum_iteration_metadata" not in data:
        return

    boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
        "epoch_start_indices"
    ]

    if len(boundaries) <= 1:
        return

    # Create a second x-axis above the plot
    ax2 = ax.twiny()

    # Calculate midpoints for each iteration (only for complete iterations)
    iteration_positions = []
    iteration_labels = []

    # Only process complete iterations (not the final incomplete one)
    for i in range(len(boundaries) - 1):
        start_epoch = boundaries[i] + 1
        end_epoch = boundaries[i + 1]

        # Calculate midpoint position
        midpoint = (start_epoch + end_epoch) / 2
        iteration_positions.append(midpoint)
        iteration_labels.append(str(i))

    # Set the iteration ticks and labels
    ax2.set_xlim(ax.get_xlim())
    ax2.set_xticks(iteration_positions)
    ax2.set_xticklabels(iteration_labels, fontsize=12)
    ax2.set_xlabel("Iteration", fontsize=15, labelpad=15)
    ax2.tick_params(axis="x", which="major", length=0)  # Remove tick marks

    # Remove all spines from the iteration axis to prevent the empty box
    for spine in ax2.spines.values():
        spine.set_visible(False)

    # Disable the legend for the secondary axis to prevent empty legend box
    ax2.legend().set_visible(False) if ax2.get_legend() else None


def plot_training_loss(data, targets, save_path=None):
    """Plot training and validation loss over epochs."""
    train_target = data["curriculum_epoch_metrics"][targets[0]]
    val_target = data["curriculum_epoch_metrics"][targets[1]]

    plt.figure(figsize=(12, 6))
    ax = plt.gca()

    # Create epoch indices starting from 1 (not 0)
    epochs = list(range(1, len(train_target) + 1))

    # Plot with breaks at iteration boundaries if available
    if "curriculum_iteration_metadata" in data:
        boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
            "epoch_start_indices"
        ]

        # Plot each iteration segment separately
        for i in range(len(boundaries)):
            start_idx = boundaries[i]
            end_idx = (
                boundaries[i + 1] if i + 1 < len(boundaries) else len(train_target)
            )

            # Get epoch range for this iteration
            iter_epochs = list(range(start_idx + 1, end_idx + 1))
            iter_train = train_target[start_idx:end_idx]
            iter_val = val_target[start_idx:end_idx]

            # Only add label for first iteration
            train_label = "Training Loss" if i == 0 else ""
            val_label = "Validation Loss" if i == 0 else ""

            plt.plot(
                iter_epochs,
                iter_train,
                linewidth=2,
                marker="o",
                markersize=5,
                label=train_label,
                color="#1f77b4",
            )
            plt.plot(
                iter_epochs,
                iter_val,
                linewidth=2,
                marker="s",
                markersize=5,
                label=val_label,
                color="#ff7f0e",
            )
    else:
        # Fallback to original plotting if no iteration metadata
        plt.plot(
            epochs,
            train_target,
            linewidth=2,
            marker="o",
            markersize=5,
            label="Training Loss",
            color="#1f77b4",
        )
        plt.plot(
            epochs,
            val_target,
            linewidth=2,
            marker="s",
            markersize=5,
            label="Validation Loss",
            color="#ff7f0e",
        )
    """plt.title(
        "Training and Validation Loss Over Epochs", fontsize=16, fontweight="bold"
    )"""
    plt.xlabel("Epoch", fontsize=15)
    plt.ylabel("Loss", fontsize=15)
    plt.legend(fontsize=15)
    plt.tick_params(axis="both", labelsize=12)
    plt.grid(True, alpha=0.3)

    # Set x-axis limits to fill the entire grid
    plt.xlim(1, len(train_target))

    # Add curriculum iteration boundaries if available
    if "curriculum_iteration_metadata" in data:
        boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
            "epoch_start_indices"
        ]

        # Add vertical lines at the END of each iteration (beginning of next iteration)
        for i, boundary in enumerate(boundaries[1:], 1):
            if boundary < len(
                train_target
            ):  # Changed <= to < to avoid boundary at the very end
                # Place vertical line half a step to the right of the boundary point
                plt.axvline(
                    x=boundary + 0.5,
                    color="red",
                    linestyle="--",
                    alpha=0.7,
                    ymin=0.0,
                    ymax=1.0,
                    clip_on=False,  # Allow line to extend beyond plot area
                    label="Iteration end" if i == 1 else "",
                )

        # Update legend to include all items
        plt.legend(fontsize=15, loc="upper right")

        # Create custom x-axis labels showing epoch within each iteration
        if len(boundaries) > 1:
            # Create tick positions and labels for each iteration
            tick_positions = []
            tick_labels = []

            for i in range(len(boundaries)):
                start_epoch = boundaries[i] + 1  # Start from 1 within each iteration
                end_epoch = (
                    boundaries[i + 1] if i + 1 < len(boundaries) else len(train_target)
                )

                # Add tick at start of iteration (epoch 1 within iteration)
                if start_epoch <= len(train_target):
                    tick_positions.append(start_epoch)
                    tick_labels.append("1")  # Removed (ItN) part

                # Add intermediate ticks (every 5 epochs within iteration)
                for global_epoch in range(
                    start_epoch + 4, end_epoch + 1, 5
                ):  # Start from epoch 5, then 10, 15, etc.
                    if global_epoch <= len(train_target):
                        epoch_in_iteration = global_epoch - boundaries[i]

                        # Skip only if this tick is exactly at the end of the iteration
                        if global_epoch == end_epoch:
                            continue

                        tick_positions.append(global_epoch)
                        tick_labels.append(str(epoch_in_iteration))

            # Set custom ticks
            plt.xticks(tick_positions, tick_labels, rotation=0)

        # Add iteration axis above the plot
        add_iteration_axis(ax, data, len(train_target))

    # Final legend call to include all items including iteration end
    # plt.legend(fontsize=15)

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.show()


def plot_validation_metrics(data, save_path=None):
    """Plot train vs val accuracy and validation F1 scores by dataset."""

    # Get epoch count for x-axis
    if "train_losses" in data["curriculum_epoch_metrics"]:
        total_epochs = len(data["curriculum_epoch_metrics"]["train_losses"])
        epochs = list(range(1, total_epochs + 1))
    else:
        return

    # Plot 1: Training vs Validation Accuracy (with iteration breaks)
    plt.figure(figsize=(12, 6))
    ax1 = plt.gca()

    if "curriculum_iteration_metadata" in data:
        boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
            "epoch_start_indices"
        ]

        # Plot each iteration segment separately for accuracy
        for i in range(len(boundaries)):
            start_idx = boundaries[i]
            end_idx = boundaries[i + 1] if i + 1 < len(boundaries) else total_epochs

            iter_epochs = list(range(start_idx + 1, end_idx + 1))

            # Only add label for first iteration
            train_label = "Training Accuracy" if i == 0 else ""
            val_label = "Validation Accuracy" if i == 0 else ""

            if "train_accuracies" in data["curriculum_epoch_metrics"]:
                train_acc = data["curriculum_epoch_metrics"]["train_accuracies"]
                iter_train_acc = train_acc[start_idx:end_idx]
                plt.plot(
                    iter_epochs,
                    iter_train_acc,
                    label=train_label,
                    color="#1f77b4",
                    linewidth=2,
                    marker="o",
                    markersize=5,
                )

            if "val_accuracies_from_train_epoch" in data["curriculum_epoch_metrics"]:
                val_acc = data["curriculum_epoch_metrics"][
                    "val_accuracies_from_train_epoch"
                ]
                iter_val_acc = val_acc[start_idx:end_idx]
                plt.plot(
                    iter_epochs,
                    iter_val_acc,
                    label=val_label,
                    color="#ff7f0e",
                    linewidth=2,
                    marker="s",
                    markersize=5,
                )
    else:
        # Fallback to original plotting
        if "train_accuracies" in data["curriculum_epoch_metrics"]:
            train_acc = data["curriculum_epoch_metrics"]["train_accuracies"]
            plt.plot(
                epochs,
                train_acc,
                label="Training Accuracy",
                color="#1f77b4",
                linewidth=2,
                marker="o",
                markersize=5,
            )

        if "val_accuracies_from_train_epoch" in data["curriculum_epoch_metrics"]:
            val_acc = data["curriculum_epoch_metrics"][
                "val_accuracies_from_train_epoch"
            ]
            plt.plot(
                epochs,
                val_acc,
                label="Validation Accuracy",
                color="#ff7f0e",
                linewidth=2,
                marker="s",
                markersize=5,
            )

    plt.xlabel("Epoch", fontsize=15)
    plt.ylabel("Accuracy", fontsize=15)
    plt.tick_params(axis="both", labelsize=12)
    plt.grid(True, alpha=0.3)
    plt.xlim(1, total_epochs)

    # Add curriculum iteration boundaries
    if "curriculum_iteration_metadata" in data:
        boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
            "epoch_start_indices"
        ]

        for i, boundary in enumerate(boundaries[1:], 1):
            if (
                boundary < total_epochs
            ):  # Changed <= to < to avoid boundary at the very end
                plt.axvline(
                    x=boundary + 0.5,
                    color="red",
                    linestyle="--",
                    alpha=0.7,
                    ymin=0.0,
                    ymax=1.0,
                    clip_on=False,  # Allow line to extend beyond plot area
                    label="Iteration end" if i == 1 else "",
                )
        plt.legend(fontsize=15)

        # Create custom x-axis labels for accuracy plot
        if len(boundaries) > 1:
            tick_positions = []
            tick_labels = []

            for i in range(len(boundaries)):
                start_epoch = boundaries[i] + 1
                end_epoch = (
                    boundaries[i + 1] if i + 1 < len(boundaries) else total_epochs
                )

                # Add tick at start of iteration
                if start_epoch <= total_epochs:
                    tick_positions.append(start_epoch)
                    tick_labels.append("1")  # Removed (ItN) part

                # Add intermediate ticks (every 5 epochs within iteration)
                for global_epoch in range(start_epoch + 4, end_epoch + 1, 5):
                    if global_epoch <= total_epochs:
                        epoch_in_iteration = global_epoch - boundaries[i]

                        # Skip only if this tick is exactly at the end of the iteration
                        if global_epoch == end_epoch:
                            continue

                        tick_positions.append(global_epoch)
                        tick_labels.append(str(epoch_in_iteration))

            plt.xticks(tick_positions, tick_labels, rotation=0)

        # Add iteration axis above the plot
        add_iteration_axis(ax1, data, total_epochs)

    # Final legend call to include all items including iteration end
    # plt.legend(fontsize=15)

    plt.tight_layout()
    if save_path:
        accuracy_save_path = str(save_path).replace(".png", "_accuracy.png")
        plt.savefig(accuracy_save_path, dpi=300, bbox_inches="tight")
    plt.show()

    # Plot 2: Validation F1 scores for each dataset (with iteration breaks)
    plt.figure(figsize=(12, 6))
    ax2 = plt.gca()

    datasets = ["iac", "ynacc", "reddit"]
    colors = ["#2E8B57", "#720E9E", "#FF4500"]
    markers = ["d", "o", "s"]  # diamonds, circles, squares

    if "curriculum_iteration_metadata" in data:
        boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
            "epoch_start_indices"
        ]

        # Plot each iteration segment separately for F1 scores
        for i in range(len(boundaries)):
            start_idx = boundaries[i]
            end_idx = boundaries[i + 1] if i + 1 < len(boundaries) else total_epochs

            for idx, dataset in enumerate(datasets):
                if f"val_f1s_{dataset}" in data["curriculum_epoch_metrics"]:
                    f1s = data["curriculum_epoch_metrics"][f"val_f1s_{dataset}"]
                    f1s_length = len(f1s)

                    # Adjust end_idx to not exceed the length of the f1s array
                    adjusted_end_idx = min(end_idx, f1s_length)

                    # Only proceed if we have data for this iteration
                    if start_idx < f1s_length:
                        iter_f1s = f1s[start_idx:adjusted_end_idx]
                        iter_epochs = list(range(start_idx + 1, adjusted_end_idx + 1))

                        # Only add label for first iteration
                        label = f"{dataset.upper()}" if i == 0 else ""

                        plt.plot(
                            iter_epochs,
                            iter_f1s,
                            label=label,
                            color=colors[idx],
                            linewidth=2,
                            marker=markers[idx],
                            markersize=5,
                        )
    else:
        # Fallback to original plotting
        for idx, dataset in enumerate(datasets):
            if f"val_f1s_{dataset}" in data["curriculum_epoch_metrics"]:
                f1s = data["curriculum_epoch_metrics"][f"val_f1s_{dataset}"]
                f1s_length = len(f1s)

                # Create epochs array matching the F1 scores length
                f1s_epochs = list(range(1, f1s_length + 1))

                plt.plot(
                    f1s_epochs,
                    f1s,
                    label=f"{dataset.upper()}",
                    color=colors[idx],
                    linewidth=2,
                    marker=markers[idx],
                    markersize=5,
                )

    plt.xlabel("Epoch", fontsize=15)
    plt.ylabel("F1 Score", fontsize=15)
    plt.tick_params(axis="both", labelsize=12)
    plt.grid(True, alpha=0.3)

    # Set x-axis limit based on the shortest dataset-specific metric array
    dataset_metrics_length = total_epochs
    for dataset in datasets:
        if f"val_f1s_{dataset}" in data["curriculum_epoch_metrics"]:
            f1s_length = len(data["curriculum_epoch_metrics"][f"val_f1s_{dataset}"])
            dataset_metrics_length = min(dataset_metrics_length, f1s_length)

    plt.xlim(1, dataset_metrics_length)

    # Add curriculum iteration boundaries
    if "curriculum_iteration_metadata" in data:
        boundaries = data["curriculum_iteration_metadata"]["iteration_boundaries"][
            "epoch_start_indices"
        ]

        for i, boundary in enumerate(boundaries[1:], 1):
            if (
                boundary < dataset_metrics_length
            ):  # Changed <= to < to avoid boundary at the very end
                plt.axvline(
                    x=boundary + 0.5,
                    color="red",
                    linestyle="--",
                    alpha=0.7,
                    ymin=0.0,
                    ymax=1.0,
                    clip_on=False,  # Allow line to extend beyond plot area
                    label="Iteration end" if i == 1 else "",
                )
        plt.legend(fontsize=15)

        # Create custom x-axis labels for F1 plot
        if len(boundaries) > 1:
            tick_positions = []
            tick_labels = []

            for i in range(len(boundaries)):
                start_epoch = boundaries[i] + 1
                end_epoch = (
                    boundaries[i + 1]
                    if i + 1 < len(boundaries)
                    else dataset_metrics_length
                )

                # Add tick at start of iteration
                if start_epoch <= dataset_metrics_length:
                    tick_positions.append(start_epoch)
                    tick_labels.append("1")  # Removed (ItN) part

                # Add intermediate ticks (every 5 epochs within iteration)
                for global_epoch in range(start_epoch + 4, end_epoch + 1, 5):
                    if global_epoch <= dataset_metrics_length:
                        epoch_in_iteration = global_epoch - boundaries[i]

                        # Skip only if this tick is exactly at the end of the iteration
                        if global_epoch == end_epoch:
                            continue

                        tick_positions.append(global_epoch)
                        tick_labels.append(str(epoch_in_iteration))

            plt.xticks(tick_positions, tick_labels, rotation=0)

        # Add iteration axis above the plot
        add_iteration_axis(ax2, data, dataset_metrics_length)

    # Final legend call to include all items including iteration end
    # plt.legend(fontsize=15)

    plt.tight_layout()
    if save_path:
        f1_save_path = str(save_path).replace(".png", "_f1.png")
        plt.savefig(f1_save_path, dpi=300, bbox_inches="tight")
    plt.show()


def plot_pseudo_label_distributions(data, save_path=None):
    """Plot pseudo-labeled data distributions at different confidence thresholds."""
    if "curriculum_iteration_metadata" not in data:
        print("No curriculum iteration metadata found for pseudo-labeling plots.")
        return

    pseudo_stats = data["curriculum_iteration_metadata"]["pseudo_labeling_stats"]

    if not pseudo_stats:
        print("No pseudo-labeling statistics found.")
        return

    # Extract data for plotting
    iterations = [stat["iteration"] for stat in pseudo_stats]
    num_pseudo_labels = [stat["num_pseudo_labels"] for stat in pseudo_stats]

    # Extract label distributions
    label_0_counts = []
    label_1_counts = []

    for stat in pseudo_stats:
        dist = stat["label_distribution"]
        label_0_counts.append(dist.get("0", 0))
        label_1_counts.append(dist.get("1", 0))

    # Create subplots - only top row (1x2 layout)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))

    # Plot 1: Number of pseudo labels vs iteration
    axes[0].plot(
        iterations,
        num_pseudo_labels,
        "o-",
        linewidth=2,
        markersize=5,
        color="#E74C3C",
    )
    """axes[0].set_title(
        "Pseudo Labels vs Iteration", fontsize=15, fontweight="bold"
    )"""
    axes[0].set_xlabel("Iteration", fontsize=15)
    axes[0].set_ylabel("Pseudo Labeled Samples", fontsize=15)
    axes[0].tick_params(axis="both", labelsize=12)
    axes[0].grid(True, alpha=0.3)
    # Set integer ticks for iterations
    axes[0].set_xticks(iterations)

    # Plot 2: Stacked bar chart of label distributions
    width = 0.6
    axes[1].bar(
        iterations, label_0_counts, width, label="Label 0", alpha=0.8, color="#5D7B9D"
    )  # Muted dark blue
    axes[1].bar(
        iterations,
        label_1_counts,
        width,
        bottom=label_0_counts,
        label="Label 1",
        alpha=0.8,
        color="#9BB4D0",  # Muted light blue
    )
    # axes[1].set_title("Label Distribution by Iteration", fontsize=15, fontweight="bold")
    axes[1].set_xlabel("Iteration", fontsize=15)
    axes[1].set_ylabel("Pseudo Labeled Samples", fontsize=15)
    axes[1].legend(fontsize=15)
    axes[1].tick_params(axis="both", labelsize=12)
    axes[1].grid(True, alpha=0.3)
    axes[1].set_xticks(iterations)

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.show()


def create_summary_table(data):
    """Create a summary table of final metrics."""
    print("\n" + "=" * 60)
    print("TRAINING SUMMARY")
    print("=" * 60)

    # Final training parameters
    if "final_training_parameters" in data:
        params = data["final_training_parameters"]
        print(f"Model: {params.get('model_name', 'modernBERT')}")
        print(f"Learning Rate: {params.get('learning_rate', 'N/A')}")
        print(f"Batch Size: {params.get('batch_size', 'N/A')}")
        print(f"Max Length: {params.get('max_len', 'N/A')}")
        print(
            f"Epochs per Iteration: {params.get('epochs_per_curriculum_iteration', 'N/A')}"
        )
        print(f"Use QLoRA: {params.get('use_qlora', 'N/A')}")
        final_val_loss = params.get("final_overall_best_val_loss", "N/A")
        if final_val_loss != "N/A":
            print(f"Final Best Val Loss: {final_val_loss:.6f}")
        else:
            print(f"Final Best Val Loss: {final_val_loss}")

    # Curriculum learning stats
    if "curriculum_iteration_metadata" in data:
        meta = data["curriculum_iteration_metadata"]
        print(
            f"\nCurriculum Iterations Completed: {meta['iteration_boundaries']['num_iterations_completed']}"
        )

        if meta["pseudo_labeling_stats"]:
            final_stats = meta["pseudo_labeling_stats"][-1]
            print(
                f"Final Confidence Threshold: {final_stats['confidence_threshold']:.3f}"
            )
            print(f"Total Pseudo Labels Generated: {final_stats['num_pseudo_labels']}")

    # Final test metrics if available
    if "final_test_metrics" in data:
        test_metrics = data["final_test_metrics"]
        print("\nFINAL TEST METRICS:")
        for dataset, metrics in test_metrics.items():
            print(f"  {dataset.upper()}:")
            accuracy = metrics.get("accuracy", "N/A")
            precision = metrics.get("precision", "N/A")
            recall = metrics.get("recall", "N/A")
            f1_score = metrics.get("f1_score", "N/A")

            if accuracy != "N/A":
                print(f"    Accuracy: {accuracy:.4f}")
            else:
                print(f"    Accuracy: {accuracy}")

            if precision != "N/A":
                print(f"    Precision: {precision:.4f}")
            else:
                print(f"    Precision: {precision}")

            if recall != "N/A":
                print(f"    Recall: {recall:.4f}")
            else:
                print(f"    Recall: {recall}")

            if f1_score != "N/A":
                print(f"    F1 Score: {f1_score:.4f}")
            else:
                print(f"    F1 Score: {f1_score}")


def main():
    """Main function to generate all visualizations."""
    model = "modernBERT"  # Set to "qwen" for qwen visualizations
    # Path to the performance metrics file
    metrics_file = f"../training_data/performance_metrics_{model}_focal.json"

    # Create output directory for plots
    output_dir = Path(f"../training_data/training_plots_{model}/")
    output_dir.mkdir(exist_ok=True)

    print("Loading performance metrics...")
    data = load_metrics(metrics_file)

    print("Creating training loss plot...")
    plot_training_loss(
        data,
        ["train_losses", "val_losses_from_train_epoch"],
        output_dir / "training_loss.png",
    )

    print("Creating validation metrics plot...")
    plot_validation_metrics(data, output_dir / "validation_metrics.png")

    print("Creating pseudo-label distribution plots...")
    plot_pseudo_label_distributions(data, output_dir / "pseudo_label_distributions.png")

    # Create summary table
    create_summary_table(data)

    print(f"\nAll plots saved to: {output_dir}")
    print("Visualization complete!")


if __name__ == "__main__":
    main()
