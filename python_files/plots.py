import matplotlib.pyplot as plt
import json
import os
from matplotlib.ticker import ScalarFormatter
import re
from create_database import (
    sort_key,
)

saved_stats = "../data/saved_stats.json"
threads_table = "training_threads"

category_groups = {
    "author_distribution": {
        # Similar groupings for author_distribution stats
        "virality": [
            f"author_distribution_{threads_table}",
            f"author_distribution_{threads_table}_viral",
            f"author_distribution_{threads_table}_non_viral",
        ],
        "subreddits": [
            f"author_distribution_{threads_table}",
            f"author_distribution_AskReddit_{threads_table}",
            f"author_distribution_ACTrade_{threads_table}",
            f"author_distribution_memes_{threads_table}",
            f"author_distribution_wallstreetbets_{threads_table}",
            f"author_distribution_AnimalCrossing_{threads_table}",
        ],
    },
    "lengths": {
        # Similar groupings for lengths stats
        "virality": [
            f"thread_lengths_{threads_table}",
            f"thread_lengths_{threads_table}_viral",
            f"thread_lengths_{threads_table}_non_viral",
        ],
        "participants": [
            f"thread_lengths_{threads_table}",
            f"thread_lengths_{threads_table}_2_authors",
            f"thread_lengths_{threads_table}_3_authors",
            f"thread_lengths_{threads_table}_4_authors",
            f"thread_lengths_{threads_table}_5_authors",
        ],
        "subreddits": [
            f"thread_lengths_{threads_table}",
            f"thread_lengths_AskReddit_{threads_table}",
            f"thread_lengths_ACTrade_{threads_table}",
            f"thread_lengths_memes_{threads_table}",
            f"thread_lengths_wallstreetbets_{threads_table}",
            f"thread_lengths_AnimalCrossing_{threads_table}",
        ],
    },
    "summed_score": {
        # Similar groupings for score stats
        "virality": [
            f"thread_score_distribution_{threads_table}",
            f"thread_score_distribution_{threads_table}_viral",
            f"thread_score_distribution_{threads_table}_non_viral",
        ],
        "participants": [
            f"thread_score_distribution_{threads_table}",
            f"thread_score_distribution_{threads_table}_2_authors",
            f"thread_score_distribution_{threads_table}_3_authors",
            f"thread_score_distribution_{threads_table}_4_authors",
            f"thread_score_distribution_{threads_table}_5_authors",
        ],
        "subreddits": [
            f"thread_score_distribution_{threads_table}",
            f"thread_score_distribution_AskReddit_{threads_table}",
            f"thread_score_distribution_ACTrade_{threads_table}",
            f"thread_score_distribution_memes_{threads_table}",
            f"thread_score_distribution_wallstreetbets_{threads_table}",
            f"thread_score_distribution_AnimalCrossing_{threads_table}",
        ],
    },
}

# Make sure the plots directory exists
os.makedirs("../data/plots", exist_ok=True)

# Define a professional color palette with good distinguishability
PROFESSIONAL_COLORS = [
    "#1f77b4",  # muted blue
    "#ff7f0e",  # safety orange
    "#2ca02c",  # cooked asparagus green
    "#d62728",  # brick red
    "#9467bd",  # muted purple
    "#8c564b",  # chestnut brown
    "#e377c2",  # raspberry yogurt pink
    "#7f7f7f",  # middle gray
    "#bcbd22",  # curry yellow-green
    "#17becf",  # blue-teal
]

# Load data
with open(saved_stats, "r") as f:
    data = json.load(f)


def concise_legend_label(key):
    # For subreddit-specific keys
    subreddits = [
        "AskReddit",
        "ACTrade",
        "memes",
        "wallstreetbets",
        "AnimalCrossing",
    ]
    for sub in subreddits:
        if f"_{sub}_" in key or key.endswith(f"_{sub}_{threads_table}"):
            return sub
    # For keys like 'thread_lengths_{threads_table}_2_authors'
    if "_authors" in key:
        match = re.search(r"(\d+)_authors", key)
        if match:
            return f"{match.group(1)} Authors"
    elif "_non_viral" in key:
        return "Non-Viral"
    elif "_viral" in key:
        return "Viral"
    # Special case for 'threads' only (not ending with _authors)
    elif key.endswith(f"_{threads_table}") and not re.search(r"_\d+_authors", key):
        return "All Threads"
    # Fallback: prettify the last part
    return key.replace("_", " ").title()


def process_numerical_xaxis(x, y, key):
    """Process numerical x-axis values to ensure complete range"""
    try:
        # Convert x values to integers
        x_int = [int(k) for k in x]

        # For score data, filter to desired range
        if "score" in key:
            filtered = [(xi, yi) for xi, yi in zip(x_int, y) if -50 <= xi <= 400]
            x_int = [xi for xi, _ in filtered]
            y = [yi for _, yi in filtered]
        elif "lengths" in key:
            filtered = [(xi, yi) for xi, yi in zip(x_int, y) if xi <= 50]
            x_int = [xi for xi, _ in filtered]
            y = [yi for _, yi in filtered]

        # Create complete range
        min_x, max_x = min(x_int), max(x_int)
        full_range = range(min_x, max_x + 1)

        # Create new y values with zeros for missing x values
        value_dict = dict(zip(x_int, y))
        new_y = [value_dict.get(xi, 0) for xi in full_range]

        return [str(xi) for xi in full_range], new_y
    except ValueError:
        return x, y


# Process each statistic group
for category, groups in category_groups.items():
    for group_name, keys in groups.items():
        # Check if we have any of these keys in our data
        available_keys = [k for k in keys if k in data]
        if not available_keys:
            continue

        # Create figure for this group
        plt.figure(figsize=(12, 8))

        # To track legend entries
        legend_handles = []
        legend_labels = []

        # For numerical x-axis, find the maximum range across all datasets
        all_x_values = set()
        is_numerical = True

        # First pass to determine if all x values are numerical and collect all unique x values
        for key in available_keys:
            values = data[key]
            if not isinstance(values, dict):
                continue

            x = list(values.keys())
            if not all(k.lstrip("-").isdigit() for k in x):
                is_numerical = False
                break

            # If numerical, collect all x values
            if is_numerical:
                x_int = [int(k) for k in x]

                # Apply appropriate filtering based on category
                if "score" in key:
                    x_int = [xi for xi in x_int if -100 <= xi <= 400]
                elif "lengths" in key:
                    # For lengths, ensure we start at 1 (no thread has length 0)
                    x_int = [xi for xi in x_int if 1 <= xi <= 50]
                elif "author_distribution" in key:
                    # For author distribution, ensure we start at 1 (no thread has 0 authors)
                    x_int = [xi for xi in x_int if 1 <= xi <= 30]

                all_x_values.update(x_int)

        # If all datasets have numerical x values, create a unified x-axis
        unified_x_axis = None
        if is_numerical and all_x_values:
            min_x = min(all_x_values)
            max_x = max(all_x_values)

            # Ensure we start at 1 for thread_lengths and author_distribution
            if category == "lengths" or category == "author_distribution":
                # Force min_x to be 1 for these categories
                min_x = max(1, min_x)
                unified_x_axis = range(min_x, max_x + 1)
            elif category == "summed_score":
                # For score, create the correct range without shift
                unified_x_axis = range(min_x, max_x + 1)
            else:
                unified_x_axis = range(min_x, max_x + 1)

            unified_x_str = [str(xi) for xi in unified_x_axis]

        # Process each statistic in this group
        for i, key in enumerate(available_keys):
            values = data[key]

            # Skip non-dictionary values
            if not isinstance(values, dict):
                continue

            x = list(values.keys())
            y = list(values.values())

            # Process numerical x-axis
            if all(k.lstrip("-").isdigit() for k in x) and unified_x_axis is not None:
                # Convert to integers for processing
                x_int = [int(k) for k in x]

                # Create a dictionary mapping x values to y values
                value_dict = dict(zip(x_int, y))

                # Use unified x-axis with zeros for missing values
                x = unified_x_str
                y = [value_dict.get(xi, 0) for xi in unified_x_axis]
            else:
                # For non-numerical keys, use original processing
                if "subreddit" in key:
                    x = x[:10]
                    y = y[:10]
                elif "score" in key:
                    pass
                else:
                    x = sorted(x, key=sort_key)[:50]
                    y = [values[k] for k in x]

            # Check if values are numerical
            try:
                y = [float(v) for v in y]
            except (ValueError, TypeError):
                print(f"Skipping {key} - values are not numerical")
                continue

            # If a y is negative, set it to 0
            y = [max(0, v) for v in y]

            # Create line plot with professional color
            color = PROFESSIONAL_COLORS[i % len(PROFESSIONAL_COLORS)]

            # For thread_lengths and author_distribution, adjust the x-axis positions
            if (
                "lengths" in key or "author_distribution" in key
            ) and unified_x_axis is not None:
                # Use positions that match the actual x values (no extra space at beginning)
                x_positions = range(len(y))
                (line,) = plt.plot(
                    x_positions,
                    y,
                    color=color,
                    linestyle="-",
                    linewidth=2,
                    markersize=6,
                    alpha=0.8,
                    label=concise_legend_label(key),
                )
            # Special handling for score plots to fix alignment
            elif "score" in key and unified_x_axis is not None:
                # Use actual x values for positioning instead of indices
                # This ensures -50 appears at the left edge
                (line,) = plt.plot(
                    unified_x_axis,  # Use actual x values instead of indices
                    y,
                    color=color,
                    linestyle="-",
                    linewidth=2,
                    markersize=6,
                    alpha=0.8,
                    label=concise_legend_label(key),
                )
            else:
                # For other plots, use the standard approach
                (line,) = plt.plot(
                    range(len(x)) if unified_x_axis is None else unified_x_axis,
                    y,
                    color=color,
                    linestyle="-",
                    linewidth=2,
                    markersize=6,
                    alpha=0.8,
                    label=concise_legend_label(key),
                )

            # Add to legend
            legend_handles.append(line)
            legend_labels.append(concise_legend_label(key))

        # Customize plot
        # plt.title(
        #     f"{category.replace('_', ' ').capitalize()} - {group_name.replace('_', ' ').capitalize()}",
        #     fontsize=25,
        #     pad=20,
        # )
        # Set descriptive axis labels
        if category == "summed_score" or (isinstance(key, str) and "score" in key):
            x_label = "Summed score"
        elif category == "author_distribution" or (
            isinstance(key, str) and "author_distribution" in key
        ):
            x_label = "Number of authors"
        elif category == "lengths" or (isinstance(key, str) and "lengths" in key):
            x_label = "Lengths"
        elif isinstance(key, str) and "subreddit" in key:
            x_label = "Subreddits"
        else:
            x_label = "Categories"

        # Y label: log scale or not
        ax = plt.gca()
        y_label = "Count (log scale)"

        plt.xlabel(x_label, fontsize=25)
        plt.yscale("log")
        plt.ylabel(y_label, fontsize=25)
        plt.yticks(fontsize=25)
        plt.gca().yaxis.offsetText.set_fontsize(25)
        plt.grid(False)
        # Remove right and top spines
        ax.spines["right"].set_visible(False)
        ax.spines["top"].set_visible(False)

        # Set x-ticks based on the data
        if unified_x_axis is not None:
            # Special handling for thread_lengths plots
            if category == "lengths":
                # Use positions that match the actual x values, starting at 1
                tick_positions = []
                tick_labels = []

                # Add ticks at reasonable intervals
                for i, val in enumerate(unified_x_axis):
                    if val == 1 or val % 5 == 0 or i == len(unified_x_axis) - 1:
                        tick_positions.append(i)
                        tick_labels.append(str(val))

                plt.xticks(
                    tick_positions,
                    tick_labels,
                    ha="right",
                    fontsize=25,
                )
                plt.xlim(min(tick_positions), max(tick_positions))
            # Special handling for author_distribution plots with ticks at 5, 10, 15, etc.
            elif category == "author_distribution":
                # Use positions that match the actual x values, starting at 1
                tick_positions = []
                tick_labels = []

                # Add tick at 1 and then at multiples of 5
                for i, val in enumerate(unified_x_axis):
                    if val == 1 or val % 5 == 0 or i == len(unified_x_axis) - 1:
                        tick_positions.append(i)
                        tick_labels.append(str(val))

                plt.xticks(
                    tick_positions,
                    tick_labels,
                    ha="right",
                    fontsize=25,
                )
                plt.xlim(min(tick_positions), max(tick_positions))
            # Special handling for score plots to fix the shifted ticks
            elif category == "summed_score":
                # For score plots, use the actual x values for ticks
                # This ensures -50 appears at the left edge and values are properly spaced
                tick_positions = []
                tick_labels = []

                # Add ticks at key points using the actual values
                important_values = [-100, 0, 100, 200, 300, 400]
                for val in important_values:
                    if val >= min(unified_x_axis) and val <= max(unified_x_axis):
                        tick_positions.append(val)
                        tick_labels.append(str(val))

                plt.xticks(
                    tick_positions,
                    tick_labels,
                    ha="right",
                    fontsize=25,
                )
                plt.xlim(min(unified_x_axis) - 5, max(unified_x_axis))
                plt.gca().tick_params(axis="x", pad=15)
                plt.yticks(fontsize=25)

                # Set the x-axis limits to match the data range
                plt.xlim(min(unified_x_axis), max(unified_x_axis))
            else:
                # For other numerical x-axis with unified range
                step = max(1, len(unified_x_axis) // 10)  # Show about 10 ticks
                tick_positions = list(range(0, len(unified_x_axis), step))
                tick_labels = [
                    str(unified_x_axis[i])
                    for i in tick_positions
                    if i < len(unified_x_axis)
                ]
                plt.xticks(
                    tick_positions,
                    tick_labels,
                    ha="right",
                    fontsize=25,
                )
        else:
            # For non-numerical x-axis
            plt.xticks(range(len(x)), x, rotation=45, ha="right", fontsize=25)

        # Add legend
        plt.legend(
            handles=legend_handles,
            labels=legend_labels,
            loc="best",
            fontsize=22,
            framealpha=1,
        )

        plt.tight_layout()

        # Save plot
        safe_group_name = "".join(c if c.isalnum() else "_" for c in group_name)
        output_path = f"../data/plots/{category}_{safe_group_name}.png"
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

# Process remaining statistics that don't fit into our groups
for key, values in data.items():
    # Skip if we've already processed this key in our groups
    processed = False
    for category, groups in category_groups.items():
        for group_keys in groups.values():
            if (
                key in group_keys
                and key
                not in [
                    f"depth_distribution_{threads_table}",
                    f"author_distribution_{threads_table}",
                    f"thread_lengths_{threads_table}",
                    f"thread_score_distribution_{threads_table}",
                ]
                or "depth" in key
                or "lookup" in key
            ):
                processed = True
                break
        if processed:
            break
    if processed:
        continue

    # Handle non-dictionary values
    if not isinstance(values, dict):
        print(f"{key}: {values}")
        continue

    # Handle dictionaries with only one value
    if len(values) == 1 or "variance" in key:
        print(f"{key}: {values}")
        continue

    # Prepare figure for individual stat
    plt.figure(figsize=(12, 8))

    x = list(values.keys())
    y = list(values.values())

    # Process numerical x-axis
    if f"thread_score_distribution_{threads_table}" in key:
        x_int = [int(k) for k in x]
        filtered = [(xi, yi) for xi, yi in zip(x_int, y) if -50 <= xi <= 50]

        # Sort by x value to ensure proper order
        filtered.sort(key=lambda pair: pair[0])

        x = [xi for xi, _ in filtered]
        y = [yi for _, yi in filtered]

        # Create bar plot using actual x values for positioning
        bars = plt.bar(x, y, color=PROFESSIONAL_COLORS[0], alpha=0.7)

        # Set x-ticks to the actual values with ticks at key points
        tick_positions = []
        tick_labels = []

        # Add ticks at key points
        important_values = [-50, -25, 0, 25, 50]
        for val in important_values:
            if val >= min(x) and val <= max(x):
                tick_positions.append(val)
                tick_labels.append(str(val))

        plt.xticks(tick_positions, tick_labels, ha="right", fontsize=25)

        # Set the x-axis limits to match the data range
        plt.xlim(min(x), max(x))
    elif f"thread_lengths_{threads_table}" in key:
        x_int = [int(k) for k in x]
        # Filter values and ensure we start at 1 (no thread has length 0)
        filtered = [(xi, yi) for xi, yi in zip(x_int, y) if 1 <= xi <= 10]

        # Sort by x value to ensure proper order
        filtered.sort(key=lambda pair: pair[0])

        x = [xi for xi, _ in filtered]
        y = [yi for _, yi in filtered]
    elif f"author_distribution_{threads_table}" in key:
        x_int = [int(k) for k in x]
        # Filter values and ensure we start at 1 (no thread has 0 authors)
        filtered = [(xi, yi) for xi, yi in zip(x_int, y) if 1 <= xi <= 10]

        # Sort by x value to ensure proper order
        filtered.sort(key=lambda pair: pair[0])

        x = [xi for xi, _ in filtered]
        y = [yi for _, yi in filtered]
    elif all(k.lstrip("-").isdigit() for k in x):
        x, y = process_numerical_xaxis(x, y, key)
    elif "subreddit" in key:
        x = x[:10]
        y = y[:10]

    # Check if values are numerical
    try:
        y = [float(v) for v in y]
    except (ValueError, TypeError):
        print(f"Skipping {key} - values are not numerical")
        continue

    # If a y is negative, set it to 0
    y = [max(0, v) for v in y]

    # Create bar plot for individual stats (keeping as bars for single distributions)
    if f"thread_lengths_{threads_table}" in key:
        # For thread lengths, use integers for x positions
        x_positions = range(len(x))
        bars = plt.bar(x_positions, y, color=PROFESSIONAL_COLORS[0], alpha=0.7)

        # Set x-ticks to the actual values with ticks at 1 and multiples of 5
        tick_positions = []
        tick_labels = []
        for i, val in enumerate(x):
            if int(val) == 1 or int(val) % 1 == 0 or i == len(x) - 1:
                tick_positions.append(i)
                tick_labels.append(str(val))

        plt.xticks(tick_positions, tick_labels, ha="right", fontsize=25)
        plt.gca().tick_params(axis="x", pad=10)

        plt.gca().yaxis.set_major_formatter(ScalarFormatter(useMathText=True))
        plt.ticklabel_format(style="sci", axis="y")
        # plt.yticks([0, 1e7, 2e7, 3e7, 4e7], fontsize=25)
        plt.gca().yaxis.offsetText.set_fontsize(25)
    elif f"author_distribution_{threads_table}" in key:
        # For author distribution, use integers for x positions
        x_positions = range(len(x))
        bars = plt.bar(x_positions, y, color=PROFESSIONAL_COLORS[0], alpha=0.7)

        # Set x-ticks to the actual values with ticks at 1 and multiples of 5
        tick_positions = []
        tick_labels = []
        for i, val in enumerate(x):
            if int(val) == 1 or int(val) % 1 == 0 or i == len(x) - 1:
                tick_positions.append(i)
                tick_labels.append(str(val))

        plt.xticks(tick_positions, tick_labels, ha="right", fontsize=25)
        plt.gca().tick_params(axis="x", pad=10)

        plt.gca().yaxis.set_major_formatter(ScalarFormatter(useMathText=True))
        plt.ticklabel_format(style="sci", axis="y")
        # plt.yticks([0, 1e7, 2e7, 3e7, 4e7], fontsize=25)
        plt.gca().yaxis.offsetText.set_fontsize(25)
    elif f"thread_score_distribution_{threads_table}" in key:
        # For score distribution, use actual x values
        x_int = [int(k) for k in x]
        filtered = [(xi, yi) for xi, yi in zip(x_int, y) if -10 <= xi <= 30]

        # Sort by x value to ensure proper order
        filtered.sort(key=lambda pair: pair[0])

        x = [xi for xi, _ in filtered]
        y = [yi for _, yi in filtered]

        # Create bar plot using actual x values for positioning
        bars = plt.bar(x, y, color=PROFESSIONAL_COLORS[0], alpha=0.7)

        # Set x-ticks to the actual values with ticks at key points
        tick_positions = []
        tick_labels = []

        # Add ticks at key points
        important_values = [-10, 0, 10, 20, 30]
        for val in important_values:
            if val >= min(x) and val <= max(x):
                tick_positions.append(val)
                tick_labels.append(str(val))

        plt.xticks(tick_positions, tick_labels, ha="right", fontsize=25)
        plt.gca().tick_params(axis="x", pad=10)

        plt.gca().yaxis.set_major_formatter(ScalarFormatter(useMathText=True))
        plt.ticklabel_format(style="sci", axis="y")
        # plt.yticks([0, 0.5e7, 1e7, 1.5e7, 2e7], fontsize=25)
        plt.gca().yaxis.offsetText.set_fontsize(25)
        plt.xlim(min(tick_positions), max(tick_positions))
    else:
        bars = plt.bar(x, y, color=PROFESSIONAL_COLORS[0], alpha=0.7)

        # Set x-ticks
        if all(str(k).isdigit() for k in x) or all(
            str(k).lstrip("-").isdigit() for k in x
        ):
            step = max(1, len(x) // 10)
            plt.xticks(
                [x[i] for i in range(0, len(x), step)],
                ha="right",
                fontsize=25,
            )
        else:
            plt.xticks(rotation=45, ha="right", fontsize=25)
            plt.yticks(fontsize=25)
            plt.gca().yaxis.offsetText.set_fontsize(25)

    # Customize plot
    # plt.title(key.replace("_", " ").title(), fontsize=25)
    # if key == f"thread_score_distribution_{threads_table}":
    #     plt.title("Summed score - Unspecific Threads", fontsize=25)
    if key == f"thread_lengths_{threads_table}":
        plt.xlabel("Lengths", fontsize=25)
    elif key == f"author_distribution_{threads_table}":
        plt.xlabel("Number of authors", fontsize=25)
    elif key == f"thread_score_distribution_{threads_table}":
        plt.xlabel("Summed score", fontsize=25)
    elif "subreddit_distribution" in key:
        plt.xlabel("Subreddits", fontsize=25)
    else:
        plt.xlabel("Categories", fontsize=25)
    plt.ylabel("Count", fontsize=25)
    plt.grid(False)
    # Remove right and top spines
    ax = plt.gca()
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    plt.tight_layout()

    # Save plot (using original naming convention)
    safe_key = "".join(c if c.isalnum() else "_" for c in key)
    output_path = f"../data/plots/{safe_key}.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

print("All visualizations complete!")
