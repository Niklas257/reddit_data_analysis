import json
from textwrap import fill


def display_thread(line_number):
    try:
        # Read the specified line from the JSONL file
        with open("random_threads.jsonl", "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if i == line_number:
                    thread_data = json.loads(line)
                    break
            else:
                print(f"Error: Line {line_number} not found in the file.")
                return

        # Display the thread data with nice formatting
        print("Thread details:")

        # Count comments by counting non-None metadata dictionaries
        comment_count = sum(
            1 for value in thread_data.values() if isinstance(value, dict)
        )
        print(f"This thread contains 1 post with {comment_count - 1} comments\n")

        for col, value in thread_data.items():
            if value is None:
                continue

            if isinstance(value, dict):
                # This is a metadata dictionary
                print(f"\nFull information for {col}:")
                for metadata_key, metadata_value in value.items():
                    if metadata_value is None:
                        print(f"{metadata_key}: NULL")
                    elif isinstance(metadata_value, str) and len(metadata_value) > 80:
                        print(f"{metadata_key}:")
                        wrapped_text = fill(
                            metadata_value,
                            width=100,
                            initial_indent="    ",
                            subsequent_indent="    ",
                        )
                        print(wrapped_text)
                        print()
                    else:
                        print(f"{metadata_key}: {metadata_value}")
                print("-" * 80)
            else:
                # This is a direct value
                print(f"{col}: {value}")

    except FileNotFoundError:
        print(
            "Error: random_threads.jsonl not found. Please run create_thread_json.py first."
        )
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in the file.")


if __name__ == "__main__":
    while True:
        try:
            line_num = int(input("Enter a number between 1 and 100 (or 0 to exit): "))
            if line_num == 0:
                break
            if 1 <= line_num <= 100:
                display_thread(line_num)
                break
            else:
                print("Please enter a number between 1 and 100.")
        except ValueError:
            print("Please enter a valid number.")
