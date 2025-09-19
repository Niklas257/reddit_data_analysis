import json
import os
from pathlib import Path


def create_constructive_dataset():
    """Create a dataset with only constructive conversations (prediction = 1)"""

    input_path = (
        "/home/niklas/reddit_project/training_data/reddit_train_annotated.jsonl"
    )
    output_path = (
        "/home/niklas/reddit_project/training_data/reddit_threads_constructive.jsonl"
    )

    print("Creating Constructive Reddit Conversations Dataset")
    print("=" * 55)

    if not os.path.exists(input_path):
        print(f"Input dataset not found: {input_path}")
        return False

    # File size info
    input_size_gb = os.path.getsize(input_path) / (1024 * 1024 * 1024)
    print(f"Input: {input_path}")
    print(f"Input size: {input_size_gb:.2f} GB")
    print(f"Output: {output_path}")

    print(f"\nProcessing dataset...")
    print("   - Filtering for prediction = 1.0 (constructive)")
    print("   - Keeping only 'sdid' and 'text' fields")

    total_samples = 0
    constructive_samples = 0

    try:
        with open(input_path, "r", encoding="utf-8") as input_file, open(
            output_path, "w", encoding="utf-8"
        ) as output_file:

            for line_num, line in enumerate(input_file, 1):
                try:
                    sample = json.loads(line.strip())
                    total_samples += 1

                    # Check if this is a constructive conversation
                    if sample.get("prediction") == 1.0:
                        constructive_samples += 1

                        # Create filtered sample with only sdid and text
                        filtered_sample = {
                            "sdid": sample["sdid"],
                            "text": sample["text"],
                        }

                        # Write to output file
                        output_file.write(
                            json.dumps(filtered_sample, ensure_ascii=False) + "\n"
                        )

                    # Progress indicator
                    if line_num % 100000 == 0:
                        percentage = (
                            (constructive_samples / total_samples) * 100
                            if total_samples > 0
                            else 0
                        )
                        print(
                            f"   Processed {line_num:,} lines... Found {constructive_samples:,} constructive ({percentage:.1f}%)"
                        )

                except json.JSONDecodeError:
                    print(f"JSON decode error at line {line_num}")
                    continue
                except KeyError as e:
                    print(f"Missing field {e} at line {line_num}")
                    continue

        # Final statistics
        print(f"\nDataset Creation Completed!")
        print(f"   Total input samples: {total_samples:,}")
        print(f"   Constructive samples: {constructive_samples:,}")
        print(
            f"   Constructive percentage: {(constructive_samples/total_samples)*100:.1f}%"
        )

        # Output file info
        if os.path.exists(output_path):
            output_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"   Output file size: {output_size_mb:.1f} MB")

            # Show a few sample entries
            print(f"\nSample entries from output:")
            with open(output_path, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i >= 3:  # Show first 3 samples
                        break
                    sample = json.loads(line.strip())
                    text_preview = (
                        sample["text"][:100] + "..."
                        if len(sample["text"]) > 100
                        else sample["text"]
                    )
                    print(f"   {i+1}. ID: {sample['sdid']}")
                    print(f"      Text: {text_preview}")
                    print()

        return True

    except Exception as e:
        print(f"Error processing dataset: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)  # Clean up partial file
        return False


def main():
    """Main function"""

    print("Reddit Constructive Conversations Extractor")
    print("=" * 50)
    print("Creating a clean dataset with only constructive conversations\n")

    success = create_constructive_dataset()

    print("\n" + "=" * 55)
    if success:
        print("SUCCESS: Constructive dataset created!")
        print("Your new dataset contains:")
        print("   - Only constructive conversations (prediction = 1)")
        print("   - Clean format with just 'sdid' and 'text' fields")
        print("   - Ready for research and model training")
        print("\nFile location:")
        print(
            "   /home/niklas/reddit_project/training_data/reddit_threads_constructive.jsonl"
        )
        print("\nNext steps:")
        print("   - Review the sample entries above")
        print("   - Use this dataset for training or research")
        print("   - Consider publishing this cleaned version")
    else:
        print("FAILED: Could not create constructive dataset")
        print("Please check the error messages above")


if __name__ == "__main__":
    main()
