import json
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from sklearn.utils import resample  # Used for random oversampling
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    get_linear_schedule_with_warmup,
)
import numpy as np
import random
import os
import matplotlib.pyplot as plt
import torch.nn as nn  # Added for CrossEntropyLoss with weights

# --- Configuration ---
MODEL_NAME = "allenai/longformer-base-4096"
MAX_LEN = 4096  # Maximum token length for Longformer
BATCH_SIZE = 8
LEARNING_RATE = 2e-5
EPOCHS = 10
RANDOM_SEED = 42
EARLY_STOPPING_PATIENCE = 3

# Set random seeds for reproducibility across runs
torch.manual_seed(RANDOM_SEED)
torch.cuda.manual_seed_all(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)
torch.backends.cudnn.deterministic = True
torch.backends.cudnn.benchmark = False

# Set device (GPU if available, else CPU)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# --- Helper Functions ---


def load_jsonl(file_path, tokenizer, max_len):
    """
    Loads JSONL data from a file into a pandas DataFrame.
    Filters out entries whose tokenized 'text' exceeds max_len.
    """
    data = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line))
    except FileNotFoundError:
        print(
            f"Error: JSONL data file not found at {file_path}. Please check the path."
        )
        return pd.DataFrame()

    df = pd.DataFrame(data)
    initial_count = len(df)
    print(
        f"Initially loaded {initial_count} entries from {os.path.basename(file_path)}."
    )

    # Calculate token lengths and filter
    print(
        f"Filtering entries longer than {max_len} tokens in {os.path.basename(file_path)}..."
    )
    # This might be slow for very large datasets and large MAX_LEN
    # Consider batching tokenization for performance if needed in actual use.
    token_lengths = [
        len(
            tokenizer.encode_plus(str(text), add_special_tokens=True, truncation=False)[
                "input_ids"
            ]
        )
        for text in df["text"]
    ]
    df["token_length"] = token_lengths

    df_filtered = df[df["token_length"] <= max_len].copy()
    filtered_count = len(df_filtered)

    print(
        f"Filtered out {initial_count - filtered_count} entries from {os.path.basename(file_path)} due to length > {max_len} tokens."
    )
    print(
        f"Remaining entries after length filtering in {os.path.basename(file_path)}: {filtered_count}."
    )

    return df_filtered.drop(columns=["token_length"])


# --- Custom PyTorch Dataset ---
class CommentDataset(Dataset):
    """
    A custom PyTorch Dataset for handling text and labels for classification.
    It tokenizes text using the provided tokenizer and pads/truncates to max_len.
    """

    def __init__(self, texts, labels, tokenizer, max_len):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = str(self.texts[idx])
        label = self.labels[idx]

        encoding = self.tokenizer.encode_plus(
            text,
            add_special_tokens=True,
            max_length=self.max_len,
            return_token_type_ids=False,
            padding="max_length",
            return_attention_mask=True,
            return_tensors="pt",
            truncation=True,
        )

        return {
            "text": text,
            "input_ids": encoding["input_ids"].flatten(),
            "attention_mask": encoding["attention_mask"].flatten(),
            "labels": torch.tensor(label, dtype=torch.long),
        }


# --- Training and Evaluation Functions ---


def train_epoch(
    model,
    train_data_loader,
    dev_data_loader,
    optimizer,
    scheduler,
    device,
    epoch,
    total_epochs,
    eval_every_steps=20,
    class_weights=None,  # New parameter to accept class weights
):
    """Performs one training epoch with in-epoch evaluation."""
    model.train()
    losses = []
    correct_predictions = 0

    # Store metrics for plotting later
    train_losses_batch = []
    train_accuracies_batch = []
    dev_losses_batch = []
    dev_accuracies_batch = []
    dev_f1_batch = []

    # Keep track of total samples processed for accurate in-epoch accuracy
    total_samples_processed_in_epoch = 0

    # Define loss function with class weights if provided
    loss_fct = nn.CrossEntropyLoss(
        weight=class_weights.to(device) if class_weights is not None else None
    )

    for step, batch in enumerate(train_data_loader):
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["labels"].to(device)

        # Forward pass: get logits
        outputs = model(
            input_ids=input_ids, attention_mask=attention_mask
        )  # Removed labels from here to compute loss manually
        logits = outputs.logits

        # Compute loss using defined loss_fct
        loss = loss_fct(logits, labels)  # Now computes with class_weights

        losses.append(loss.item())

        _, preds = torch.max(logits, dim=1)
        correct_predictions += torch.sum(preds == labels)
        total_samples_processed_in_epoch += labels.size(
            0
        )  # Add batch size to total processed samples

        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        scheduler.step()
        optimizer.zero_grad()

        # In-epoch evaluation
        if (step + 1) % eval_every_steps == 0 or step == len(train_data_loader) - 1:
            print(
                f"  Epoch {epoch + 1}/{total_epochs} | Step {step + 1}/{len(train_data_loader)} - Train Loss: {np.mean(losses[-eval_every_steps:]):.4f}"
            )

            # Evaluate on development set
            dev_loss, dev_acc, dev_precision, dev_recall, dev_f1, _, _ = evaluate_model(
                model,
                dev_data_loader,
                device,
                class_weights,  # Pass weights for eval too
            )

            print(
                f"    -> Dev Loss: {dev_loss:.4f}, Dev Accuracy: {dev_acc:.4f}, Dev F1-score: {dev_f1:.4f}"
            )

            train_losses_batch.append(
                np.mean(losses)
            )  # Still mean of all losses so far
            # Calculate accuracy based on samples processed SO FAR in this epoch
            train_accuracies_batch.append(
                (correct_predictions.double() / total_samples_processed_in_epoch).item()
            )

            dev_losses_batch.append(dev_loss)
            dev_accuracies_batch.append(dev_acc.item())
            dev_f1_batch.append(dev_f1)

            model.train()  # Set model back to training mode after evaluation

    # Return epoch-level averages for summary, and lists for plotting
    return (
        np.mean(losses),
        correct_predictions.double() / len(train_data_loader.dataset),
        train_losses_batch,
        train_accuracies_batch,
        dev_losses_batch,
        dev_accuracies_batch,
        dev_f1_batch,
    )


def evaluate_model(
    model, data_loader, device, class_weights=None
):  # New parameter to accept class weights
    """Evaluates the model on a given data loader."""
    model.eval()
    losses = []
    correct_predictions = 0
    all_labels = []
    all_preds = []

    # Define loss function with class weights if provided
    loss_fct = nn.CrossEntropyLoss(
        weight=class_weights.to(device) if class_weights is not None else None
    )

    with torch.no_grad():
        for batch in data_loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            outputs = model(
                input_ids=input_ids, attention_mask=attention_mask
            )  # Removed labels from here
            logits = outputs.logits

            loss = loss_fct(logits, labels)  # Now computes with class_weights
            losses.append(loss.item())

            _, preds = torch.max(logits, dim=1)
            correct_predictions += torch.sum(preds == labels)

            all_labels.extend(labels.cpu().numpy())
            all_preds.extend(preds.cpu().numpy())

    avg_loss = np.mean(losses)
    accuracy = correct_predictions.double() / len(data_loader.dataset)

    # Use zero_division parameter for precision_recall_fscore_support to handle cases where no samples of a class are predicted
    precision, recall, f1, _ = precision_recall_fscore_support(
        all_labels, all_preds, average="binary", labels=[0, 1], zero_division=0
    )

    return avg_loss, accuracy, precision, recall, f1, all_labels, all_preds


def plot_metrics(
    train_losses, dev_losses, train_accuracies, dev_accuracies, title_suffix="Epochs"
):
    """Plots training and development loss and accuracy."""
    plt.figure(figsize=(12, 5))

    plt.subplot(1, 2, 1)
    plt.plot(train_losses, label="Train Loss")
    plt.plot(dev_losses, label="Dev Loss")
    plt.title(f"Loss over {title_suffix}")
    plt.xlabel(title_suffix)
    plt.ylabel("Loss")
    plt.legend()
    plt.grid(True)

    plt.subplot(1, 2, 2)
    plt.plot(train_accuracies, label="Train Accuracy")
    plt.plot(dev_accuracies, label="Dev Accuracy")
    plt.title(f"Accuracy over {title_suffix}")
    plt.xlabel(title_suffix)
    plt.ylabel("Accuracy")
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.show()


# --- Main Script ---


def training():
    ynacc_file_path = "/kaggle/input/ynacc-processed/ynacc_processed.jsonl"
    iac_file_path = "/kaggle/input/iac-processed/iac_processed.jsonl"

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    # --- Load Data ---
    print("Loading YNACC data...")
    df_ynacc_raw = load_jsonl(ynacc_file_path, tokenizer, MAX_LEN)
    print("Loading IAC data...")
    df_iac_raw = load_jsonl(iac_file_path, tokenizer, MAX_LEN)

    if df_ynacc_raw.empty or df_iac_raw.empty:
        print(
            "Exiting: One or both datasets could not be loaded or are empty after filtering."
        )
        return

    # --- Separate Test Sets (100 samples each, not mixed) ---
    print("\n--- Splitting Test Sets ---")
    # Stratify by label to ensure class balance in test sets
    df_ynacc_train_val, df_test_ynacc = train_test_split(
        df_ynacc_raw,
        test_size=100,
        random_state=RANDOM_SEED,
        stratify=df_ynacc_raw["label"],
    )
    df_iac_train_val, df_test_iac = train_test_split(
        df_iac_raw,
        test_size=100,
        random_state=RANDOM_SEED,
        stratify=df_iac_raw["label"],
    )

    print(f"YNACC data for training/validation: {len(df_ynacc_train_val)} samples.")
    print(
        f"YNACC test data: {len(df_test_ynacc)} samples (Label distribution: {df_test_ynacc['label'].value_counts().to_dict()})."
    )
    print(f"IAC data for training/validation: {len(df_iac_train_val)} samples.")
    print(
        f"IAC test data: {len(df_test_iac)} samples (Label distribution: {df_test_iac['label'].value_counts().to_dict()})."
    )

    # --- Balance IAC Training/Validation Data using Random Oversampling ---
    # Note: For true SMOTE (Synthetic Minority Over-sampling Technique),
    # you would typically need to vectorize text (e.g., using embeddings)
    # before applying SMOTE, as it operates on numerical features.
    # This implementation uses random oversampling (duplicating existing samples
    # from the minority class) as a simpler and common approach to balance
    # text datasets directly within a DataFrame.
    print("\n--- Balancing IAC Training/Validation Data ---")
    df_iac_minority = df_iac_train_val[
        df_iac_train_val["label"] == 0
    ]  # Assuming 0 is the minority class based on your previous description
    df_iac_majority = df_iac_train_val[
        df_iac_train_val["label"] == 1
    ]  # Assuming 1 is the majority class

    if (
        len(df_iac_minority) > 0
        and len(df_iac_majority) > 0
        and len(df_iac_minority) < len(df_iac_majority)
    ):
        # Oversample minority class to match majority class count
        df_iac_minority_oversampled = resample(
            df_iac_minority,
            replace=True,  # sample with replacement
            n_samples=len(df_iac_majority),  # to match majority class
            random_state=RANDOM_SEED,
        )
        df_iac_train_val_balanced = pd.concat(
            [df_iac_majority, df_iac_minority_oversampled]
        )
        print(
            f"IAC after random oversampling: {len(df_iac_train_val_balanced)} samples."
        )
        print("IAC balanced training/validation label distribution:")
        print(df_iac_train_val_balanced["label"].value_counts().to_dict())
    else:
        df_iac_train_val_balanced = df_iac_train_val
        print(
            "IAC is already balanced or minority class is not less than majority, no oversampling applied."
        )
        print("IAC training/validation label distribution:")
        print(df_iac_train_val_balanced["label"].value_counts().to_dict())

    # --- Combine YNACC and Balanced IAC Data for Unified Training/Validation ---
    df_combined_train_val = (
        pd.concat([df_ynacc_train_val, df_iac_train_val_balanced])
        .sample(frac=1, random_state=RANDOM_SEED)
        .reset_index(drop=True)
    )  # Shuffle the combined data to mix entries from both datasets

    print(
        f"\nCombined training/validation data size: {len(df_combined_train_val)} samples."
    )
    print("Combined training/validation data label distribution:")
    print(df_combined_train_val["label"].value_counts().to_dict())

    # Calculate class weights for the combined training/validation set
    class_counts = df_combined_train_val["label"].value_counts().sort_index()
    num_classes = len(class_counts)
    if num_classes > 0:
        total_samples = sum(class_counts)
        # Weights are inversely proportional to class frequencies to give more importance to underrepresented classes
        weights = [total_samples / (num_classes * count) for count in class_counts]
        class_weights = torch.tensor(weights, dtype=torch.float)
        print(
            f"Calculated Class Weights for combined training/validation: {class_weights.tolist()}"
        )
    else:
        class_weights = None
        print(
            "Warning: Cannot calculate class weights (no classes found in combined data)."
        )

    # --- Split Combined Data into Training and Development (Validation) Sets ---
    # User wants 100 samples for evaluation (dev set) from the combined, balanced dataset
    # Ensure there's enough data for 100 samples and stratify the split
    dev_test_size = 100
    if len(df_combined_train_val) <= dev_test_size:
        print(
            f"Warning: Not enough combined data ({len(df_combined_train_val)}) to create a {dev_test_size}-sample dev set. Adjusting test_size to 0.1 of available data."
        )
        df_train, df_dev = train_test_split(
            df_combined_train_val,
            test_size=0.1,
            random_state=RANDOM_SEED,
            stratify=df_combined_train_val["label"],
        )
    else:
        df_train, df_dev = train_test_split(
            df_combined_train_val,
            test_size=dev_test_size,
            random_state=RANDOM_SEED,
            stratify=df_combined_train_val["label"],
        )

    print(f"\nFinal Training entries: {len(df_train)} samples.")
    print(f"Final Development (Validation) entries: {len(df_dev)} samples.")

    # Final check for empty splits
    if (
        len(df_train) == 0
        or len(df_dev) == 0
        or len(df_test_ynacc) == 0
        or len(df_test_iac) == 0
    ):
        print(
            "Error: One or more final data splits are empty. Please check data loading/splitting logic and dataset sizes."
        )
        return  # Exit if no data to train/evaluate on

    # --- Create PyTorch Datasets and DataLoaders ---
    print("\n--- Creating DataLoaders ---")
    train_dataset = CommentDataset(
        df_train["text"].tolist(), df_train["label"].tolist(), tokenizer, MAX_LEN
    )
    dev_dataset = CommentDataset(
        df_dev["text"].tolist(), df_dev["label"].tolist(), tokenizer, MAX_LEN
    )
    test_ynacc_dataset = CommentDataset(
        df_test_ynacc["text"].tolist(),
        df_test_ynacc["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    test_iac_dataset = CommentDataset(
        df_test_iac["text"].tolist(), df_test_iac["label"].tolist(), tokenizer, MAX_LEN
    )

    train_dataloader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    dev_dataloader = DataLoader(dev_dataset, batch_size=BATCH_SIZE)
    test_ynacc_dataloader = DataLoader(test_ynacc_dataset, batch_size=BATCH_SIZE)
    test_iac_dataloader = DataLoader(test_iac_dataset, batch_size=BATCH_SIZE)

    print(f"Train DataLoader batches: {len(train_dataloader)}")
    print(f"Dev DataLoader batches: {len(dev_dataloader)}")
    print(f"YNACC Test DataLoader batches: {len(test_ynacc_dataloader)}")
    print(f"IAC Test DataLoader batches: {len(test_iac_dataloader)}")

    # --- Model Initialization ---
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2)
    model.to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)
    total_steps = len(train_dataloader) * EPOCHS
    scheduler = get_linear_schedule_with_warmup(
        optimizer, num_warmup_steps=0, num_training_steps=total_steps
    )

    # Lists to store metrics for plotting per epoch
    train_losses_per_epoch = []
    dev_losses_per_epoch = []
    train_accuracies_per_epoch = []
    dev_accuracies_per_epoch = []

    # Lists to store metrics for plotting per batch (in-epoch)
    train_losses_per_batch_step = []
    train_accuracies_per_batch_step = []
    dev_losses_per_batch_step = []
    dev_accuracies_per_batch_step = []
    dev_f1_per_batch_step = []

    print("\n--- Starting Training Loop ---")
    best_dev_f1 = -1
    epochs_no_improve = 0
    model_save_path = None

    for epoch in range(EPOCHS):
        print(f"\n--- Epoch {epoch + 1}/{EPOCHS} ---")

        # Pass class_weights to train_epoch
        (
            current_train_loss,
            current_train_acc,
            batch_train_losses,
            batch_train_accuracies,
            batch_dev_losses,
            batch_dev_accuracies,
            batch_dev_f1s,
        ) = train_epoch(
            model,
            train_dataloader,
            dev_dataloader,
            optimizer,
            scheduler,
            device,
            epoch,
            EPOCHS,
            class_weights=class_weights,  # Pass class weights
        )

        # Store epoch-level metrics
        train_losses_per_epoch.append(current_train_loss)
        train_accuracies_per_epoch.append(current_train_acc.item())

        # Store batch-level metrics for in-epoch plots
        train_losses_per_batch_step.extend(batch_train_losses)
        train_accuracies_per_batch_step.extend(batch_train_accuracies)
        dev_losses_per_batch_step.extend(batch_dev_losses)
        dev_accuracies_per_batch_step.extend(batch_dev_accuracies)
        dev_f1_per_batch_step.extend(batch_dev_f1s)

        # Evaluate at the end of the epoch to get definitive epoch-end metrics
        # and compare against best_dev_f1 for early stopping
        current_dev_loss, current_dev_acc, dev_precision, dev_recall, dev_f1, _, _ = (
            evaluate_model(
                model, dev_dataloader, device, class_weights
            )  # Pass weights for epoch-end eval too
        )
        dev_losses_per_epoch.append(current_dev_loss)
        dev_accuracies_per_epoch.append(current_dev_acc.item())

        print(
            f"\nEpoch {epoch + 1} Summary - Train Loss: {current_train_loss:.4f}, Train Acc: {current_train_acc:.4f}"
        )
        print(
            f"Epoch {epoch + 1} Summary - Dev Loss: {current_dev_loss:.4f}, Dev Acc: {current_dev_acc:.4f}, Dev Precision: {dev_precision:.4f}, Dev Recall: {dev_recall:.4f}, Dev F1-score: {dev_f1:.4f}"
        )

        # Early stopping logic
        if dev_f1 > best_dev_f1:
            best_dev_f1 = dev_f1
            epochs_no_improve = 0
            model_save_path = f"best_longformer_classifier_epoch_{epoch+1}.pt"
            torch.save(model.state_dict(), model_save_path)
            print(
                f"Saved best model to {model_save_path} with Dev F1: {best_dev_f1:.4f}"
            )
        else:
            epochs_no_improve += 1
            print(f"No improvement in Dev F1 for {epochs_no_improve} epochs.")
            if epochs_no_improve >= EARLY_STOPPING_PATIENCE:
                print(f"Early stopping triggered after {epoch + 1} epochs.")
                break  # Exit the training loop

    print("\n--- Training Complete ---")

    # Plotting the epoch-level metrics
    plot_metrics(
        train_losses_per_epoch,
        dev_losses_per_epoch,
        train_accuracies_per_epoch,
        dev_accuracies_per_epoch,
        "Epochs",
    )

    # Plotting the in-epoch (batch-level) metrics - you might want to adjust the x-axis for better readability
    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    plt.plot(train_losses_per_batch_step, label="Train Loss (Batch)")
    plt.plot(dev_losses_per_batch_step, label="Dev Loss (Batch)")
    plt.title("Loss over Training Steps (In-Epoch)")
    plt.xlabel("Evaluation Step")
    plt.ylabel("Loss")
    plt.legend()
    plt.grid(True)

    plt.subplot(1, 2, 2)
    plt.plot(train_accuracies_per_batch_step, label="Train Accuracy (Batch)")
    plt.plot(dev_accuracies_per_batch_step, label="Dev Accuracy (Batch)")
    plt.title("Accuracy over Training Steps (In-Epoch)")
    plt.xlabel("Evaluation Step")
    plt.ylabel("Accuracy")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Plotting Dev F1-score over training steps
    plt.figure(figsize=(6, 5))
    plt.plot(dev_f1_per_batch_step, label="Dev F1-score (Batch)", color="green")
    plt.title("Development F1-score over Training Steps (In-Epoch)")
    plt.xlabel("Evaluation Step")
    plt.ylabel("F1-score")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    print("\n--- Final Evaluation on Test Sets ---")
    if model_save_path and os.path.exists(model_save_path):
        model.load_state_dict(torch.load(model_save_path))
        model.to(device)
        print(f"Loaded best model from {model_save_path}")
    else:
        print(
            "No best model saved or path is invalid. Using the model from the last epoch for test evaluation."
        )

    print("\n--- Evaluating on YNACC Test Set ---")
    (
        test_loss_ynacc,
        test_acc_ynacc,
        test_precision_ynacc,
        test_recall_ynacc,
        test_f1_ynacc,
        _,
        _,
    ) = evaluate_model(
        model,
        test_ynacc_dataloader,
        device,
        class_weights,  # Use class weights for consistency in test eval too
    )
    print(
        f"YNACC Test Loss: {test_loss_ynacc:.4f}, Test Accuracy: {test_acc_ynacc:.4f}, Test Precision: {test_precision_ynacc:.4f}, Test Recall: {test_recall_ynacc:.4f}, Test F1-score: {test_f1_ynacc:.4f}"
    )

    print("\n--- Evaluating on IAC Test Set ---")
    (
        test_loss_iac,
        test_acc_iac,
        test_precision_iac,
        test_recall_iac,
        test_f1_iac,
        _,
        _,
    ) = evaluate_model(
        model,
        test_iac_dataloader,
        device,
        class_weights,  # Use class weights for consistency in test eval too
    )
    print(
        f"IAC Test Loss: {test_loss_iac:.4f}, Test Accuracy: {test_acc_iac:.4f}, Test Precision: {test_precision_iac:.4f}, Test Recall: {test_recall_iac:.4f}, Test F1-score: {test_f1_iac:.4f}"
    )

    print("\nLongformer Classification Training and Evaluation Complete.")


training()
