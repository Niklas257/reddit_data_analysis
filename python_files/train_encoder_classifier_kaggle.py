import json
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
)
import numpy as np
import random
import os
import torch.nn as nn
from collections import Counter
import torch._dynamo

torch._dynamo.config.suppress_errors = True

# --- Configuration ---
MODEL_NAME = "answerdotai/ModernBERT-base"
MAX_LEN = 512
BATCH_SIZE = 8
LEARNING_RATE = 2e-6
EPOCHS = 5
RANDOM_SEED = 42
EARLY_STOPPING_PATIENCE = 3
PERFORMANCE_FILE = "../data/model_performance.json"  # Define performance file path

# Set random seeds for reproducibility across runs
torch.manual_seed(RANDOM_SEED)
torch.cuda.manual_seed_all(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)
torch.backends.cudnn.deterministic = True
torch.backends.cudnn.benchmark = False
torch.set_float32_matmul_precision("high")

# Set device (GPU if available, else CPU)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# --- Helper Functions (No changes needed for these) ---


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

    print(
        f"Filtering entries longer than {max_len} tokens in {os.path.basename(file_path)}..."
    )
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


def train_epoch(
    model,
    train_data_loader,
    val_data_loader,  # Kept for in-epoch evaluation
    optimizer,
    # Removed scheduler from here
    device,
    epoch,
    total_epochs,
    eval_every_steps=20,
    class_weights=None,
):
    """Performs one training epoch with in-epoch evaluation."""
    model.train()
    losses = []
    correct_predictions = 0

    # Store metrics for plotting later
    train_losses_batch = []
    train_accuracies_batch = []
    val_losses_batch = []
    val_accuracies_batch = []
    val_f1_batch = []

    # Keep track of total samples processed for accurate in-epoch accuracy
    total_samples_processed_in_epoch = 0

    # Define loss function with class weights if provided
    loss_fct = nn.CrossEntropyLoss(
        weight=class_weights.to(device) if class_weights is not None else None
    )

    print("Step    | Train Loss | Train Acc | Val Loss | Val Acc | Val F1")
    for step, batch in enumerate(train_data_loader):
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["labels"].to(device)

        # Forward pass: get logits
        outputs = model(input_ids=input_ids, attention_mask=attention_mask)
        logits = outputs.logits

        # Compute loss using defined loss_fct
        loss = loss_fct(logits, labels)

        losses.append(loss.item())

        _, preds = torch.max(logits, dim=1)
        correct_predictions += torch.sum(preds == labels)
        total_samples_processed_in_epoch += labels.size(0)

        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        # >>> CRUCIAL CHANGE: REMOVED scheduler.step() from here <<<
        optimizer.zero_grad()

        # In-epoch evaluation (kept as per your request)
        if (step + 1) % eval_every_steps == 0 or step == len(train_data_loader) - 1:

            # Evaluate on validation set
            val_loss, val_acc, val_precision, val_recall, val_f1, _, _ = evaluate_model(
                model,
                val_data_loader,
                device,
                class_weights,
            )

            print(
                f"{step + 1:03d}/{len(train_data_loader)} | {np.mean(losses[-eval_every_steps:]):.4f}     | "
                f"{(correct_predictions.double() / total_samples_processed_in_epoch).item():.4f}    | "
                f"{val_loss:.4f}   | {val_acc:.4f}  | {val_f1:.4f}"
            )

            train_losses_batch.append(np.mean(losses))
            # Calculate accuracy based on samples processed SO FAR in this epoch
            train_accuracies_batch.append(
                (correct_predictions.double() / total_samples_processed_in_epoch).item()
            )

            val_losses_batch.append(val_loss)
            val_accuracies_batch.append(val_acc.item())
            val_f1_batch.append(val_f1)

            model.train()  # Set model back to train mode after validation

    # Return epoch-level averages for summary, and lists for plotting
    return (
        np.mean(losses),
        correct_predictions.double()
        / len(train_data_loader.dataset),  # This should be over total dataset size
        train_losses_batch,
        train_accuracies_batch,
        val_losses_batch,
        val_accuracies_batch,
        val_f1_batch,
    )


def evaluate_model(model, data_loader, device, class_weights=None):
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

            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits

            loss = loss_fct(logits, labels)
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


# --- Main Script (Modified for Scheduler) ---


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

    # --- Combine All Training/Validation Data ---
    df_combined_train_val = (
        pd.concat([df_ynacc_train_val, df_iac_train_val])
        .sample(frac=1, random_state=RANDOM_SEED)
        .reset_index(drop=True)
    )

    print(
        f"\nCombined training/validation data size (before split): {len(df_combined_train_val)} samples."
    )
    print("Combined training/validation data label distribution (before split):")
    print(df_combined_train_val["label"].value_counts().to_dict())

    # --- Split Combined Data into Training and Validation Sets (20% for validation) ---
    print("\n--- Splitting Combined Data into Train and Validation ---")
    val_size = 0.2

    if len(df_combined_train_val) < 2:
        print(
            f"Warning: Not enough combined data ({len(df_combined_train_val)}) for a stratified split. Skipping validation split and using all for training."
        )
        df_train = df_combined_train_val
        df_val = pd.DataFrame()
    else:
        df_train, df_val = train_test_split(
            df_combined_train_val,
            test_size=val_size,
            random_state=RANDOM_SEED,
            stratify=df_combined_train_val["label"],
        )

    print(f"Final Training entries: {len(df_train)} samples.")
    print(f"Final Validation entries: {len(df_val)} samples.")
    print("Training data label distribution:")
    print(df_train["label"].value_counts().to_dict())
    print("Validation data label distribution:")
    print(df_val["label"].value_counts().to_dict())

    # --- Calculate Class Weights for the Training Set ---
    print("\n--- Calculating Class Weights ---")
    train_labels = df_train["label"].tolist()
    class_counts = Counter(train_labels)
    num_classes = len(class_counts)

    if num_classes > 0:
        sorted_class_counts = sorted(class_counts.items())
        total_samples = sum(count for _, count in sorted_class_counts)
        weights = [
            total_samples / (num_classes * count) for _, count in sorted_class_counts
        ]
        class_weights = torch.tensor(weights, dtype=torch.float)
        print(
            f"Calculated Class Weights (based on training data): {class_weights.tolist()}"
        )
    else:
        class_weights = None
        print(
            "Warning: Cannot calculate class weights (no classes found in training data)."
        )

    # Final check for empty splits
    if (
        len(df_train) == 0
        or len(df_val) == 0
        or len(df_test_ynacc) == 0
        or len(df_test_iac) == 0
    ):
        print(
            "Error: One or more final data splits are empty. Please check data loading/splitting logic and dataset sizes."
        )
        return

    # --- Create PyTorch Datasets and DataLoaders ---
    print("\n--- Creating DataLoaders ---")
    train_dataset = CommentDataset(
        df_train["text"].tolist(), df_train["label"].tolist(), tokenizer, MAX_LEN
    )
    val_dataset = CommentDataset(
        df_val["text"].tolist(), df_val["label"].tolist(), tokenizer, MAX_LEN
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
    val_dataloader = DataLoader(val_dataset, batch_size=BATCH_SIZE)
    test_ynacc_dataloader = DataLoader(test_ynacc_dataset, batch_size=BATCH_SIZE)
    test_iac_dataloader = DataLoader(test_iac_dataset, batch_size=BATCH_SIZE)

    print(f"Train DataLoader batches: {len(train_dataloader)}")
    print(f"Dev DataLoader batches: {len(val_dataloader)}")
    print(f"YNACC Test DataLoader batches: {len(test_ynacc_dataloader)}")
    print(f"IAC Test DataLoader batches: {len(test_iac_dataloader)}")

    # --- Model Initialization ---
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2)
    model.to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)
    # Scheduler patience set to EARLY_STOPPING_PATIENCE - 1 as a common practice
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", patience=EARLY_STOPPING_PATIENCE - 1
    )

    # Lists to store metrics for saving
    train_losses_per_epoch = []
    val_losses_per_epoch = []
    train_accuracies_per_epoch = []
    val_accuracies_per_epoch = []
    val_f1s_per_epoch = []  # Track F1 per epoch for scheduler and early stopping

    # In-epoch batch metrics (will be collected from train_epoch's return)
    in_epoch_train_losses_batch = []
    in_epoch_train_accuracies_batch = []
    in_epoch_val_losses_batch = []
    in_epoch_val_accuracies_batch = []
    in_epoch_val_f1_batch = []

    print("\n--- Starting Training Loop ---")
    best_val_f1 = -1
    epochs_no_improve = 0
    model_save_path = None

    for epoch in range(EPOCHS):
        print(f"\n--- Epoch {epoch + 1}/{EPOCHS} ---")

        (
            current_train_loss,
            current_train_acc,
            batch_train_losses,
            batch_train_accuracies,
            batch_val_losses,
            batch_val_accuracies,
            batch_val_f1s,
        ) = train_epoch(
            model,
            train_dataloader,
            val_dataloader,  # Passed for in-epoch logging
            optimizer,
            device,
            epoch,
            EPOCHS,
            class_weights=class_weights,
        )

        train_losses_per_epoch.append(current_train_loss)
        train_accuracies_per_epoch.append(current_train_acc.item())

        # Collect in-epoch batch metrics for plotting
        in_epoch_train_losses_batch.extend(batch_train_losses)
        in_epoch_train_accuracies_batch.extend(batch_train_accuracies)
        in_epoch_val_losses_batch.extend(batch_val_losses)
        in_epoch_val_accuracies_batch.extend(batch_val_accuracies)
        in_epoch_val_f1_batch.extend(batch_val_f1s)

        # --- Perform full validation at the end of the epoch ---
        current_val_loss, current_val_acc, val_precision, val_recall, val_f1, _, _ = (
            evaluate_model(model, val_dataloader, device, class_weights)
        )
        val_losses_per_epoch.append(current_val_loss)
        val_accuracies_per_epoch.append(current_val_acc.item())
        val_f1s_per_epoch.append(val_f1)  # Store for scheduler and early stopping

        print(
            f"\nEpoch {epoch + 1} Summary - Train Loss: {current_train_loss:.4f}, Train Acc: {current_train_acc:.4f}"
        )
        print(
            f"Epoch {epoch + 1} Summary - Dev Loss: {current_val_loss:.4f}, Dev Acc: {current_val_acc:.4f}, Dev Precision: {val_precision:.4f}, Dev Recall: {val_recall:.4f}, Dev F1-score: {val_f1:.4f}"
        )

        # --- Scheduler Step (THIS IS THE CRUCIAL, CORRECTED CHANGE) ---
        # Step the scheduler based on the *full epoch's* validation F1 score
        scheduler.step(current_val_loss)

        # --- Early Stopping Logic ---
        if val_f1 > best_val_f1:
            best_val_f1 = val_f1
            epochs_no_improve = 0
            model_save_path = f"best_modernbert_classifier_epoch_{epoch+1}.pt"
            torch.save(model.state_dict(), model_save_path)
            print(
                f"Saved best model to {model_save_path} with Dev F1: {best_val_f1:.4f}"
            )
        else:
            epochs_no_improve += 1
            print(f"No improvement in Dev F1 for {epochs_no_improve} epochs.")
            if epochs_no_improve >= EARLY_STOPPING_PATIENCE:
                print(f"Early stopping triggered after {epoch + 1} epochs.")
                break

    print("\n--- Training Complete ---")

    print("\n--- Final Evaluation on Test Sets ---")
    if model_save_path and os.path.exists(model_save_path):
        model.load_state_dict(torch.load(model_save_path))
        model.to(device)
        print(f"Loaded best model from {model_save_path}")
    else:
        print(
            "No best model saved or path is invalid. Using the model from the last epoch for test evaluation."
        )

    # Evaluate on YNACC Test Set
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
        class_weights,
    )
    print(
        f"YNACC Test Loss: {test_loss_ynacc:.4f}, Test Accuracy: {test_acc_ynacc:.4f}, Test Precision: {test_precision_ynacc:.4f}, Test Recall: {test_recall_ynacc:.4f}, Test F1-score: {test_f1_ynacc:.4f}"
    )

    # Evaluate on IAC Test Set
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
        class_weights,
    )
    print(
        f"IAC Test Loss: {test_loss_iac:.4f}, Test Accuracy: {test_acc_iac:.4f}, Test Precision: {test_precision_iac:.4f}, Test Recall: {test_recall_iac:.4f}, Test F1-score: {test_f1_iac:.4f}"
    )

    # --- Save Performance Metrics to JSON ---
    performance_metrics = {
        "epoch_metrics": {
            "train_losses": train_losses_per_epoch,
            "val_losses": val_losses_per_epoch,
            "train_accuracies": train_accuracies_per_epoch,
            "val_accuracies": val_accuracies_per_epoch,
            "val_f1_scores": val_f1s_per_epoch,  # Added epoch-level F1s
        },
        "in_epoch_batch_metrics": {
            "train_losses": in_epoch_train_losses_batch,
            "train_accuracies": in_epoch_train_accuracies_batch,
            "val_losses": in_epoch_val_losses_batch,
            "val_accuracies": in_epoch_val_accuracies_batch,
            "val_f1_scores": in_epoch_val_f1_batch,
        },
        "final_test_results": {
            "ynacc": {
                "loss": test_loss_ynacc,
                "accuracy": test_acc_ynacc.item(),
                "precision": test_precision_ynacc,
                "recall": test_recall_ynacc,
                "f1_score": test_f1_ynacc,
            },
            "iac": {
                "loss": test_loss_iac,
                "accuracy": test_acc_iac.item(),
                "precision": test_precision_iac,
                "recall": test_recall_iac,
                "f1_score": test_f1_iac,
            },
        },
        "model_details": {
            "model_name": MODEL_NAME,
            "max_len": MAX_LEN,
            "batch_size": BATCH_SIZE,
            "learning_rate": LEARNING_RATE,
            "epochs": EPOCHS,
            "random_seed": RANDOM_SEED,
            "early_stopping_patience": EARLY_STOPPING_PATIENCE,
            "final_best_val_f1": best_val_f1,
            "model_saved_path": model_save_path,
            "class_weights_used": (
                class_weights.tolist() if class_weights is not None else None
            ),
        },
    }

    os.makedirs(os.path.dirname(PERFORMANCE_FILE), exist_ok=True)

    with open(PERFORMANCE_FILE, "w") as f:
        json.dump(performance_metrics, f, indent=4)
    print(f"\nAll performance metrics saved to {PERFORMANCE_FILE}")

    print("\nLongformer Classification Training and Evaluation Complete.")


training()
