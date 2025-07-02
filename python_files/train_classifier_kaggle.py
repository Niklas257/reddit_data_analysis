import json
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    BitsAndBytesConfig,
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training, PeftModel
import numpy as np
import random
import os
import torch.nn as nn
from collections import Counter
import torch._dynamo
import datetime
import gc
import warnings
from huggingface_hub import login
from kaggle_secrets import UserSecretsClient

# Suppress warnings
warnings.filterwarnings("ignore", message=".*torch.utils.checkpoint.*use_reentrant.*")
warnings.filterwarnings("ignore", message=".*WON'T CONVERT.*")
warnings.filterwarnings("ignore", module="torch*")
warnings.filterwarnings("ignore", module="transformers.*")

user_secrets = UserSecretsClient()
secret_value_0 = user_secrets.get_secret("HF_TOKEN")
login(token=secret_value_0)

torch._dynamo.config.suppress_errors = True
torch._dynamo.config.verbose = False

# --- Configuration ---
MODEL_NAME = "meta-llama/Llama-3.2-1B"
MAX_LEN = 512
BATCH_SIZE = 8
LEARNING_RATE = 1e-5
RANDOM_SEED = 42
EARLY_STOPPING_PATIENCE = 3
# Generate a unique performance file name with a timestamp
current_time_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
PERFORMANCE_FILE = f"/kaggle/working/performance_metrics_{current_time_str}.json"

# --- New Global/Configurable Parameters for Curriculum Learning ---
STUDENT_TEACHER_EPOCHS_PER_ITERATION = (
    3  # Number of epochs the student trains on the current curriculum
)
CONFIDENCE_THRESHOLD_START = 0.95  # Initial high confidence for pseudo-labeling
CONFIDENCE_THRESHOLD_END = 0.70  # Final lower confidence threshold
CONFIDENCE_DECAY_FACTOR = (
    0.05  # How much the confidence threshold decreases per iteration
)
MAX_CURRICULUM_ITERATIONS = 5  # Max number of curriculum steps
UNLABELED_DATA_FRACTION_PER_STEP = 1  # Fraction of unlabeled data to consider for pseudo-labeling in each step, helps with large datasets
PSEUDO_LABEL_BATCH_SIZE = (
    BATCH_SIZE * 4
)  # Larger batch size for pseudo-labeling for efficiency
# Set random seeds for reproducibility across runs
torch.manual_seed(RANDOM_SEED)
torch.cuda.manual_seed_all(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)
torch.backends.cudnn.deterministic = True
torch.backends.cudnn.benchmark = False
torch.set_float32_matmul_precision("high")

# Additional CUDA settings to help with stability
if torch.cuda.is_available():
    torch.cuda.empty_cache()
    # Set memory allocation strategy
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True

# Set device (GPU if available, else CPU)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

USE_QLORA = True  # Set to True to enable QLoRA
lora_rank = 16  # Rank for QLoRA, can be adjusted based on model capacity
lora_alpha = 32  # Scaling factor for LoRA, typically 2x the rank
bnb_bits = 4  # Bits for quantization, can be 4 or 8 depending on model and GPU support


def get_target_modules_for_model(model_name):
    """
    Get appropriate LoRA target modules based on the model architecture.
    """
    if "modernbert" in model_name.lower():
        # ModernBERT specific modules - might need adjustment based on actual architecture
        return ["Wqkv", "Wo", "Wi", "dense"]
    elif "llama" in model_name.lower():
        return [
            "q_proj",
            "k_proj",
            "v_proj",
            "o_proj",
            "gate_proj",
            "up_proj",
            "down_proj",
        ]
    else:
        # Default fallback
        return ["query", "value", "key", "dense"]


USE_QLORA = True  # Set to True to enable QLoRA
# --- QLoRA Configuration (Only defined if USE_QLORA is True) ---
bnb_config = None
lora_config = None
if USE_QLORA:
    # Dynamic BitsAndBytesConfig based on bnb_bits setting
    if bnb_bits == 4:
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",  # NormalFloat4 quantization
            bnb_4bit_compute_dtype=torch.bfloat16,  # Or torch.float16 for older GPUs
            llm_int8_skip_modules=(
                ["classifier"] if "modernbert" in MODEL_NAME.lower() else None
            ),
        )
        print("Using 4-bit quantization with NF4 and double quantization")
    elif bnb_bits == 8:
        bnb_config = BitsAndBytesConfig(
            load_in_8bit=True,
            llm_int8_threshold=6.0,  # Threshold for outlier detection
            llm_int8_skip_modules=(
                ["classifier"] if "modernbert" in MODEL_NAME.lower() else None
            ),
        )
        print("Using 8-bit quantization with int8 threshold=6.0")
    else:
        raise ValueError(f"bnb_bits must be 4 or 8, got {bnb_bits}")

    # LoRA configuration - improved parameters for better performance
    lora_config = LoraConfig(
        r=lora_rank,  # Uses the configurable rank parameter
        lora_alpha=lora_alpha,  # Uses the configurable alpha parameter
        target_modules=get_target_modules_for_model(
            MODEL_NAME
        ),  # Auto-detect based on model
        modules_to_save=["classifier"] if "modernbert" in MODEL_NAME.lower() else None,
        lora_dropout=0.1,  # Dropout probability on the LoRA layers.
        bias="none",  # Can be "none", "all", "lora_only"
        task_type="SEQ_CLS",  # Specify the task type
    )
    print(f"LoRA config: rank={lora_rank}, alpha={lora_alpha}, dropout=0.1")


"""
model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_NAME,
    num_labels=2,
    # quantization_config=bnb_config,
    device_map=(
        {"": 0} if torch.cuda.is_available() else None
    ),  # Load directly to GPU if available
)
print(model)
"""
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


# --- Function to load and prepare the model (handles QLoRA dynamically) ---
def load_and_prepare_model(
    model_name, num_labels, use_qlora, bnb_config, lora_config, device, tokenizer
):
    print(f"Loading model {model_name}...")
    if use_qlora:
        # Load base model with 4-bit quantization
        model = AutoModelForSequenceClassification.from_pretrained(
            model_name,
            num_labels=num_labels,
            quantization_config=bnb_config,
            device_map=(
                {"": 0} if torch.cuda.is_available() else None
            ),  # Load directly to GPU if available
        )
        model.config.pad_token_id = tokenizer.pad_token_id
        # Prepare for k-bit training and add LoRA adapters
        model = prepare_model_for_kbit_training(model, use_gradient_checkpointing=True)
        model = get_peft_model(model, lora_config)

        # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
        if hasattr(model, "gradient_checkpointing_enable"):
            model.gradient_checkpointing_enable(
                gradient_checkpointing_kwargs={"use_reentrant": False}
            )
        print("QLoRA enabled. Trainable parameters:")
        model.print_trainable_parameters()
    else:
        # Load standard model
        model = AutoModelForSequenceClassification.from_pretrained(
            model_name, num_labels=num_labels
        )
    model.to(device)
    return model


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
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

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


def training():
    ynacc_file_path = "/kaggle/input/ynacc-processed/ynacc_processed.jsonl"
    iac_file_path = "/kaggle/input/iac-processed/iac_processed.jsonl"
    unlabeled_reddit_file_path = (
        "/kaggle/input/reddit-data/reddit_train.jsonl"  # Unlabeled data for curriculum
    )
    reddit_val_file_path = "/kaggle/input/reddit-data/reddit_val.jsonl"
    reddit_test_file_path = "/kaggle/input/reddit-data/reddit_test.jsonl"

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    # --- Load Data ---
    print("Loading YNACC data...")
    df_ynacc_raw = load_jsonl(ynacc_file_path, tokenizer, MAX_LEN)
    print("Loading IAC data...")
    df_iac_raw = load_jsonl(iac_file_path, tokenizer, MAX_LEN)
    print("Loading Unlabeled Reddit data...")
    df_unlabeled_reddit_raw = load_jsonl(
        unlabeled_reddit_file_path, tokenizer, MAX_LEN
    )  # No labels needed initially
    print("Loading Reddit Validation data...")
    df_reddit_val = load_jsonl(reddit_val_file_path, tokenizer, MAX_LEN)
    print("Loading Reddit Test data...")
    df_reddit_test = load_jsonl(reddit_test_file_path, tokenizer, MAX_LEN)

    if (
        df_ynacc_raw.empty
        or df_iac_raw.empty
        or df_unlabeled_reddit_raw.empty
        or df_reddit_val.empty
        or df_reddit_test.empty
    ):
        print(
            "Exiting: One or more datasets could not be loaded or are empty after filtering."
        )
        return

    # --- Separate Test Sets (100 samples each, not mixed) ---
    print("\n--- Splitting Test Sets ---")
    # Stratify by label to ensure class balance in test sets
    df_ynacc_train, df_ynacc_test = (
        train_test_split(  # df_ynacc_train will be part of initial training data
            df_ynacc_raw,
            test_size=100,
            random_state=RANDOM_SEED,
            stratify=df_ynacc_raw["label"],
        )
    )
    df_iac_train, df_iac_test = (
        train_test_split(  # df_iac_train will be part of initial training data
            df_iac_raw,
            test_size=100,
            random_state=RANDOM_SEED,
            stratify=df_iac_raw["label"],
        )
    )
    df_ynacc_train, df_ynacc_val = train_test_split(
        df_ynacc_train,  # Remaining YNACC data for initial training
        test_size=100,
        random_state=RANDOM_SEED,
        stratify=df_ynacc_train["label"],
    )
    df_iac_train, df_iac_val = train_test_split(
        df_iac_train,  # Remaining IAC data for initial training
        test_size=100,
        random_state=RANDOM_SEED,
        stratify=df_iac_train["label"],
    )

    print(f"YNACC data for initial training: {len(df_ynacc_train)} samples.")
    print(
        f"YNACC val data: {len(df_ynacc_val)} samples (Label distribution: {df_ynacc_val['label'].value_counts().to_dict()})."
    )
    print(
        f"YNACC test data: {len(df_ynacc_test)} samples (Label distribution: {df_ynacc_test['label'].value_counts().to_dict()})."
    )
    print(f"IAC data for initial training: {len(df_iac_train)} samples.")
    print(
        f"IAC val data: {len(df_iac_val)} samples (Label distribution: {df_iac_val['label'].value_counts().to_dict()})."
    )
    print(
        f"IAC test data: {len(df_iac_test)} samples (Label distribution: {df_iac_test['label'].value_counts().to_dict()})."
    )
    print(
        f"Unlabeled Reddit data for curriculum: {len(df_unlabeled_reddit_raw)} samples."
    )
    print(
        f"Reddit val data: {len(df_reddit_val)} samples (Label distribution: {df_reddit_val['label'].value_counts().to_dict()})."
    )
    print(
        f"Reddit test data: {len(df_reddit_test)} samples (Label distribution: {df_reddit_test['label'].value_counts().to_dict()})."
    )

    # --- Combine All Initial Labeled Training Data ---
    df_train_initial = (
        pd.concat([df_ynacc_train, df_iac_train])
        .sample(frac=1, random_state=RANDOM_SEED)
        .reset_index(drop=True)
    )

    print(f"\nCombined initial training data size: {len(df_train_initial)} samples.")
    print("Combined initial training data label distribution:")
    print(df_train_initial["label"].value_counts().to_dict())

    # --- Calculate Class Weights for the Initial Training Set ---
    print("\n--- Calculating Class Weights ---")
    train_labels_initial = df_train_initial["label"].tolist()
    class_counts_initial = Counter(train_labels_initial)
    num_classes = len(class_counts_initial)

    if num_classes > 0:
        sorted_class_counts = sorted(class_counts_initial.items())
        total_samples = sum(count for _, count in sorted_class_counts)
        weights = [
            total_samples / (num_classes * count) for _, count in sorted_class_counts
        ]
        class_weights = torch.tensor(weights, dtype=torch.float)
        print(
            f"Calculated Class Weights (based on initial training data): {class_weights.tolist()}"
        )
    else:
        class_weights = None
        print(
            "Warning: Cannot calculate class weights (no classes found in initial training data)."
        )

    # Final check for empty splits
    if (
        len(df_train_initial) == 0
        or len(df_ynacc_val) == 0
        or len(df_ynacc_test) == 0
        or len(df_iac_val) == 0
        or len(df_iac_test) == 0
        or len(df_reddit_val) == 0
        or len(df_reddit_test) == 0
    ):
        print(
            "Error: One or more final data splits are empty. Please check data loading/splitting logic and dataset sizes."
        )
        return

    # --- Create PyTorch Datasets and DataLoaders for Val and Test ---
    # These are the *fixed* validation and test sets used throughout
    val_ynacc_dataset = CommentDataset(
        df_ynacc_val["text"].tolist(),
        df_ynacc_val["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    val_iac_dataset = CommentDataset(
        df_iac_val["text"].tolist(), df_iac_val["label"].tolist(), tokenizer, MAX_LEN
    )
    val_reddit_dataset = CommentDataset(
        df_reddit_val["text"].tolist(),
        df_reddit_val["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    test_ynacc_dataset = CommentDataset(  # Real test sets for final eval
        df_ynacc_test["text"].tolist(),
        df_ynacc_test["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    test_iac_dataset = CommentDataset(
        df_iac_test["text"].tolist(), df_iac_test["label"].tolist(), tokenizer, MAX_LEN
    )
    test_reddit_dataset = CommentDataset(
        df_reddit_test["text"].tolist(),
        df_reddit_test["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )

    # DataLoaders for the dedicated validation sets (used in each epoch/iteration)
    val_ynacc_dataloader = DataLoader(val_ynacc_dataset, batch_size=BATCH_SIZE)
    val_iac_dataloader = DataLoader(val_iac_dataset, batch_size=BATCH_SIZE)
    val_reddit_dataloader = DataLoader(val_reddit_dataset, batch_size=BATCH_SIZE)

    # DataLoaders for the dedicated test sets (used only at the very end)
    test_ynacc_dataloader = DataLoader(test_ynacc_dataset, batch_size=BATCH_SIZE)
    test_iac_dataloader = DataLoader(test_iac_dataset, batch_size=BATCH_SIZE)
    test_reddit_dataloader = DataLoader(test_reddit_dataset, batch_size=BATCH_SIZE)

    # --- Model Initialization ---
    model = load_and_prepare_model(
        MODEL_NAME, 2, USE_QLORA, bnb_config, lora_config, device, tokenizer
    )

    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", patience=2
    )

    # Lists to store metrics for saving
    # Epoch-level metrics per curriculum iteration (aggregated)
    all_curriculum_epoch_train_losses = []
    all_curriculum_epoch_train_accuracies = []
    all_curriculum_epoch_val_losses_ynacc = []
    all_curriculum_epoch_val_accuracies_ynacc = []
    all_curriculum_epoch_val_f1s_ynacc = []
    all_curriculum_epoch_val_losses_iac = []
    all_curriculum_epoch_val_accuracies_iac = []
    all_curriculum_epoch_val_f1s_iac = []
    all_curriculum_epoch_val_losses_reddit = []
    all_curriculum_epoch_val_accuracies_reddit = []
    all_curriculum_epoch_val_f1s_reddit = []

    # Batch-wise metrics (cumulative across all epochs and iterations)
    in_epoch_train_losses_batch = []
    in_epoch_train_accuracies_batch = []
    in_epoch_val_losses_batch_from_train_epoch = (
        []
    )  # From the val_dataloader passed to train_epoch (e.g., Reddit val)
    in_epoch_val_accuracies_batch_from_train_epoch = []
    in_epoch_val_f1_batch_from_train_epoch = []

    print("\n--- Starting Curriculum Training Loop ---")
    best_combined_val_loss_overall = float("inf")
    epochs_no_improve_overall = 0  # For overall curriculum early stopping
    overall_best_model_save_path = None
    current_confidence_threshold = CONFIDENCE_THRESHOLD_START

    # Initial training phase (Curriculum Iteration 0)
    print("\n--- Curriculum Iteration 0: Initial Supervised Training ---")
    current_train_dataset = CommentDataset(
        df_train_initial["text"].tolist(),
        df_train_initial["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    current_train_dataloader = DataLoader(
        current_train_dataset, batch_size=BATCH_SIZE, shuffle=True
    )

    print(f"Initial Train DataLoader batches: {len(current_train_dataloader)}")
    print(f"YNACC Dev DataLoader batches: {len(val_ynacc_dataloader)}")
    print(f"IAC Dev DataLoader batches: {len(val_iac_dataloader)}")
    print(f"Reddit Dev DataLoader batches: {len(val_reddit_dataloader)}")

    # Inner early stopping for student within this iteration
    best_val_loss_student_in_iter = float("inf")
    epochs_no_improve_student_in_iter = 0
    model_save_path_in_iter = None

    for epoch_in_iter in range(STUDENT_TEACHER_EPOCHS_PER_ITERATION):
        print(
            f"\n--- Initial Supervised Training Epoch {epoch_in_iter + 1}/{STUDENT_TEACHER_EPOCHS_PER_ITERATION} ---"
        )

        # Pass Reddit val dataloader to train_epoch for its internal batch-wise metric logging
        (
            current_train_loss,
            current_train_acc,
            batch_train_losses,
            batch_train_accuracies,
            batch_val_losses_from_te,  # TE = train_epoch
            batch_val_accuracies_from_te,
            batch_val_f1s_from_te,
        ) = train_epoch(
            model,
            current_train_dataloader,
            val_reddit_dataloader,  # Used by train_epoch for batch-level val metrics
            optimizer,
            device,
            epoch_in_iter,
            STUDENT_TEACHER_EPOCHS_PER_ITERATION,
            class_weights=class_weights,
        )

        # Collect batch-wise metrics
        in_epoch_train_losses_batch.extend(batch_train_losses)
        in_epoch_train_accuracies_batch.extend(batch_train_accuracies)
        in_epoch_val_losses_batch_from_train_epoch.extend(batch_val_losses_from_te)
        in_epoch_val_accuracies_batch_from_train_epoch.extend(
            batch_val_accuracies_from_te
        )
        in_epoch_val_f1_batch_from_train_epoch.extend(batch_val_f1s_from_te)

        # --- Perform full evaluation on all validation sets at the end of the epoch ---
        print("\n--- Evaluating on all development sets ---")
        val_losses_epoch = {}
        val_accuracies_epoch = {}
        val_f1s_epoch = {}

        # Evaluate on YNACC Dev Set
        val_loss_ynacc, val_acc_ynacc, _, _, val_f1_ynacc, _, _ = evaluate_model(
            model, val_ynacc_dataloader, device, class_weights
        )
        print(
            f"YNACC Dev Loss: {val_loss_ynacc:.4f}, Acc: {val_acc_ynacc:.4f}, F1: {val_f1_ynacc:.4f}"
        )
        val_losses_epoch["ynacc"] = val_loss_ynacc
        val_accuracies_epoch["ynacc"] = val_acc_ynacc.item()
        val_f1s_epoch["ynacc"] = val_f1_ynacc

        # Evaluate on IAC Dev Set
        val_loss_iac, val_acc_iac, _, _, val_f1_iac, _, _ = evaluate_model(
            model, val_iac_dataloader, device, class_weights
        )
        print(
            f"IAC Dev Loss: {val_loss_iac:.4f}, Acc: {val_acc_iac:.4f}, F1: {val_f1_iac:.4f}"
        )
        val_losses_epoch["iac"] = val_loss_iac
        val_accuracies_epoch["iac"] = val_acc_iac.item()
        val_f1s_epoch["iac"] = val_f1_iac

        # Evaluate on Reddit Validation Set (now used for combined dev in curriculum)
        val_loss_reddit, val_acc_reddit, _, _, val_f1_reddit, _, _ = evaluate_model(
            model, val_reddit_dataloader, device, class_weights
        )
        print(
            f"Reddit Dev Loss: {val_loss_reddit:.4f}, Acc: {val_acc_reddit:.4f}, F1: {val_f1_reddit:.4f}"
        )
        val_losses_epoch["reddit"] = val_loss_reddit
        val_accuracies_epoch["reddit"] = val_acc_reddit.item()
        val_f1s_epoch["reddit"] = val_f1_reddit

        # For Iteration 0, combined validation loss is from YNACC and IAC dev sets
        current_combined_val_loss_for_scheduler = (val_loss_ynacc + val_loss_iac) / 2

        print(
            f"\nEpoch {epoch_in_iter + 1} Summary - Train Loss: {current_train_loss:.4f}, Train Acc: {current_train_acc:.4f}"
        )
        print(
            f"Epoch {epoch_in_iter + 1} Combined Dev Loss (YNACC+IAC): {current_combined_val_loss_for_scheduler:.4f}"
        )

        # Store epoch-level metrics for saving
        all_curriculum_epoch_train_losses.append(current_train_loss)
        all_curriculum_epoch_train_accuracies.append(current_train_acc.item())
        all_curriculum_epoch_val_losses_ynacc.append(val_loss_ynacc)
        all_curriculum_epoch_val_accuracies_ynacc.append(val_acc_ynacc.item())
        all_curriculum_epoch_val_f1s_ynacc.append(val_f1_ynacc)
        all_curriculum_epoch_val_losses_iac.append(val_loss_iac)
        all_curriculum_epoch_val_accuracies_iac.append(val_acc_iac.item())
        all_curriculum_epoch_val_f1s_iac.append(val_f1_iac)
        all_curriculum_epoch_val_losses_reddit.append(val_loss_reddit)
        all_curriculum_epoch_val_accuracies_reddit.append(val_acc_reddit.item())
        all_curriculum_epoch_val_f1s_reddit.append(val_f1_reddit)

        # --- Scheduler Step (based on combined validation loss for inner loop) ---
        scheduler.step(current_combined_val_loss_for_scheduler)

        # --- Inner Early Stopping Logic for Student within this Iteration ---
        if current_combined_val_loss_for_scheduler < best_val_loss_student_in_iter:
            best_val_loss_student_in_iter = current_combined_val_loss_for_scheduler
            epochs_no_improve_student_in_iter = 0
            # Save the best model state within this particular curriculum iteration
            model_save_path_in_iter = f"best_model_iter_0_epoch_{epoch_in_iter+1}"
            if USE_QLORA:
                model.save_pretrained(model_save_path_in_iter)  # Save PEFT adapters
            else:
                torch.save(model.state_dict(), f"{model_save_path_in_iter}.pt")
            print(
                f"Saved best model for initial training phase to {model_save_path_in_iter} with Combined Dev Loss: {best_val_loss_student_in_iter:.4f}"
            )
        else:
            epochs_no_improve_student_in_iter += 1
            print(
                f"No improvement in Combined Dev Loss for student for {epochs_no_improve_student_in_iter} epochs in initial training."
            )
            if epochs_no_improve_student_in_iter >= EARLY_STOPPING_PATIENCE:
                print(
                    f"Inner early stopping triggered during initial training after {epoch_in_iter + 1} epochs."
                )
                break

    # If inner early stopping occurred, load the best model for this initial phase
    if model_save_path_in_iter and os.path.exists(model_save_path_in_iter):
        if USE_QLORA:
            # To load PEFT adapters back into the 'model' variable
            # We need to re-create the base model and then load adapters
            del model  # Delete current model to free up GPU memory before reloading
            gc.collect()
            torch.cuda.empty_cache()

            model = load_and_prepare_model(
                MODEL_NAME, 2, USE_QLORA, bnb_config, lora_config, device, tokenizer
            )
            # Load the saved PEFT adapters
            model = PeftModel.from_pretrained(model, model_save_path_in_iter)
            # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
            if hasattr(model, "gradient_checkpointing_enable"):
                model.gradient_checkpointing_enable(
                    gradient_checkpointing_kwargs={"use_reentrant": False}
                )
            print(
                f"Loaded best QLoRA model from {model_save_path_in_iter} for curriculum phase initialization."
            )
        else:
            model.load_state_dict(torch.load(f"{model_save_path_in_iter}.pt"))
            model.to(device)
            print(
                f"Loaded best model from {model_save_path_in_iter}.pt for curriculum phase initialization."
            )
    else:
        print(
            "No best model saved from initial training. Continuing with the last trained model from initial phase."
        )

    # Update the overall best model and path if this initial training achieved a new best
    if best_val_loss_student_in_iter < best_combined_val_loss_overall:
        best_combined_val_loss_overall = best_val_loss_student_in_iter
        overall_best_model_save_path = model_save_path_in_iter
        print(
            f"Overall best model updated after initial training to {overall_best_model_save_path}"
        )

    # Update the teacher model with the current best student's weights from the initial phase
    # model.load_state_dict(model.state_dict())
    model.eval()

    # --- Curriculum Learning Iterations ---
    for iteration in range(1, MAX_CURRICULUM_ITERATIONS + 1):
        print(f"\n--- Curriculum Iteration {iteration}/{MAX_CURRICULUM_ITERATIONS} ---")

        # 1. Teacher Pseudo-Labeling
        print(
            f"Teacher pseudo-labeling unlabeled Reddit data with confidence threshold: {current_confidence_threshold:.2f}"
        )
        unlabeled_texts = df_unlabeled_reddit_raw["text"].tolist()
        unlabeled_dataset = CommentDataset(
            unlabeled_texts, [0] * len(unlabeled_texts), tokenizer, MAX_LEN
        )  # Dummy labels
        unlabeled_dataloader = DataLoader(
            unlabeled_dataset, batch_size=PSEUDO_LABEL_BATCH_SIZE
        )

        pseudo_labels = []
        confidences = []
        with torch.no_grad():
            for batch in unlabeled_dataloader:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)
                outputs = model(input_ids=input_ids, attention_mask=attention_mask)
                logits = outputs.logits
                probabilities = torch.softmax(logits, dim=-1)
                max_confidences, predicted_labels = torch.max(probabilities, dim=-1)

                pseudo_labels.extend(predicted_labels.cpu().tolist())
                confidences.extend(max_confidences.cpu().tolist())

        df_pseudo_labeled = pd.DataFrame(
            {
                "text": unlabeled_texts,
                "pseudo_label": pseudo_labels,
                "confidence": confidences,
            }
        )

        # Select high-confidence pseudo-labeled data for the current curriculum step
        high_confidence_pseudo_labeled_df = df_pseudo_labeled[
            df_pseudo_labeled["confidence"] >= current_confidence_threshold
        ]

        # Optionally, sample a fraction of the high-confidence data to manage dataset size
        if UNLABELED_DATA_FRACTION_PER_STEP < 1.0:
            high_confidence_pseudo_labeled_df = (
                high_confidence_pseudo_labeled_df.sample(
                    frac=UNLABELED_DATA_FRACTION_PER_STEP, random_state=RANDOM_SEED
                ).reset_index(drop=True)
            )

        print(
            f"Selected {len(high_confidence_pseudo_labeled_df)} high-confidence pseudo-labeled samples for training."
        )
        print("Pseudo-labeled data label distribution:")
        print(
            high_confidence_pseudo_labeled_df["pseudo_label"].value_counts().to_dict()
        )

        # If no new samples are added, we might consider stopping or just continue with current data
        if (
            len(high_confidence_pseudo_labeled_df) == 0
            and current_confidence_threshold == CONFIDENCE_THRESHOLD_END
        ):
            print(
                "No new high-confidence pseudo-labeled samples found and minimum threshold reached. Ending curriculum."
            )
            break  # No new data to learn from, stop.

        # Combine initial labeled training data with selected pseudo-labeled data
        current_train_df = (
            pd.concat(
                [
                    df_train_initial,
                    high_confidence_pseudo_labeled_df.rename(
                        columns={"pseudo_label": "label"}
                    ),
                ]
            )
            .sample(frac=1, random_state=RANDOM_SEED)
            .reset_index(drop=True)
        )

        # Update class weights based on the new combined training set
        train_labels_current = current_train_df["label"].tolist()
        class_counts_current = Counter(train_labels_current)
        if num_classes > 0:
            sorted_class_counts = sorted(class_counts_current.items())
            total_samples_current = sum(count for _, count in sorted_class_counts)
            weights_current = [
                total_samples_current / (num_classes * count)
                for _, count in sorted_class_counts
            ]
            class_weights = torch.tensor(weights_current, dtype=torch.float)
            print(f"Updated Class Weights: {class_weights.tolist()}")
        else:
            class_weights = None
            print(
                "Warning: Cannot update class weights (no classes found in current training data)."
            )

        # Create DataLoader for the current curriculum
        current_train_dataset = CommentDataset(
            current_train_df["text"].tolist(),
            current_train_df["label"].tolist(),
            tokenizer,
            MAX_LEN,
        )
        current_train_dataloader = DataLoader(
            current_train_dataset, batch_size=BATCH_SIZE, shuffle=True
        )
        print(f"Current Training DataLoader batches: {len(current_train_dataloader)}")

        # 2. Student Training on Current Curriculum
        # Reset inner early stopping patience for student training within this curriculum step
        epochs_no_improve_student_in_iter = 0
        best_val_loss_student_in_iter = float("inf")
        model_save_path_in_iter = None
        # Initialize a new model for the student in this iteration
        model = load_and_prepare_model(
            MODEL_NAME, 2, USE_QLORA, bnb_config, lora_config, device, tokenizer
        )

        optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", patience=2
        )
        for epoch_in_iter in range(STUDENT_TEACHER_EPOCHS_PER_ITERATION):
            print(
                f"\n--- Student Training Epoch {epoch_in_iter + 1}/{STUDENT_TEACHER_EPOCHS_PER_ITERATION} in Iteration {iteration} ---"
            )
            (
                current_train_loss,
                current_train_acc,
                batch_train_losses,
                batch_train_accuracies,
                batch_val_losses_from_te,
                batch_val_accuracies_from_te,
                batch_val_f1s_from_te,
            ) = train_epoch(
                model,
                current_train_dataloader,
                # TODO: This should be combined val datasets from all dev sets?
                val_reddit_dataloader,  # Still pass Reddit val dataloader for internal batch-level logging
                optimizer,
                device,
                epoch_in_iter,
                STUDENT_TEACHER_EPOCHS_PER_ITERATION,
                class_weights=class_weights,
            )

            # Collect batch-wise metrics
            in_epoch_train_losses_batch.extend(batch_train_losses)
            in_epoch_train_accuracies_batch.extend(batch_train_accuracies)
            in_epoch_val_losses_batch_from_train_epoch.extend(batch_val_losses_from_te)
            in_epoch_val_accuracies_batch_from_train_epoch.extend(
                batch_val_accuracies_from_te
            )
            in_epoch_val_f1_batch_from_train_epoch.extend(batch_val_f1s_from_te)

            # --- Perform full evaluation on all validation sets at the end of the epoch ---
            print("\n--- Evaluating on all development sets ---")
            val_losses_epoch = {}
            val_accuracies_epoch = {}
            val_f1s_epoch = {}

            # Evaluate on YNACC Dev Set
            val_loss_ynacc, val_acc_ynacc, _, _, val_f1_ynacc, _, _ = evaluate_model(
                model, val_ynacc_dataloader, device, class_weights
            )
            print(
                f"YNACC Dev Loss: {val_loss_ynacc:.4f}, Acc: {val_acc_ynacc:.4f}, F1: {val_f1_ynacc:.4f}"
            )
            val_losses_epoch["ynacc"] = val_loss_ynacc
            val_accuracies_epoch["ynacc"] = val_acc_ynacc.item()
            val_f1s_epoch["ynacc"] = val_f1_ynacc

            # Evaluate on IAC Dev Set
            val_loss_iac, val_acc_iac, _, _, val_f1_iac, _, _ = evaluate_model(
                model, val_iac_dataloader, device, class_weights
            )
            print(
                f"IAC Dev Loss: {val_loss_iac:.4f}, Acc: {val_acc_iac:.4f}, F1: {val_f1_iac:.4f}"
            )
            val_losses_epoch["iac"] = val_loss_iac
            val_accuracies_epoch["iac"] = val_acc_iac.item()
            val_f1s_epoch["iac"] = val_f1_iac

            # Evaluate on Reddit Validation Set
            val_loss_reddit, val_acc_reddit, _, _, val_f1_reddit, _, _ = evaluate_model(
                model, val_reddit_dataloader, device, class_weights
            )
            print(
                f"Reddit Dev Loss: {val_loss_reddit:.4f}, Acc: {val_acc_reddit:.4f}, F1: {val_f1_reddit:.4f}"
            )
            val_losses_epoch["reddit"] = val_loss_reddit
            val_accuracies_epoch["reddit"] = val_acc_reddit.item()
            val_f1s_epoch["reddit"] = val_f1_reddit

            # Combined validation loss for scheduler and inner early stopping (mean of all 3 dev sets)
            current_combined_val_loss_for_scheduler = (
                val_loss_ynacc + val_loss_iac + val_loss_reddit
            ) / 3

            print(
                f"\nEpoch {epoch_in_iter + 1} Summary - Train Loss: {current_train_loss:.4f}, Train Acc: {current_train_acc:.4f}"
            )
            print(
                f"Epoch {epoch_in_iter + 1} Combined Dev Loss (YNACC+IAC+Reddit): {current_combined_val_loss_for_scheduler:.4f}"
            )

            # Store epoch-level metrics for saving
            all_curriculum_epoch_train_losses.append(current_train_loss)
            all_curriculum_epoch_train_accuracies.append(current_train_acc.item())
            all_curriculum_epoch_val_losses_ynacc.append(val_loss_ynacc)
            all_curriculum_epoch_val_accuracies_ynacc.append(val_acc_ynacc.item())
            all_curriculum_epoch_val_f1s_ynacc.append(val_f1_ynacc)
            all_curriculum_epoch_val_losses_iac.append(val_loss_iac)
            all_curriculum_epoch_val_accuracies_iac.append(val_acc_iac.item())
            all_curriculum_epoch_val_f1s_iac.append(val_f1_iac)
            all_curriculum_epoch_val_losses_reddit.append(val_loss_reddit)
            all_curriculum_epoch_val_accuracies_reddit.append(val_acc_reddit.item())
            all_curriculum_epoch_val_f1s_reddit.append(val_f1_reddit)

            # --- Scheduler Step (based on combined validation loss for inner loop) ---
            scheduler.step(current_combined_val_loss_for_scheduler)

            # --- Inner Early Stopping Logic for Student within this Iteration ---
            if current_combined_val_loss_for_scheduler < best_val_loss_student_in_iter:
                best_val_loss_student_in_iter = current_combined_val_loss_for_scheduler
                epochs_no_improve_student_in_iter = 0
                model_save_path_in_iter = (
                    f"best_model_iter_{iteration}_epoch_{epoch_in_iter+1}"
                )
                if USE_QLORA:
                    # Save the LoRA weights if using QLoRA
                    model.save_pretrained(model_save_path_in_iter)
                else:
                    torch.save(model.state_dict(), f"{model_save_path_in_iter}.pt")
                print(
                    f"Saved best model for iteration {iteration} to {model_save_path_in_iter}.pt with Combined Dev Loss: {best_val_loss_student_in_iter:.4f}"
                )
            else:
                epochs_no_improve_student_in_iter += 1
                print(
                    f"No improvement in Combined Dev Loss for student for {epochs_no_improve_student_in_iter} epochs in iteration {iteration}."
                )
                if epochs_no_improve_student_in_iter >= EARLY_STOPPING_PATIENCE:
                    print(
                        f"Inner early stopping triggered for student in iteration {iteration} after {epoch_in_iter + 1} epochs."
                    )
                    break

        # Load the best model from this iteration to ensure it's used for the next teacher update and overall early stopping check
        if model_save_path_in_iter and os.path.exists(model_save_path_in_iter):
            if USE_QLORA:
                # To load PEFT adapters back into the 'model' variable
                # We need to re-create the base model and then load adapters
                del model  # Delete current model to free up GPU memory before reloading
                gc.collect()
                torch.cuda.empty_cache()

                model = load_and_prepare_model(
                    MODEL_NAME, 2, USE_QLORA, bnb_config, lora_config, device, tokenizer
                )
                # Load the saved PEFT adapters
                model = PeftModel.from_pretrained(model, model_save_path_in_iter)
                # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
                if hasattr(model, "gradient_checkpointing_enable"):
                    model.gradient_checkpointing_enable(
                        gradient_checkpointing_kwargs={"use_reentrant": False}
                    )
                print(
                    f"Loaded best QLoRA model from {model_save_path_in_iter} for curriculum phase initialization."
                )
            else:
                model.load_state_dict(torch.load(f"{model_save_path_in_iter}.pt"))
                model.to(device)
                print(
                    f"Loaded best model from {model_save_path_in_iter}.pt for curriculum phase initialization."
                )

        else:
            print(
                "No best model saved in this iteration. Continuing with the last trained model."
            )

        # Update the teacher model with the current best student's weights from this iteration
        # model.load_state_dict(model.state_dict())
        model.eval()  # Ensure teacher is in eval mode

        # --- Overall Curriculum Early Stopping Logic ---
        # Evaluate the current best student model from this iteration on all dev sets
        # This is essentially re-evaluating the model that was just loaded from model_save_path_in_iter
        _, _, _, _, _, _, _ = evaluate_model(
            model, val_ynacc_dataloader, device, class_weights
        )  # Re-run for current state
        _, _, _, _, _, _, _ = evaluate_model(
            model, val_iac_dataloader, device, class_weights
        )
        _, _, _, _, _, _, _ = evaluate_model(
            model, val_reddit_dataloader, device, class_weights
        )

        # Use the already calculated 'best_val_loss_student_in_iter' as the performance for this curriculum iteration
        current_overall_iteration_val_loss = best_val_loss_student_in_iter  # This is the best combined loss from inner loop

        if current_overall_iteration_val_loss < best_combined_val_loss_overall:
            best_combined_val_loss_overall = current_overall_iteration_val_loss
            epochs_no_improve_overall = 0
            # Save the overall best model
            overall_best_model_save_path = (
                f"best_model_classifier_overall_iter_{iteration}"
            )
            if USE_QLORA:
                model.save_pretrained(
                    overall_best_model_save_path
                )  # Save PEFT adapters
            else:
                torch.save(model.state_dict(), f"{overall_best_model_save_path}.pt")
            print(
                f"Saved OVERALL best model to {overall_best_model_save_path} with Combined Dev Loss: {best_combined_val_loss_overall:.4f}"
            )
        else:
            epochs_no_improve_overall += 1
            print(
                f"No improvement in Overall Combined Dev Loss for {epochs_no_improve_overall} curriculum iterations."
            )
            if epochs_no_improve_overall >= EARLY_STOPPING_PATIENCE:
                print(
                    f"Overall curriculum early stopping triggered after {iteration} iterations."
                )
                break

        # 3. Adjust Curriculum Difficulty
        current_confidence_threshold = max(
            CONFIDENCE_THRESHOLD_END,
            current_confidence_threshold - CONFIDENCE_DECAY_FACTOR,
        )
        print(
            f"Next confidence threshold for pseudo-labeling: {current_confidence_threshold:.2f}"
        )

    print("\n--- Curriculum Training Complete ---")

    print("\n--- Final Evaluation on Test Sets ---")
    if overall_best_model_save_path and os.path.exists(overall_best_model_save_path):
        if USE_QLORA:
            model = AutoModelForSequenceClassification.from_pretrained(
                MODEL_NAME,
                num_labels=2,
                quantization_config=bnb_config,
                device_map=device,
            )
            model.config.pad_token_id = tokenizer.pad_token_id
            model = PeftModel.from_pretrained(model, overall_best_model_save_path)
            # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
            if hasattr(model, "gradient_checkpointing_enable"):
                model.gradient_checkpointing_enable(
                    gradient_checkpointing_kwargs={"use_reentrant": False}
                )
        else:
            # Load the overall best model state dict
            model = AutoModelForSequenceClassification.from_pretrained(
                MODEL_NAME, num_labels=2
            )
            model.config.pad_token_id = tokenizer.pad_token_id
            model.load_state_dict(torch.load(f"{overall_best_model_save_path}.pt"))
        model.eval()  # Set to eval mode for inference
        model.to(device)
        print(
            f"Loaded overall best model from {overall_best_model_save_path} for final test evaluation."
        )
    else:
        print(
            "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for test evaluation."
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
        f"YNACC Final Test Loss: {test_loss_ynacc:.4f}, Test Accuracy: {test_acc_ynacc:.4f}, Test Precision: {test_precision_ynacc:.4f}, Test Recall: {test_recall_ynacc:.4f}, Test F1-score: {test_f1_ynacc:.4f}"
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
        f"IAC Final Test Loss: {test_loss_iac:.4f}, Test Accuracy: {test_acc_iac:.4f}, Test Precision: {test_precision_iac:.4f}, Test Recall: {test_recall_iac:.4f}, Test F1-score: {test_f1_iac:.4f}"
    )

    # Evaluate on Reddit Test Set
    (
        test_loss_reddit,
        test_acc_reddit,
        test_precision_reddit,
        test_recall_reddit,
        test_f1_reddit,
        _,
        _,
    ) = evaluate_model(
        model,
        test_reddit_dataloader,
        device,
        class_weights,
    )
    print(
        f"Reddit Final Test Loss: {test_loss_reddit:.4f}, Test Accuracy: {test_acc_reddit:.4f}, Test Precision: {test_precision_reddit:.4f}, Test Recall: {test_recall_reddit:.4f}, Test F1-score: {test_f1_reddit:.4f}"
    )

    # --- Save Performance Metrics to JSON ---
    performance_metrics = {
        "curriculum_epoch_metrics": {  # Epoch-level metrics for each epoch within each curriculum iteration
            "train_losses": all_curriculum_epoch_train_losses,
            "train_accuracies": all_curriculum_epoch_train_accuracies,
            "val_losses_ynacc": all_curriculum_epoch_val_losses_ynacc,
            "val_accuracies_ynacc": all_curriculum_epoch_val_accuracies_ynacc,
            "val_f1s_ynacc": all_curriculum_epoch_val_f1s_ynacc,
            "val_losses_iac": all_curriculum_epoch_val_losses_iac,
            "val_accuracies_iac": all_curriculum_epoch_val_accuracies_iac,
            "val_f1s_iac": all_curriculum_epoch_val_f1s_iac,
            "val_losses_reddit": all_curriculum_epoch_val_losses_reddit,
            "val_accuracies_reddit": all_curriculum_epoch_val_accuracies_reddit,
            "val_f1s_reddit": all_curriculum_epoch_val_f1s_reddit,
        },
        "in_epoch_batch_metrics": {  # Cumulative batch-wise metrics from train_epoch
            "train_losses": in_epoch_train_losses_batch,
            "train_accuracies": in_epoch_train_accuracies_batch,
            "val_losses_from_train_epoch": in_epoch_val_losses_batch_from_train_epoch,
            "val_accuracies_from_train_epoch": in_epoch_val_accuracies_batch_from_train_epoch,
            "val_f1_scores_from_train_epoch": in_epoch_val_f1_batch_from_train_epoch,
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
            "reddit": {
                "loss": test_loss_reddit,
                "accuracy": test_acc_reddit.item(),
                "precision": test_precision_reddit,
                "recall": test_recall_reddit,
                "f1_score": test_f1_reddit,
            },
        },
        "model_details": {
            "model_name": MODEL_NAME,
            "max_len": MAX_LEN,
            "batch_size": BATCH_SIZE,
            "learning_rate": LEARNING_RATE,
            "epochs_per_curriculum_iteration": STUDENT_TEACHER_EPOCHS_PER_ITERATION,
            "max_curriculum_iterations": MAX_CURRICULUM_ITERATIONS,
            "confidence_threshold_start": CONFIDENCE_THRESHOLD_START,
            "confidence_threshold_end": CONFIDENCE_THRESHOLD_END,
            "confidence_decay_factor": CONFIDENCE_DECAY_FACTOR,
            "unlabeled_data_fraction_per_step": UNLABELED_DATA_FRACTION_PER_STEP,
            "random_seed": RANDOM_SEED,
            "early_stopping_patience": EARLY_STOPPING_PATIENCE,
            "final_overall_best_val_loss": best_combined_val_loss_overall,
            "overall_best_model_saved_path": overall_best_model_save_path,
            "class_weights_used": (
                class_weights.tolist() if class_weights is not None else None
            ),
        },
    }

    os.makedirs(
        os.path.dirname(PERFORMANCE_FILE) or ".", exist_ok=True
    )  # Ensure directory exists, or create in current if no path
    with open(PERFORMANCE_FILE, "w") as f:
        json.dump(performance_metrics, f, indent=4)
    print(f"\nAll performance metrics saved to {PERFORMANCE_FILE}")

    print("\nCurriculum Learning Training and Evaluation Complete.")
