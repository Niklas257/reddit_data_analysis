import json
import time
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
import torch
from torch.utils.data import Dataset, DataLoader, ConcatDataset
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
import gc
import warnings
from classifier_config_kaggle import ClassifierConfig

# Initialize configuration
config = ClassifierConfig()
config.login_to_huggingface()

# Suppress warnings
warnings.filterwarnings("ignore", message=".*torch.utils.checkpoint.*use_reentrant.*")
warnings.filterwarnings("ignore", message=".*WON'T CONVERT.*")
warnings.filterwarnings("ignore", module="torch*")
warnings.filterwarnings("ignore", module="transformers.*")
torch._dynamo.config.suppress_errors = True
torch._dynamo.config.verbose = False

# --- Configuration ---
MODEL_NAME = config.MODEL_NAME
MAX_LEN = config.MAX_LEN
BATCH_SIZE = config.BATCH_SIZE
LEARNING_RATE = config.LEARNING_RATE
RANDOM_SEED = config.RANDOM_SEED
EARLY_STOPPING_PATIENCE = config.EARLY_STOPPING_PATIENCE
OUT_DIR = config.OUT_DIR
PERFORMANCE_FILE = config.PERFORMANCE_FILE
YNACC_FILE_PATH = config.YNACC_FILE_PATH
IAC_FILE_PATH = config.IAC_FILE_PATH
REDDIT_UNLABELED_FILE_PATH = config.REDDIT_UNLABELED_FILE_PATH
REDDIT_VAL_FILE_PATH = config.REDDIT_VAL_FILE_PATH
REDDIT_TEST_FILE_PATH = config.REDDIT_TEST_FILE_PATH

MAX_CURRICULUM_ITERATIONS = config.MAX_CURRICULUM_ITERATIONS
STUDENT_TEACHER_EPOCHS_PER_ITERATION = config.STUDENT_TEACHER_EPOCHS_PER_ITERATION
CONFIDENCE_THRESHOLD_START = config.CONFIDENCE_THRESHOLD_START
CONFIDENCE_THRESHOLD_END = config.CONFIDENCE_THRESHOLD_END
CONFIDENCE_DECAY_FACTOR = config.CONFIDENCE_DECAY_FACTOR
# TODO: Apply Unlabeled data fraction to inference too?
UNLABELED_DATA_FRACTION_PER_STEP = config.UNLABELED_DATA_FRACTION_PER_STEP
PSEUDO_LABEL_BATCH_SIZE = config.PSEUDO_LABEL_BATCH_SIZE

SUPERVISED_TRAINING_ONLY = config.SUPERVISED_TRAINING_ONLY
USE_QLORA = config.USE_QLORA
lora_rank = config.lora_rank
lora_alpha = config.lora_alpha
bnb_bits = config.bnb_bits

# Set random seeds for reproducibility across runs
torch.manual_seed(RANDOM_SEED)
torch.cuda.manual_seed_all(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Configure PyTorch for deterministic behavior (reproducible results)
torch.backends.cudnn.deterministic = (
    True  # Forces cuDNN to use deterministic algorithms (slower but reproducible)
)
torch.backends.cudnn.benchmark = (
    False  # Disables cuDNN's auto-tuning (prevents non-deterministic optimization)
)
torch.set_float32_matmul_precision(
    "high"
)  # Uses higher precision for matrix multiplications (better accuracy, slightly slower)

# Additional CUDA settings to help with stability
if torch.cuda.is_available():
    torch.cuda.empty_cache()
    # Set memory allocation strategy
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True

# Set device (GPU if available, else CPU)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")


def get_target_modules_for_model(model_name):
    """
    Get appropriate LoRA target modules based on the model architecture.
    """
    if "modernbert" in model_name.lower():
        # ModernBERT specific modules - might need adjustment based on actual architecture
        return ["Wqkv", "Wo", "Wi", "dense"]
    elif "llama" in model_name.lower() or "qwen" in model_name.lower():
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
        # Default fallback (BERT-like models)
        print(
            f"Warning: No specific LoRA target modules defined for {model_name}. Using BERT modules."
        )
        return ["query", "value", "key", "dense"]


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
                ["classifier"]
                if "modernbert" in MODEL_NAME.lower()
                else (
                    ["score"]
                    if "llama" in MODEL_NAME.lower() or "qwen" in MODEL_NAME.lower()
                    else None
                )
            ),  # Modules to skip for 4-bit quantization
        )
        print("Using 4-bit quantization with NF4 and double quantization")
    elif bnb_bits == 8:
        bnb_config = BitsAndBytesConfig(
            load_in_8bit=True,
            llm_int8_threshold=6.0,  # Threshold for outlier detection
            llm_int8_skip_modules=(
                ["classifier"]
                if "modernbert" in MODEL_NAME.lower()
                else (
                    ["score"]
                    if "llama" in MODEL_NAME.lower() or "qwen" in MODEL_NAME.lower()
                    else None
                )
            ),  # Modules to skip for 8-bit quantization
        )
        print("Using 8-bit quantization with int8 threshold=6.0")
    else:
        raise ValueError(f"bnb_bits must be 4 or 8, got {bnb_bits}")

    # LoRA configuration
    lora_config = LoraConfig(
        r=lora_rank,
        lora_alpha=lora_alpha,
        target_modules=get_target_modules_for_model(
            MODEL_NAME
        ),  # Auto-detect based on model
        modules_to_save=(
            ["classifier"]
            if "modernbert" in MODEL_NAME.lower()
            else (
                ["score"]
                if "llama" in MODEL_NAME.lower() or "qwen" in MODEL_NAME.lower()
                else None
            )
        ),  # Modules to save in the LoRA model
        lora_dropout=0.1,  # Dropout probability on the LoRA layers.
        bias="none",  # Can be "none", "all", "lora_only"
        task_type="SEQ_CLS",  # Specify the task type
    )
    print(f"LoRA config: rank={lora_rank}, alpha={lora_alpha}, dropout=0.1")


# Uncomment this block to print the model structure after loading
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
# --- Helper Functions ---


def load_jsonl(file_path, tokenizer, max_len, filter_max_len=True):
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
    if not filter_max_len:
        print(
            f"Skipping length filtering for {os.path.basename(file_path)}. All entries will be included."
        )
        return df
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
def load_base_model_and_apply_peft(
    model_name,
    num_labels,
    use_qlora,
    bnb_config,
    lora_config,
    device,
    tokenizer,
    peft_model_path=None,
):
    """
    Loads the base model and applies QLoRA if specified.
    Also adds PEFT adapters if a path is provided.
    """
    print(f"Loading model {model_name}...")
    if use_qlora:
        # Load base model with quantization config
        model = AutoModelForSequenceClassification.from_pretrained(
            model_name,
            num_labels=num_labels,
            quantization_config=bnb_config,
            device_map=({"": 0} if torch.cuda.is_available() else None),
        )
        model.config.pad_token_id = tokenizer.pad_token_id

        # If a PEFT path is provided, load the PEFT adapters from there
        if peft_model_path:
            # Prepare for k-bit training (this function usually wraps the base model)
            # This step is crucial even when loading saved adapters
            model = prepare_model_for_kbit_training(
                model, use_gradient_checkpointing=True
            )
            model = PeftModel.from_pretrained(model, peft_model_path)
            print(f"Loaded PEFT adapters from {peft_model_path}")
        else:
            # If no PEFT path, apply new PEFT adapters for training from scratch (or initial load)
            model = prepare_model_for_kbit_training(
                model, use_gradient_checkpointing=True
            )
            model = get_peft_model(model, lora_config)
            print("Initialized new PEFT adapters.")

        # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
        if hasattr(model, "gradient_checkpointing_enable"):
            model.gradient_checkpointing_enable(
                gradient_checkpointing_kwargs={"use_reentrant": False}
            )
        print("QLoRA enabled. Trainable parameters:")
        model.print_trainable_parameters()
    else:
        # Standard model loading
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
    device,
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
        # Gradient clipping to prevent exploding gradients
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        optimizer.zero_grad()

        # In-epoch evaluation (kept as per your request)
        if (step + 1) % eval_every_steps == 0 or step == len(train_data_loader) - 1:

            # Evaluate on validation set
            val_loss, val_acc, _, _, val_f1, _, _ = evaluate_model(
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


def supervised_training_loop(
    model,
    current_train_dataloader,
    val_ynacc_dataloader,
    val_iac_dataloader,
    val_reddit_dataloader,
    combined_val_dataloader,
    optimizer,
    scheduler,
    device,
    class_weights,
    tokenizer,
    iteration=0,
    phase_description="Supervised Training",
    all_curriculum_epoch_train_losses=None,
    all_curriculum_epoch_train_accuracies=None,
    all_curriculum_epoch_val_losses_ynacc=None,
    all_curriculum_epoch_val_accuracies_ynacc=None,
    all_curriculum_epoch_val_f1s_ynacc=None,
    all_curriculum_epoch_val_losses_iac=None,
    all_curriculum_epoch_val_accuracies_iac=None,
    all_curriculum_epoch_val_f1s_iac=None,
    all_curriculum_epoch_val_losses_reddit=None,
    all_curriculum_epoch_val_accuracies_reddit=None,
    all_curriculum_epoch_val_f1s_reddit=None,
    in_epoch_train_losses_batch=None,
    in_epoch_train_accuracies_batch=None,
    in_epoch_val_losses_batch_from_train_epoch=None,
    in_epoch_val_accuracies_batch_from_train_epoch=None,
    in_epoch_val_f1_batch_from_train_epoch=None,
):
    """
    Perform supervised training for a given number of epochs with early stopping.

    Args:
        model: The model to train
        current_train_dataloader: DataLoader for training data
        val_*_dataloader: DataLoaders for validation sets
        optimizer: Optimizer for training
        scheduler: Learning rate scheduler
        device: Device to use for training
        class_weights: Class weights for loss function
        iteration: Current curriculum iteration (0 for initial training)
        phase_description: Description of the current training phase
        all_*: Lists to collect epoch-level metrics (will be modified in-place)
        in_epoch_*: Lists to collect batch-level metrics (will be modified in-place)

    Returns:
        tuple: (best_val_loss, model_save_path, model)
    """
    # Initialize lists if not provided
    if all_curriculum_epoch_train_losses is None:
        all_curriculum_epoch_train_losses = []
    if all_curriculum_epoch_train_accuracies is None:
        all_curriculum_epoch_train_accuracies = []
    if all_curriculum_epoch_val_losses_ynacc is None:
        all_curriculum_epoch_val_losses_ynacc = []
    if all_curriculum_epoch_val_accuracies_ynacc is None:
        all_curriculum_epoch_val_accuracies_ynacc = []
    if all_curriculum_epoch_val_f1s_ynacc is None:
        all_curriculum_epoch_val_f1s_ynacc = []
    if all_curriculum_epoch_val_losses_iac is None:
        all_curriculum_epoch_val_losses_iac = []
    if all_curriculum_epoch_val_accuracies_iac is None:
        all_curriculum_epoch_val_accuracies_iac = []
    if all_curriculum_epoch_val_f1s_iac is None:
        all_curriculum_epoch_val_f1s_iac = []
    if all_curriculum_epoch_val_losses_reddit is None:
        all_curriculum_epoch_val_losses_reddit = []
    if all_curriculum_epoch_val_accuracies_reddit is None:
        all_curriculum_epoch_val_accuracies_reddit = []
    if all_curriculum_epoch_val_f1s_reddit is None:
        all_curriculum_epoch_val_f1s_reddit = []
    if in_epoch_train_losses_batch is None:
        in_epoch_train_losses_batch = []
    if in_epoch_train_accuracies_batch is None:
        in_epoch_train_accuracies_batch = []
    if in_epoch_val_losses_batch_from_train_epoch is None:
        in_epoch_val_losses_batch_from_train_epoch = []
    if in_epoch_val_accuracies_batch_from_train_epoch is None:
        in_epoch_val_accuracies_batch_from_train_epoch = []
    if in_epoch_val_f1_batch_from_train_epoch is None:
        in_epoch_val_f1_batch_from_train_epoch = []

    # Inner early stopping for student within this iteration and save best model
    best_val_loss_student_in_iter = float("inf")
    epochs_no_improve_student_in_iter = 0
    model_save_path_in_iter = None

    # Training loop for the current curriculum iteration
    for epoch_in_iter in range(STUDENT_TEACHER_EPOCHS_PER_ITERATION):
        print(
            f"\n--- {phase_description} Epoch {epoch_in_iter + 1}/{STUDENT_TEACHER_EPOCHS_PER_ITERATION}"
            + (f" in Iteration {iteration}" if iteration > 0 else "")
            + " ---"
        )

        # Training epoch
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
            combined_val_dataloader,  # Used by train_epoch for batch-level val metrics
            optimizer,
            device,
            eval_every_steps=20,
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

        # Evaluate on YNACC Dev Set
        val_loss_ynacc, val_acc_ynacc, _, _, val_f1_ynacc, _, _ = evaluate_model(
            model, val_ynacc_dataloader, device, class_weights
        )
        print(
            f"YNACC Dev Loss: {val_loss_ynacc:.4f}, Acc: {val_acc_ynacc:.4f}, F1: {val_f1_ynacc:.4f}"
        )

        # Evaluate on IAC Dev Set
        val_loss_iac, val_acc_iac, _, _, val_f1_iac, _, _ = evaluate_model(
            model, val_iac_dataloader, device, class_weights
        )
        print(
            f"IAC Dev Loss: {val_loss_iac:.4f}, Acc: {val_acc_iac:.4f}, F1: {val_f1_iac:.4f}"
        )

        if iteration > 0:
            # Evaluate on Reddit Validation Set
            val_loss_reddit, val_acc_reddit, _, _, val_f1_reddit, _, _ = evaluate_model(
                model, val_reddit_dataloader, device, class_weights
            )
            print(
                f"Reddit Dev Loss: {val_loss_reddit:.4f}, Acc: {val_acc_reddit:.4f}, F1: {val_f1_reddit:.4f}"
            )

        # Calculate combined validation loss based on iteration
        if iteration == 0:
            # For initial training (iteration 0), use only YNACC and IAC
            current_combined_val_loss_for_scheduler = (
                val_loss_ynacc + val_loss_iac
            ) / 2
            loss_description = "Combined Dev Loss (YNACC+IAC)"
        else:
            # For curriculum iterations, use all three dev sets
            current_combined_val_loss_for_scheduler = (
                0.5 * val_loss_ynacc + 0.5 * val_loss_iac + val_loss_reddit
            ) / 2
            loss_description = (
                "Combined Dev Loss (YNACC+IAC+Reddit) (reddit weighted 2x)"
            )

        print(
            f"\nEpoch {epoch_in_iter + 1} Summary - Train Loss: {current_train_loss:.4f}, Train Acc: {current_train_acc:.4f}"
        )
        print(
            f"Epoch {epoch_in_iter + 1} {loss_description}: {current_combined_val_loss_for_scheduler:.4f}"
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
        if iteration > 0:
            # Only append Reddit metrics if this is not the initial training phase
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
            model_save_path_in_iter = (
                f"{OUT_DIR}best_model_iter_{iteration}_epoch_{epoch_in_iter+1}"
            )
            if USE_QLORA:
                model.save_pretrained(model_save_path_in_iter)  # Save PEFT adapters
            else:
                torch.save(
                    model.state_dict(), f"{model_save_path_in_iter}.pt"
                )  # Save full model state_dict

            phase_name = (
                "initial training phase" if iteration == 0 else f"iteration {iteration}"
            )
            print(
                f"Saved best model for {phase_name} to {model_save_path_in_iter} with {loss_description}: {best_val_loss_student_in_iter:.4f}"
            )
        else:
            epochs_no_improve_student_in_iter += 1
            phase_name = (
                "initial training" if iteration == 0 else f"iteration {iteration}"
            )
            print(
                f"No improvement in {loss_description} for student for {epochs_no_improve_student_in_iter} epochs in {phase_name}."
            )
            if epochs_no_improve_student_in_iter >= EARLY_STOPPING_PATIENCE:
                phase_name = (
                    "initial training" if iteration == 0 else f"iteration {iteration}"
                )
                print(
                    f"Inner early stopping triggered during {phase_name} after {epoch_in_iter + 1} epochs."
                )
                break

    # Load the best model from this training phase if it exists
    if model_save_path_in_iter:
        if USE_QLORA:
            model_exists = os.path.exists(model_save_path_in_iter)
            model_path_to_load = model_save_path_in_iter
        else:
            model_exists = os.path.exists(f"{model_save_path_in_iter}.pt")
            model_path_to_load = f"{model_save_path_in_iter}.pt"

        if model_exists:
            if USE_QLORA:
                # To load PEFT adapters back into the 'model' variable
                # We need to re-create the base model and then load adapters
                del model  # Delete current model to free up GPU memory before reloading
                gc.collect()
                torch.cuda.empty_cache()
                # Load the base model and then apply the saved PEFT adapters
                model = load_base_model_and_apply_peft(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    device,
                    tokenizer,
                    peft_model_path=model_save_path_in_iter,
                )

                # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
                if hasattr(model, "gradient_checkpointing_enable"):
                    model.gradient_checkpointing_enable(
                        gradient_checkpointing_kwargs={"use_reentrant": False}
                    )
                print(
                    f"Loaded best QLoRA model from {model_save_path_in_iter} for curriculum phase initialization."
                )
            else:
                # Load the full model state_dict
                model.load_state_dict(torch.load(model_path_to_load))
                model.to(device)
                print(
                    f"Loaded best model from {model_path_to_load} for curriculum phase initialization."
                )
        else:
            phase_name = (
                "initial training"
                if iteration == 0
                else f"Curriculum iteration {iteration}"
            )
            print(
                f"No best model saved from {phase_name}. Continuing with the last trained model from {phase_name}."
            )
    else:
        phase_name = (
            "initial training"
            if iteration == 0
            else f"Curriculum iteration {iteration}"
        )
        print(
            f"No best model saved from {phase_name}. Continuing with the last trained model from {phase_name}."
        )

    return best_val_loss_student_in_iter, model_save_path_in_iter, model


def training():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    # --- Load Data ---
    print("Loading YNACC data...")
    df_ynacc_raw = load_jsonl(YNACC_FILE_PATH, tokenizer, MAX_LEN)
    print("Loading IAC data...")
    df_iac_raw = load_jsonl(IAC_FILE_PATH, tokenizer, MAX_LEN)
    print("Loading Unlabeled Reddit data...")
    df_unlabeled_reddit_raw = load_jsonl(
        REDDIT_UNLABELED_FILE_PATH, tokenizer, MAX_LEN
    )  # No labels needed initially
    print("Loading Reddit Validation data...")
    df_reddit_val = load_jsonl(
        REDDIT_VAL_FILE_PATH, tokenizer, MAX_LEN, filter_max_len=False
    )
    print("Loading Reddit Test data...")
    df_reddit_test = load_jsonl(
        REDDIT_TEST_FILE_PATH, tokenizer, MAX_LEN, filter_max_len=False
    )

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
    # Split remaining YNACC training data into training/validation set
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

    # Combined validation dataset and dataloader
    combined_val_dataset_2 = ConcatDataset([val_ynacc_dataset, val_iac_dataset])
    combined_val_dataloader_2 = DataLoader(
        combined_val_dataset_2, batch_size=BATCH_SIZE, shuffle=False
    )
    combined_val_dataset_3 = ConcatDataset(
        [val_ynacc_dataset, val_iac_dataset, val_reddit_dataset]
    )
    combined_val_dataloader_3 = DataLoader(
        combined_val_dataset_3, batch_size=BATCH_SIZE, shuffle=False
    )

    # DataLoaders for the dedicated test sets (used only at the very end)
    test_ynacc_dataloader = DataLoader(test_ynacc_dataset, batch_size=BATCH_SIZE)
    test_iac_dataloader = DataLoader(test_iac_dataset, batch_size=BATCH_SIZE)
    test_reddit_dataloader = DataLoader(test_reddit_dataset, batch_size=BATCH_SIZE)

    # --- Model Initialization ---
    model = load_base_model_and_apply_peft(
        MODEL_NAME,
        2,
        USE_QLORA,
        bnb_config,
        lora_config,
        device,
        tokenizer,
        peft_model_path=None,
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

    # Call supervised training function for initial training
    best_val_loss_student_in_iter, model_save_path_in_iter, model = (
        supervised_training_loop(
            model=model,
            current_train_dataloader=current_train_dataloader,
            val_ynacc_dataloader=val_ynacc_dataloader,
            val_iac_dataloader=val_iac_dataloader,
            val_reddit_dataloader=val_reddit_dataloader,
            combined_val_dataloader=combined_val_dataloader_2,
            optimizer=optimizer,
            scheduler=scheduler,
            device=device,
            class_weights=class_weights,
            tokenizer=tokenizer,
            iteration=0,
            phase_description="Initial Supervised Training",
            all_curriculum_epoch_train_losses=all_curriculum_epoch_train_losses,
            all_curriculum_epoch_train_accuracies=all_curriculum_epoch_train_accuracies,
            all_curriculum_epoch_val_losses_ynacc=all_curriculum_epoch_val_losses_ynacc,
            all_curriculum_epoch_val_accuracies_ynacc=all_curriculum_epoch_val_accuracies_ynacc,
            all_curriculum_epoch_val_f1s_ynacc=all_curriculum_epoch_val_f1s_ynacc,
            all_curriculum_epoch_val_losses_iac=all_curriculum_epoch_val_losses_iac,
            all_curriculum_epoch_val_accuracies_iac=all_curriculum_epoch_val_accuracies_iac,
            all_curriculum_epoch_val_f1s_iac=all_curriculum_epoch_val_f1s_iac,
            all_curriculum_epoch_val_losses_reddit=all_curriculum_epoch_val_losses_reddit,
            all_curriculum_epoch_val_accuracies_reddit=all_curriculum_epoch_val_accuracies_reddit,
            all_curriculum_epoch_val_f1s_reddit=all_curriculum_epoch_val_f1s_reddit,
            in_epoch_train_losses_batch=in_epoch_train_losses_batch,
            in_epoch_train_accuracies_batch=in_epoch_train_accuracies_batch,
            in_epoch_val_losses_batch_from_train_epoch=in_epoch_val_losses_batch_from_train_epoch,
            in_epoch_val_accuracies_batch_from_train_epoch=in_epoch_val_accuracies_batch_from_train_epoch,
            in_epoch_val_f1_batch_from_train_epoch=in_epoch_val_f1_batch_from_train_epoch,
        )
    )

    # Update the overall best model and path if this initial training achieved a new best
    if best_val_loss_student_in_iter < best_combined_val_loss_overall:
        best_combined_val_loss_overall = best_val_loss_student_in_iter
        overall_best_model_save_path = model_save_path_in_iter
        print(
            f"Overall best model updated after initial training to {overall_best_model_save_path}"
        )

    if not SUPERVISED_TRAINING_ONLY:
        # --- Curriculum Learning Iterations ---
        for iteration in range(1, MAX_CURRICULUM_ITERATIONS + 1):
            print(
                f"\n--- Curriculum Iteration {iteration}/{MAX_CURRICULUM_ITERATIONS} ---"
            )

            # 1. Teacher Pseudo-Labeling
            print(
                f"Teacher pseudo-labeling unlabeled Reddit data with confidence threshold: {current_confidence_threshold:.2f}"
            )
            model.eval()
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
                high_confidence_pseudo_labeled_df["pseudo_label"]
                .value_counts()
                .to_dict()
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
            print(
                f"Current Training DataLoader batches: {len(current_train_dataloader)}"
            )

            # 2. Student Training on Current Curriculum
            # Initialize a new model for the student in this iteration
            model = load_base_model_and_apply_peft(
                MODEL_NAME,
                2,
                USE_QLORA,
                bnb_config,
                lora_config,
                device,
                tokenizer,
                peft_model_path=None,
            )

            optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)
            scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer, mode="min", patience=2
            )

            # Call supervised training function for curriculum training
            best_val_loss_student_in_iter, model_save_path_in_iter, model = (
                supervised_training_loop(
                    model=model,
                    current_train_dataloader=current_train_dataloader,
                    val_ynacc_dataloader=val_ynacc_dataloader,
                    val_iac_dataloader=val_iac_dataloader,
                    val_reddit_dataloader=val_reddit_dataloader,
                    combined_val_dataloader=combined_val_dataloader_3,
                    optimizer=optimizer,
                    scheduler=scheduler,
                    device=device,
                    class_weights=class_weights,
                    tokenizer=tokenizer,
                    iteration=iteration,
                    phase_description="Student Training",
                    all_curriculum_epoch_train_losses=all_curriculum_epoch_train_losses,
                    all_curriculum_epoch_train_accuracies=all_curriculum_epoch_train_accuracies,
                    all_curriculum_epoch_val_losses_ynacc=all_curriculum_epoch_val_losses_ynacc,
                    all_curriculum_epoch_val_accuracies_ynacc=all_curriculum_epoch_val_accuracies_ynacc,
                    all_curriculum_epoch_val_f1s_ynacc=all_curriculum_epoch_val_f1s_ynacc,
                    all_curriculum_epoch_val_losses_iac=all_curriculum_epoch_val_losses_iac,
                    all_curriculum_epoch_val_accuracies_iac=all_curriculum_epoch_val_accuracies_iac,
                    all_curriculum_epoch_val_f1s_iac=all_curriculum_epoch_val_f1s_iac,
                    all_curriculum_epoch_val_losses_reddit=all_curriculum_epoch_val_losses_reddit,
                    all_curriculum_epoch_val_accuracies_reddit=all_curriculum_epoch_val_accuracies_reddit,
                    all_curriculum_epoch_val_f1s_reddit=all_curriculum_epoch_val_f1s_reddit,
                    in_epoch_train_losses_batch=in_epoch_train_losses_batch,
                    in_epoch_train_accuracies_batch=in_epoch_train_accuracies_batch,
                    in_epoch_val_losses_batch_from_train_epoch=in_epoch_val_losses_batch_from_train_epoch,
                    in_epoch_val_accuracies_batch_from_train_epoch=in_epoch_val_accuracies_batch_from_train_epoch,
                    in_epoch_val_f1_batch_from_train_epoch=in_epoch_val_f1_batch_from_train_epoch,
                )
            )

            # --- Overall Curriculum Early Stopping Logic ---
            # Use the already calculated 'best_val_loss_student_in_iter' as the performance for this curriculum iteration
            current_overall_iteration_val_loss = best_val_loss_student_in_iter  # This is the best combined loss from inner loop

            if current_overall_iteration_val_loss < best_combined_val_loss_overall:
                best_combined_val_loss_overall = current_overall_iteration_val_loss
                epochs_no_improve_overall = 0
                # Save the overall best model
                overall_best_model_save_path = (
                    f"{OUT_DIR}best_model_overall_iter_{iteration}"
                )
                if USE_QLORA:
                    model.save_pretrained(
                        overall_best_model_save_path
                    )  # Save PEFT adapters
                else:
                    torch.save(
                        model.state_dict(), f"{overall_best_model_save_path}.pt"
                    )  # Save full model state_dict
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
    # Check for the correct file extension based on USE_QLORA setting
    if overall_best_model_save_path:
        if USE_QLORA:
            model_exists = os.path.exists(overall_best_model_save_path)
            model_path_to_load = overall_best_model_save_path
        else:
            model_exists = os.path.exists(f"{overall_best_model_save_path}.pt")
            model_path_to_load = f"{overall_best_model_save_path}.pt"

        if model_exists:
            if USE_QLORA:
                del model  # Ensure previous model is gone
                gc.collect()
                torch.cuda.empty_cache()
                # Load the base model and then apply the saved PEFT adapters
                model = load_base_model_and_apply_peft(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    device,
                    tokenizer,
                    peft_model_path=overall_best_model_save_path,
                )
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
                model.load_state_dict(torch.load(model_path_to_load))
            model.eval()  # Set to eval mode for inference
            model.to(device)
            print(
                f"Loaded overall best model from {model_path_to_load} for final test evaluation."
            )
        else:
            print(
                "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for test evaluation."
            )
    else:
        print(
            "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for test evaluation."
        )

    # Evaluate the final model on the test sets and measure time taken
    start_time = time.time()
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

    if not SUPERVISED_TRAINING_ONLY:
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
    end_time = time.time()
    print(f"Final evaluation completed in {end_time - start_time:.2f} seconds.")

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
            "val_losses_reddit": all_curriculum_epoch_val_losses_reddit or [],
            "val_accuracies_reddit": all_curriculum_epoch_val_accuracies_reddit or [],
            "val_f1s_reddit": all_curriculum_epoch_val_f1s_reddit or [],
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
            "reddit": (
                {
                    "loss": test_loss_reddit,
                    "accuracy": test_acc_reddit.item(),
                    "precision": test_precision_reddit,
                    "recall": test_recall_reddit,
                    "f1_score": test_f1_reddit,
                }
                if not SUPERVISED_TRAINING_ONLY
                else None
            ),
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
            "use_qlora": USE_QLORA,
            "quantization": bnb_bits,
            "lora_rank": lora_rank,
            "lora_alpha": lora_alpha,
            "final_overall_best_val_loss": best_combined_val_loss_overall,
            "overall_best_model_saved_path": overall_best_model_save_path,
            "class_weights_used": (
                class_weights.tolist() if class_weights is not None else None
            ),
            "evaluation_time_seconds": end_time - start_time,
        },
    }

    os.makedirs(
        os.path.dirname(PERFORMANCE_FILE) or ".", exist_ok=True
    )  # Ensure directory exists, or create in current if no path
    with open(PERFORMANCE_FILE, "w") as f:
        json.dump(performance_metrics, f, indent=4)
    print(f"\nAll performance metrics saved to {PERFORMANCE_FILE}")

    print("\nCurriculum Learning Training and Evaluation Complete.")


training()
