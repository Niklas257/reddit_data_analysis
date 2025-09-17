import json
import time
import sys
import gc
import glob
import re
from datetime import timedelta
import pandas as pd
from sklearn.metrics import (
    precision_recall_fscore_support,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)
from sklearn.model_selection import train_test_split
import torch
from torch.utils.data import Dataset, DataLoader, ConcatDataset
import transformers
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training, PeftModel
import numpy as np
import random
import os
import shutil
import torch.nn as nn
from collections import Counter
import torch._dynamo
import torch.distributed as dist
import torch.multiprocessing as mp
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data.distributed import DistributedSampler
import warnings
from classifier_config_hpc import ClassifierConfig

# Initialize configuration
config = ClassifierConfig()
# Note: HuggingFace login is called inside training function with rank parameter

# Suppress warnings
warnings.filterwarnings("ignore", message=".*torch.utils.checkpoint.*use_reentrant.*")
warnings.filterwarnings("ignore", message=".*WON'T CONVERT.*")
warnings.filterwarnings("ignore", module="torch*")
warnings.filterwarnings("ignore", module="transformers.*")
warnings.filterwarnings("ignore", message=".*Some weights of.*were not initialized.*")
warnings.filterwarnings("ignore", message=".*You should probably TRAIN this model.*")
warnings.filterwarnings("ignore", message=".*Token indices sequence length is longer.*")

# Set transformers logging to ERROR to suppress INFO and WARNING messages
transformers.logging.set_verbosity_error()

torch._dynamo.config.suppress_errors = True
torch._dynamo.config.verbose = False

# --- Configuration ---
MODEL_NAME = config.MODEL_NAME
MAX_LEN = config.MAX_LEN
BATCH_SIZE = config.BATCH_SIZE
LEARNING_RATE = config.LEARNING_RATE
RANDOM_SEED = config.RANDOM_SEED
EARLY_STOPPING_PATIENCE = config.EARLY_STOPPING_PATIENCE

# In-epoch early stopping and LR scheduling
USE_IN_EPOCH_EARLY_STOPPING = config.USE_IN_EPOCH_EARLY_STOPPING
IN_EPOCH_EVAL_STEPS = config.IN_EPOCH_EVAL_STEPS
IN_EPOCH_EARLY_STOPPING_PATIENCE = config.IN_EPOCH_EARLY_STOPPING_PATIENCE
IN_EPOCH_LR_SCHEDULING = config.IN_EPOCH_LR_SCHEDULING
SCHEDULER_PATIENCE = config.SCHEDULER_PATIENCE

OUT_DIR = config.OUT_DIR
PERFORMANCE_FILE = config.PERFORMANCE_FILE
YNACC_FILE_PATH = config.YNACC_FILE_PATH
IAC_FILE_PATH = config.IAC_FILE_PATH
REDDIT_UNLABELED_FILE_PATH = config.REDDIT_UNLABELED_FILE_PATH
REDDIT_VAL_FILE_PATH = config.REDDIT_VAL_FILE_PATH
REDDIT_TEST_FILE_PATH = config.REDDIT_TEST_FILE_PATH

MAX_CURRICULUM_ITERATIONS = config.MAX_CURRICULUM_ITERATIONS
INITIAL_TRAINING_EPOCHS = config.INITIAL_TRAINING_EPOCHS
STUDENT_TEACHER_EPOCHS_PER_ITERATION = config.STUDENT_TEACHER_EPOCHS_PER_ITERATION
CONFIDENCE_THRESHOLD_START = config.CONFIDENCE_THRESHOLD_START
CONFIDENCE_THRESHOLD_END = config.CONFIDENCE_THRESHOLD_END
CONFIDENCE_DECAY_FACTOR = config.CONFIDENCE_DECAY_FACTOR
PSEUDO_LABELING_TEMPERATURE = config.PSEUDO_LABELING_TEMPERATURE
UNLABELED_DATA_FRACTION_PER_STEP = config.UNLABELED_DATA_FRACTION_PER_STEP
PSEUDO_LABEL_BATCH_SIZE = config.PSEUDO_LABEL_BATCH_SIZE

# Checkpoint resumption configuration
CONTINUE_FROM_CHECKPOINT = config.CONTINUE_FROM_CHECKPOINT
STARTING_ITERATION = config.STARTING_ITERATION

# Dynamic threshold configuration
MIN_PSEUDO_SAMPLES_REQUIRED = config.MIN_PSEUDO_SAMPLES_REQUIRED

SUPERVISED_TRAINING_ONLY = config.SUPERVISED_TRAINING_ONLY

# Mean Teacher configuration
USE_MEAN_TEACHER = config.USE_MEAN_TEACHER
EMA_DECAY = config.EMA_DECAY

USE_QLORA = config.USE_QLORA
lora_rank = config.lora_rank
lora_alpha = config.lora_alpha
bnb_bits = config.bnb_bits

# Testing configuration
TESTING_MODE_ONLY = config.TESTING_MODE_ONLY
CHECKPOINT_FOLDERS = config.CHECKPOINT_FOLDERS
TEST_THRESHOLDS = config.TEST_THRESHOLDS

# Annotation configuration
CORPUS_ANNOTATION_MODE = config.CORPUS_ANNOTATION_MODE
ANNOTATION_CORPUS_PATH = config.ANNOTATION_CORPUS_PATH
ANNOTATION_OUTPUT_PATH = config.ANNOTATION_OUTPUT_PATH
ANNOTATION_STARTING_ITERATION = config.ANNOTATION_STARTING_ITERATION
ANNOTATION_CHECKPOINT_FREQUENCY = config.ANNOTATION_CHECKPOINT_FREQUENCY

# Instruction tuning configuration
USE_LANGUAGE_MODEL = config.USE_LANGUAGE_MODEL
INSTRUCTION_MODEL_NAME = config.INSTRUCTION_MODEL_NAME
INSTRUCTION_THINKING_MODE = config.INSTRUCTION_THINKING_MODE
INSTRUCTION_SYSTEM_PROMPT = config.INSTRUCTION_SYSTEM_PROMPT

# Regularization parameters
CLASSIFIER_DROPOUT = config.classifier_dropout
WEIGHT_DECAY = config.weight_decay
BACKBONE_WEIGHT_DECAY = config.backbone_weight_decay

# Focal Loss parameters
FOCAL_LOSS_ALPHA = config.focal_loss_alpha
FOCAL_LOSS_GAMMA = config.focal_loss_gamma

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


class FocalLoss(nn.Module):
    """
    Focal Loss implementation for addressing class imbalance.

    Args:
        alpha: Weighting factor for classes. Can be:
               - None: No additional alpha weighting (relies on class weights only)
               - 'auto': Automatically determine alpha from class weights
               - float: Fixed alpha value for class 1
               - Tensor: Per-class alpha values
        gamma: Focusing parameter (typically 2.0)
        weight: Manual rescaling weight given to each class (class weights)
        reduction: Specifies the reduction to apply to the output
    """

    def __init__(self, alpha="auto", gamma=2.0, weight=None, reduction="mean"):
        super(FocalLoss, self).__init__()
        self.alpha_mode = alpha
        self.gamma = gamma
        self.weight = weight
        self.reduction = reduction

        # Compute alpha based on class weights if alpha='auto'
        if alpha == "auto" and weight is not None:
            # Convert class weights to alpha values
            # Higher weight means minority class, so give it higher alpha
            total_weight = weight.sum()
            normalized_weights = weight / total_weight
            # Alpha should be inverse of normalized weight (minority gets higher alpha)
            self.alpha = 1.0 - normalized_weights
        elif alpha == "auto":
            # No class weights provided, use balanced alpha
            self.alpha = torch.tensor([0.5, 0.5])
        else:
            self.alpha = alpha

    def forward(self, inputs, targets):
        """
        Forward pass for focal loss computation.

        Args:
            inputs: Predictions from model (before softmax) [batch_size, num_classes]
            targets: Ground truth labels [batch_size]
        """
        # Compute cross entropy with class weights
        ce_loss = nn.functional.cross_entropy(
            inputs, targets, weight=self.weight, reduction="none"
        )

        # Compute probabilities
        pt = torch.exp(-ce_loss)

        # Compute alpha weight
        if self.alpha is not None:
            if isinstance(self.alpha, (float, int)):
                # Single alpha value - apply to positive class (class 1)
                alpha_t = torch.where(targets == 1, self.alpha, 1.0 - self.alpha)
            elif torch.is_tensor(self.alpha):
                # Per-class alpha values - ensure alpha tensor is on same device as targets
                alpha_device_tensor = self.alpha.to(targets.device)
                alpha_t = alpha_device_tensor[targets]
            else:
                alpha_t = 1.0
        else:
            alpha_t = 1.0

        # Move alpha_t to same device as other tensors
        if torch.is_tensor(alpha_t):
            alpha_t = alpha_t.to(inputs.device)

        # Compute focal loss
        focal_loss = alpha_t * (1 - pt) ** self.gamma * ce_loss

        if self.reduction == "mean":
            return focal_loss.mean()
        elif self.reduction == "sum":
            return focal_loss.sum()
        else:
            return focal_loss


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


def create_focal_loss(class_weights, device, rank=0):
    """
    Create FocalLoss with automatic alpha calculation and logging.

    Args:
        class_weights: Tensor of class weights or None
        device: Device to move tensors to
        rank: Process rank for logging (only rank 0 logs)

    Returns:
        FocalLoss instance
    """
    loss_fct = FocalLoss(
        alpha=FOCAL_LOSS_ALPHA,  # 'auto' from config
        gamma=FOCAL_LOSS_GAMMA,  # Focusing parameter
        weight=class_weights.to(device) if class_weights is not None else None,
        reduction="mean",
    )

    return loss_fct


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


def print_rank0(message, rank=0):
    """Print message only from rank 0 and flush immediately"""
    if rank == 0:
        print(message, flush=True)


def setup(rank, world_size):
    """Initialize the process group for distributed training"""
    # Use environment variables set by main() function
    master_addr = os.environ.get("MASTER_ADDR", "localhost")
    master_port = os.environ.get("MASTER_PORT", "12355")

    os.environ["MASTER_ADDR"] = master_addr
    os.environ["MASTER_PORT"] = master_port

    # Set longer timeout for distributed operations to handle model reinitialization
    os.environ["TORCH_NCCL_BLOCKING_WAIT"] = (
        "1"  # Updated from deprecated NCCL_BLOCKING_WAIT
    )
    os.environ["NCCL_ASYNC_ERROR_HANDLING"] = "1"

    # Add timeout settings for distributed operations
    os.environ["NCCL_TIMEOUT_MS"] = "300000"  # 5 minutes timeout
    os.environ["NCCL_HEARTBEAT_TIMEOUT_SEC"] = "300"  # 5 minutes heartbeat timeout

    # Initialize the process group with timeout
    try:
        # Use a longer timeout for process group initialization
        dist.init_process_group(
            backend="nccl",
            rank=rank,
            world_size=world_size,
            timeout=timedelta(minutes=30),
        )
        torch.cuda.set_device(rank)

        if rank == 0:
            print(
                f"Successfully initialized process group with {world_size} ranks",
                flush=True,
            )
    except Exception as e:
        print(f"Rank {rank}: Failed to initialize process group: {e}", flush=True)
        raise e

    # Memory optimization for multi-GPU training
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        # Enable memory efficient attention if available (for newer PyTorch versions)
        try:
            torch.backends.cuda.enable_flash_sdp(True)
        except AttributeError:
            pass  # Not available in this PyTorch version


def cleanup():
    """Clean up the process group"""
    try:
        if torch.distributed.is_initialized():
            # Add a final barrier to ensure all ranks complete
            safe_barrier()
            torch.distributed.destroy_process_group()
    except Exception as e:
        print(f"Warning: Error during cleanup: {e}", flush=True)

    # Clear CUDA cache
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


def safe_barrier(timeout_seconds=300):
    """
    Perform a distributed barrier with timeout and error handling.

    Args:
        timeout_seconds: Maximum time to wait for barrier

    Returns:
        bool: True if barrier succeeded, False otherwise
    """
    if not torch.distributed.is_initialized():
        return True

    try:
        # Use a simple barrier - timeouts are handled by NCCL environment variables
        torch.distributed.barrier()
        return True
    except RuntimeError as e:
        error_msg = str(e)
        if (
            "Connection closed by peer" in error_msg
            or "ProcessGroupWrapper" in error_msg
        ):
            print(
                f"WARNING: Distributed barrier failed due to connection issues: {e}",
                flush=True,
            )
            print(
                "This indicates rank 0 may have crashed. Attempting graceful exit...",
                flush=True,
            )
            # Don't try to continue - this usually means rank 0 is gone
            return False
        else:
            print(f"WARNING: Distributed barrier failed: {e}", flush=True)
            return False
    except Exception as e:
        print(f"WARNING: Distributed barrier failed: {e}", flush=True)
        return False


def load_jsonl(file_path, tokenizer, max_len, filter_max_len=True, rank=0):
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
        if rank == 0:
            print(
                f"Error: JSONL data file not found at {file_path}. Please check the path."
            )
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if not filter_max_len:
        if rank == 0:
            print(
                f"Skipping length filtering for {os.path.basename(file_path)}. All entries will be included."
            )
        return df
    initial_count = len(df)
    if rank == 0:
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

    if rank == 0:
        print(
            f"Filtered out {initial_count - filtered_count} entries from {os.path.basename(file_path)} due to length > {max_len} tokens."
        )
        print(
            f"Remaining entries after length filtering in {os.path.basename(file_path)}: {filtered_count}."
        )

    return df_filtered.drop(columns=["token_length"])


def load_corpus_for_annotation(file_path, rank=0):
    """
    Loads JSONL corpus data for annotation without any filtering.
    Preserves all original fields (sdid, text, subreddit, label, etc.)
    """
    data = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line))
    except FileNotFoundError:
        if rank == 0:
            print(
                f"Error: Corpus file not found at {file_path}. Please check the path."
            )
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if rank == 0:
        print(f"Loaded {len(df)} entries from corpus: {os.path.basename(file_path)}")
        print(f"Corpus columns: {list(df.columns)}")

    return df


def validate_peft_files(peft_path):
    """
    Validate that PEFT model files exist and are not corrupted.

    Args:
        peft_path: Path to the PEFT model directory

    Returns:
        bool: True if files are valid, False otherwise
    """
    if not os.path.exists(peft_path):
        return False

    # Check for required files
    adapter_config_path = os.path.join(peft_path, "adapter_config.json")
    adapter_model_path = os.path.join(peft_path, "adapter_model.safetensors")

    if not (os.path.exists(adapter_config_path) and os.path.exists(adapter_model_path)):
        return False

    # Check file sizes
    if not (
        os.path.getsize(adapter_config_path) > 0
        and os.path.getsize(adapter_model_path) > 0
    ):
        return False

    # Try to read the config file to check if it's valid JSON
    try:
        with open(adapter_config_path, "r") as f:
            json.load(f)
    except (json.JSONDecodeError, IOError):
        return False

    # Basic check for safetensors file integrity by trying to read header
    try:
        from safetensors import safe_open

        with safe_open(adapter_model_path, framework="pt", device="cpu") as f:
            # Just try to get the keys to validate the file format
            list(f.keys())
    except Exception:
        return False

    return True


def cleanup_corrupted_peft_files(peft_path, rank=0):
    """
    Clean up corrupted PEFT files.

    Args:
        peft_path: Path to the potentially corrupted PEFT model directory
        rank: Process rank (only rank 0 should perform cleanup)
    """
    if rank == 0 and os.path.exists(peft_path):
        try:
            shutil.rmtree(peft_path)
            print(f"Cleaned up corrupted PEFT files at {peft_path}", flush=True)
        except Exception as e:
            print(
                f"Failed to clean up corrupted PEFT files at {peft_path}: {e}",
                flush=True,
            )


def wait_for_model_file(model_path, max_wait_time=30, check_interval=1):
    """
    Wait for a model file to be completely written and accessible.

    Args:
        model_path: Path to the model file or directory
        max_wait_time: Maximum time to wait in seconds
        check_interval: Time between checks in seconds

    Returns:
        bool: True if file is ready, False if timeout
    """
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        try:
            if os.path.exists(model_path):
                # For PEFT models (directories)
                if os.path.isdir(model_path):
                    if validate_peft_files(model_path):
                        return True
                # For regular model files
                elif os.path.isfile(model_path) and os.path.getsize(model_path) > 0:
                    return True

            time.sleep(check_interval)
        except Exception:
            time.sleep(check_interval)

    return False


# --- Function to load and prepare the model (handles QLoRA dynamically) ---
def load_base_model_and_apply_peft(
    model_name,
    num_labels,
    use_qlora,
    bnb_config,
    lora_config,
    local_rank,  # Changed from device to local_rank
    tokenizer,
    peft_model_path=None,
):
    """
    Loads the base model and applies QLoRA if specified.
    Also adds PEFT adapters if a path is provided.
    DDP-compatible version.
    """
    if local_rank == 0:
        print(f"Loading model {model_name} on rank {local_rank}...", flush=True)

    if use_qlora:
        # Load base model with quantization config
        # Use local_rank for device mapping in DDP
        device_map = {"": local_rank} if torch.cuda.is_available() else None

        model = AutoModelForSequenceClassification.from_pretrained(
            model_name,
            num_labels=num_labels,
            quantization_config=bnb_config,
            device_map=device_map,
        )
        model.config.pad_token_id = tokenizer.pad_token_id

        # Set classifier dropout if the model supports it
        if hasattr(model.config, "classifier_dropout"):
            model.config.classifier_dropout = CLASSIFIER_DROPOUT
        elif hasattr(model.config, "hidden_dropout_prob"):
            # For some models, classifier dropout is controlled by hidden_dropout_prob
            model.config.hidden_dropout_prob = max(
                model.config.hidden_dropout_prob, CLASSIFIER_DROPOUT
            )

        # If a PEFT path is provided, load the PEFT adapters from there
        if peft_model_path:
            model = prepare_model_for_kbit_training(
                model, use_gradient_checkpointing=True
            )

            # Robust PEFT loading with distributed synchronization and retry logic
            peft_loaded = False
            backup_path = f"{peft_model_path}_backup"
            max_retries = 3

            for retry in range(max_retries):
                if retry > 0:
                    if local_rank == 0:
                        print(
                            f"PEFT loading retry {retry}/{max_retries - 1}", flush=True
                        )
                    time.sleep(1 + retry)  # Increasing delay with each retry

                # Synchronize before attempting to load
                if torch.distributed.is_initialized():
                    safe_barrier()

                # Try to load from main path first
                if validate_peft_files(peft_model_path):
                    try:
                        model = PeftModel.from_pretrained(model, peft_model_path)
                        peft_loaded = True
                        if local_rank == 0:
                            print(
                                f"Loaded PEFT adapters from {peft_model_path}",
                                flush=True,
                            )
                        break
                    except Exception as e:
                        if local_rank == 0:
                            print(
                                f"Failed to load PEFT adapters from {peft_model_path}: {e}",
                                flush=True,
                            )
                        # Clean up corrupted files on the last retry
                        if retry == max_retries - 1:
                            cleanup_corrupted_peft_files(peft_model_path, local_rank)
                        continue
                else:
                    if local_rank == 0:
                        print(
                            f"PEFT files at {peft_model_path} are invalid or corrupted",
                            flush=True,
                        )
                    # Clean up corrupted files on the last retry
                    if retry == max_retries - 1:
                        cleanup_corrupted_peft_files(peft_model_path, local_rank)

                # Try backup path if main path failed
                if not peft_loaded and validate_peft_files(backup_path):
                    try:
                        model = PeftModel.from_pretrained(model, backup_path)
                        peft_loaded = True
                        if local_rank == 0:
                            print(
                                f"Loaded PEFT adapters from backup: {backup_path}",
                                flush=True,
                            )
                        break
                    except Exception as e:
                        if local_rank == 0:
                            print(
                                f"Failed to load PEFT adapters from backup {backup_path}: {e}",
                                flush=True,
                            )
                        # Clean up corrupted backup files on the last retry
                        if retry == max_retries - 1:
                            cleanup_corrupted_peft_files(backup_path, local_rank)
                        continue

                # If we get here, both main and backup failed for this retry
                if retry < max_retries - 1:
                    if local_rank == 0:
                        print(
                            "Both main and backup PEFT loading failed, retrying...",
                            flush=True,
                        )

            # Synchronize after loading attempts
            if torch.distributed.is_initialized():
                safe_barrier()

            # If loading failed completely, initialize new adapters
            if not peft_loaded:
                if local_rank == 0:
                    print(
                        "Could not load PEFT adapters from any source after retries. Initializing new adapters.",
                        flush=True,
                    )
                model = get_peft_model(model, lora_config)
        else:
            model = prepare_model_for_kbit_training(
                model, use_gradient_checkpointing=True
            )
            model = get_peft_model(model, lora_config)
            if local_rank == 0:
                print("Initialized new PEFT adapters.", flush=True)

        # Explicitly set gradient checkpointing
        if hasattr(model, "gradient_checkpointing_enable"):
            model.gradient_checkpointing_enable(
                gradient_checkpointing_kwargs={"use_reentrant": False}
            )

        # Only print on rank 0 to avoid spam
        if local_rank == 0:
            print("QLoRA enabled. Trainable parameters:", flush=True)
            model.print_trainable_parameters()
    else:
        # Standard model loading
        model = AutoModelForSequenceClassification.from_pretrained(
            model_name, num_labels=num_labels
        )
        model.config.pad_token_id = tokenizer.pad_token_id

        # Set classifier dropout if the model supports it
        if hasattr(model.config, "classifier_dropout"):
            model.config.classifier_dropout = CLASSIFIER_DROPOUT
        elif hasattr(model.config, "hidden_dropout_prob"):
            # For some models, classifier dropout is controlled by hidden_dropout_prob
            model.config.hidden_dropout_prob = max(
                model.config.hidden_dropout_prob, CLASSIFIER_DROPOUT
            )

        model.to(local_rank)  # Move to specific GPU

    return model


def save_ddp_model(model, save_path, use_qlora=True):
    """
    Helper function to save a DDP-wrapped model correctly with distributed synchronization.

    Args:
        model: DDP-wrapped model
        save_path: Path to save the model
        use_qlora: Whether to use PEFT save method or regular torch.save
    """
    # Synchronize before saving to prevent race conditions
    if torch.distributed.is_initialized():
        safe_barrier()

    if use_qlora:
        # For PEFT/QLoRA models, use robust saving with backup
        temp_save_path = f"{save_path}_temp"
        backup_save_path = f"{save_path}_backup"

        try:
            # Only rank 0 should perform the save operation to prevent conflicts
            if not torch.distributed.is_initialized() or dist.get_rank() == 0:
                # Ensure the parent directory exists before saving
                save_dir = os.path.dirname(save_path)
                if save_dir and not os.path.exists(save_dir):
                    print(f"Creating directory: {save_dir}", flush=True)
                    os.makedirs(save_dir, exist_ok=True)

                # Save to temporary location first
                model.module.save_pretrained(temp_save_path)

                # Create backup of existing model if it exists
                if os.path.exists(save_path):
                    if os.path.exists(backup_save_path):
                        shutil.rmtree(backup_save_path)
                    shutil.move(save_path, backup_save_path)

                # Move temp to final location
                shutil.move(temp_save_path, save_path)

                # Clean up backup after successful save
                if os.path.exists(backup_save_path):
                    shutil.rmtree(backup_save_path)

            # Synchronize after saving to ensure all ranks see the saved model
            if torch.distributed.is_initialized():
                safe_barrier()

        except Exception as e:
            # If save failed, restore from backup if available
            if not torch.distributed.is_initialized() or dist.get_rank() == 0:
                if os.path.exists(backup_save_path) and not os.path.exists(save_path):
                    shutil.move(backup_save_path, save_path)
                # Clean up temp directory if it exists
                if os.path.exists(temp_save_path):
                    shutil.rmtree(temp_save_path)

            # Synchronize after cleanup
            if torch.distributed.is_initialized():
                safe_barrier()
            raise e
    else:
        # For regular models
        try:
            if not torch.distributed.is_initialized() or dist.get_rank() == 0:
                # Ensure the parent directory exists before saving
                save_dir = os.path.dirname(f"{save_path}.pt")
                if save_dir and not os.path.exists(save_dir):
                    print(f"Creating directory: {save_dir}", flush=True)
                    os.makedirs(save_dir, exist_ok=True)

                torch.save(model.module.state_dict(), f"{save_path}.pt")

            # Synchronize after saving
            if torch.distributed.is_initialized():
                safe_barrier()

        except Exception as e:
            # Ensure all ranks reach the barrier even if saving fails
            if torch.distributed.is_initialized():
                safe_barrier()
            raise e


def load_ddp_model(
    model_name,
    num_labels,
    use_qlora,
    bnb_config,
    lora_config,
    rank,
    tokenizer,
    model_path=None,
):
    """
    Helper function to load and wrap a model with DDP correctly.

    Args:
        model_name: Name of the base model
        num_labels: Number of output labels
        use_qlora: Whether to use QLoRA
        bnb_config: BitsAndBytes config
        lora_config: LoRA config
        rank: Current process rank
        tokenizer: Model tokenizer
        model_path: Path to saved model (if loading from checkpoint)

    Returns:
        DDP-wrapped model
    """
    # Load the base model
    model = load_base_model_and_apply_peft(
        model_name,
        num_labels,
        use_qlora,
        bnb_config,
        lora_config,
        rank,
        tokenizer,
        peft_model_path=model_path,
    )

    # Add a small delay to help with synchronization
    time.sleep(0.5)

    # Wrap with DDP - find_unused_parameters=False for better performance
    model = DDP(
        model,
        device_ids=[rank],
        find_unused_parameters=False,
    )

    # Set gradient checkpointing if available
    if hasattr(model.module, "gradient_checkpointing_enable"):
        model.module.gradient_checkpointing_enable(
            gradient_checkpointing_kwargs={"use_reentrant": False}
        )

    return model


def setup_optimizer_with_weight_decay(
    model, learning_rate, weight_decay, backbone_weight_decay
):
    """
    Set up optimizer with different weight decay for different parameter groups.

    Args:
        model: The model to optimize
        learning_rate: Learning rate for all parameters
        weight_decay: Weight decay for classification head parameters
        backbone_weight_decay: Weight decay for backbone parameters

    Returns:
        Optimizer with configured parameter groups
    """
    # Separate parameters into groups
    classifier_params = []
    backbone_params = []

    # Get model without DDP wrapper if needed
    unwrapped_model = model.module if hasattr(model, "module") else model

    # For QLoRA models, we need to handle PEFT parameters differently
    if hasattr(unwrapped_model, "peft_config"):
        # For PEFT models, get trainable parameters
        for name, param in unwrapped_model.named_parameters():
            if param.requires_grad:
                if "classifier" in name or "score" in name:
                    classifier_params.append(param)
                else:
                    backbone_params.append(param)
    else:
        # For regular models, separate classifier from backbone
        for name, param in unwrapped_model.named_parameters():
            if param.requires_grad:
                if "classifier" in name or "score" in name:
                    classifier_params.append(param)
                else:
                    backbone_params.append(param)

    # Create parameter groups
    param_groups = []

    if classifier_params:
        param_groups.append(
            {
                "params": classifier_params,
                "lr": learning_rate,
                "weight_decay": weight_decay,
            }
        )

    if backbone_params:
        param_groups.append(
            {
                "params": backbone_params,
                "lr": learning_rate,
                "weight_decay": backbone_weight_decay,
            }
        )

    # Fallback to all parameters if no specific groups found
    if not param_groups:
        param_groups = [
            {
                "params": model.parameters(),
                "lr": learning_rate,
                "weight_decay": weight_decay,
            }
        ]

    return torch.optim.AdamW(param_groups)


# --- Mean Teacher Functions ---
def create_teacher_model(student_model, rank=0):
    """
    Create a teacher model as a copy of the student model for Mean Teacher approach.

    Args:
        student_model: The student model to copy
        rank: Current process rank (for logging)

    Returns:
        Teacher model (copy of student with frozen parameters)
    """
    import copy

    if rank == 0:
        print("Creating teacher model for Mean Teacher approach...", flush=True)

    # Get the underlying model (unwrap DDP if necessary)
    unwrapped_student = (
        student_model.module if hasattr(student_model, "module") else student_model
    )

    # Create a deep copy of the student model
    teacher_model = copy.deepcopy(unwrapped_student)

    # Freeze all teacher parameters (teacher is only updated via EMA)
    for param in teacher_model.parameters():
        param.requires_grad = False

    # Set teacher to eval mode by default
    teacher_model.eval()

    if rank == 0:
        print("Teacher model created and frozen.", flush=True)

    return teacher_model


def update_teacher_ema(teacher_model, student_model, ema_decay=0.999, rank=0):
    """
    Update teacher model weights using Exponential Moving Average (EMA) of student weights.

    Args:
        teacher_model: Teacher model to update
        student_model: Student model to get weights from
        ema_decay: EMA decay factor (typically 0.999)
        rank: Current process rank (for error handling)
    """
    # Get the underlying student model (unwrap DDP if necessary)
    unwrapped_student = (
        student_model.module if hasattr(student_model, "module") else student_model
    )

    try:
        with torch.no_grad():
            # Update teacher parameters using EMA
            for teacher_param, student_param in zip(
                teacher_model.parameters(), unwrapped_student.parameters()
            ):
                teacher_param.data = (
                    ema_decay * teacher_param.data
                    + (1.0 - ema_decay) * student_param.data
                )
    except Exception as e:
        if rank == 0:
            print(f"WARNING: Failed to update teacher EMA: {e}", flush=True)


def generate_pseudo_labels_with_dynamic_threshold(
    teacher_model,
    unlabeled_dataloader,
    device,
    initial_confidence_threshold,
    min_samples_required=100,
    min_threshold=None,
    decay_factor=None,
    rank=0,
):
    """
    Generate pseudo-labels using the teacher model with dynamic threshold lowering.

    If not enough high-confidence samples are found, the threshold is lowered
    until at least min_samples_required samples are obtained or min_threshold is reached.

    Args:
        teacher_model: Teacher model for generating pseudo-labels
        unlabeled_dataloader: DataLoader containing unlabeled data
        device: Device to run inference on
        initial_confidence_threshold: Starting confidence threshold
        min_samples_required: Minimum number of samples needed (default: 100)
        min_threshold: Minimum threshold allowed (defaults to CONFIDENCE_THRESHOLD_END)
        decay_factor: How much to decrease threshold each step (defaults to CONFIDENCE_DECAY_FACTOR)
        rank: Current process rank (for logging)

    Returns:
        Tuple of (high_conf_texts, high_conf_labels, confidence_scores, final_threshold)
    """
    if min_threshold is None:
        min_threshold = CONFIDENCE_THRESHOLD_END
    if decay_factor is None:
        decay_factor = CONFIDENCE_DECAY_FACTOR

    # First, get all predictions and confidences from the teacher model
    teacher_model.eval()
    all_texts = []
    all_labels = []
    all_confidences = []

    if rank == 0:
        print(
            f"Generating pseudo-labels with dynamic threshold (starting at {initial_confidence_threshold:.3f})...",
            flush=True,
        )

    with torch.no_grad():
        for batch in unlabeled_dataloader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)

            # Get teacher predictions with temperature scaling
            outputs = teacher_model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits

            # Apply temperature scaling to reduce overconfidence
            scaled_logits = logits / PSEUDO_LABELING_TEMPERATURE

            # Convert to probabilities
            probs = torch.softmax(scaled_logits, dim=-1)
            max_probs, predicted_labels = torch.max(probs, dim=-1)

            # Store all predictions
            batch_texts = batch.get("original_texts", [])
            for i in range(len(batch_texts)):
                if i < len(batch_texts):
                    all_texts.append(batch_texts[i])
                    all_labels.append(predicted_labels[i].item())
                    all_confidences.append(max_probs[i].item())

    # Now apply dynamic thresholding
    current_threshold = initial_confidence_threshold
    final_threshold = current_threshold

    while current_threshold >= min_threshold:
        # Apply current threshold
        high_conf_indices = [
            i for i, conf in enumerate(all_confidences) if conf >= current_threshold
        ]

        num_samples = len(high_conf_indices)

        if rank == 0:
            print(f"Threshold {current_threshold:.3f}: Found {num_samples} samples")

        # Check if we have enough samples
        if num_samples >= min_samples_required:
            final_threshold = current_threshold
            break

        # Lower the threshold
        current_threshold = max(min_threshold, current_threshold - decay_factor)
        final_threshold = current_threshold

    # Extract the final high-confidence samples
    high_conf_indices = [
        i for i, conf in enumerate(all_confidences) if conf >= final_threshold
    ]

    high_conf_texts = [all_texts[i] for i in high_conf_indices]
    high_conf_labels = [all_labels[i] for i in high_conf_indices]
    confidence_scores = [all_confidences[i] for i in high_conf_indices]

    if rank == 0:
        print(
            f"Final threshold: {final_threshold:.3f}, Generated {len(high_conf_texts)} pseudo-labels",
            flush=True,
        )
        if final_threshold != initial_confidence_threshold:
            print(
                f"Threshold was lowered from {initial_confidence_threshold:.3f} to {final_threshold:.3f}",
                flush=True,
            )

    return high_conf_texts, high_conf_labels, confidence_scores, final_threshold


def generate_pseudo_labels_with_teacher(
    teacher_model, unlabeled_dataloader, device, confidence_threshold, rank=0
):
    """
    Generate pseudo-labels using the teacher model.

    Args:
        teacher_model: Teacher model for generating pseudo-labels
        unlabeled_dataloader: DataLoader containing unlabeled data
        device: Device to run inference on
        confidence_threshold: Minimum confidence for pseudo-labeling
        rank: Current process rank (for logging)

    Returns:
        Tuple of (high_conf_texts, high_conf_labels, confidence_scores)
    """
    teacher_model.eval()
    high_conf_texts = []
    high_conf_labels = []
    confidence_scores = []

    if rank == 0:
        print(
            f"Generating pseudo-labels with teacher model (threshold={confidence_threshold:.3f})...",
            flush=True,
        )

    with torch.no_grad():
        for batch in unlabeled_dataloader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)

            # Get teacher predictions
            outputs = teacher_model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits

            # Convert to probabilities
            probs = torch.softmax(logits, dim=-1)
            max_probs, predicted_labels = torch.max(probs, dim=-1)

            # Filter by confidence threshold
            high_conf_mask = max_probs >= confidence_threshold

            if high_conf_mask.any():
                # Get original texts for high-confidence predictions
                batch_texts = batch.get("original_texts", [])

                for i, is_high_conf in enumerate(high_conf_mask):
                    if is_high_conf:
                        if i < len(batch_texts):
                            high_conf_texts.append(batch_texts[i])
                            high_conf_labels.append(predicted_labels[i].item())
                            confidence_scores.append(max_probs[i].item())

    if rank == 0:
        print(
            f"Generated {len(high_conf_texts)} pseudo-labels from teacher model",
            flush=True,
        )

    return high_conf_texts, high_conf_labels, confidence_scores


def load_checkpoint_from_iteration(iteration, rank=0):
    """
    Load the best model from a specific curriculum iteration.

    Args:
        iteration: The iteration number to load from (0 for initial training)
        rank: Current process rank (for logging)

    Returns:
        str: Path to the loaded model checkpoint, or None if not found
    """
    if iteration == 0:
        # Load best model from initial training
        checkpoint_pattern = f"{OUT_DIR}best_model_iter_0_epoch_*"
    else:
        # Load best model from specific iteration
        if CORPUS_ANNOTATION_MODE:
            checkpoint_pattern = (
                f"{CHECKPOINT_FOLDERS[0]}best_model_iter_{iteration}_epoch_*"
            )
        else:
            checkpoint_pattern = f"{OUT_DIR}best_model_iter_{iteration}_epoch_*"

    potential_checkpoints = glob.glob(checkpoint_pattern)

    if not potential_checkpoints:
        if rank == 0:
            print(
                f"WARNING: No checkpoint found for iteration {iteration} (pattern: {checkpoint_pattern})"
            )
        return None

    # If multiple checkpoints found, take the one with highest epoch number
    if len(potential_checkpoints) > 1:
        # Extract epoch numbers and find the highest
        epoch_numbers = []
        for path in potential_checkpoints:
            try:
                # Extract epoch number from path like "best_model_iter_0_epoch_25"
                parts = path.split("_epoch_")
                if len(parts) == 2:
                    epoch_num = int(parts[1])
                    epoch_numbers.append((epoch_num, path))
            except (ValueError, IndexError):
                continue

        if epoch_numbers:
            # Sort by epoch number and take the highest
            epoch_numbers.sort(key=lambda x: x[0], reverse=True)
            checkpoint_path = epoch_numbers[0][1]
        else:
            checkpoint_path = potential_checkpoints[0]
    else:
        checkpoint_path = potential_checkpoints[0]

    if rank == 0:
        print(f"Found checkpoint for iteration {iteration}: {checkpoint_path}")

    return checkpoint_path


def save_teacher_model(teacher_model, save_path, use_qlora=True):
    """
    Save teacher model to disk.

    Args:
        teacher_model: Teacher model to save
        save_path: Path to save the model
        use_qlora: Whether to use PEFT save method
    """
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    if use_qlora and hasattr(teacher_model, "save_pretrained"):
        # Save as PEFT model
        teacher_model.save_pretrained(save_path)
    else:
        # Save as regular PyTorch model
        torch.save(teacher_model.state_dict(), save_path + ".pth")


# ================================================
# DATASET CLASSES
# ================================================
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


def convert_numpy_types(obj):
    """
    Recursively convert numpy types to Python native types for JSON serialization.
    """
    if isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif hasattr(obj, "item"):  # Handle any numpy scalar types
        return obj.item()
    else:
        # Check for pandas types if pandas is available
        try:
            if isinstance(obj, pd.Series):
                return obj.tolist()
            elif pd.isna(obj):
                return None
        except (NameError, AttributeError):
            pass
        return obj


def evaluate_model_with_threshold(
    model,
    data_loader,
    device,
    threshold=0.5,
    class_weights=None,
    annotation_only=False,
    output_file_path=None,
    original_data=None,
):
    """
    Evaluates the model on a given data loader with a configurable threshold.
    Returns detailed metrics including TP, FP, TN, FN rates.

    Args:
        model: The classification model
        data_loader: DataLoader containing the data
        device: Device to run on
        threshold: Classification threshold
        class_weights: Class weights for loss calculation
        annotation_only: If True, skip evaluation metrics and just annotate
        output_file_path: If provided (and annotation_only=True), save annotations to this file
        original_data: Original data items for annotation (list of dicts with metadata)
    """
    model.eval()
    losses = []
    all_labels = []
    all_preds = []
    all_probs = []

    # Define loss function - Use Focal Loss with auto-computed alpha
    loss_fct = create_focal_loss(class_weights, device, rank=0)

    with torch.no_grad():
        for batch in data_loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits

            loss = loss_fct(logits, labels)
            losses.append(loss.item())

            # Get probabilities for positive class (class 1)
            probs = torch.softmax(logits, dim=1)[:, 1]  # Probability of class 1

            # Apply threshold to get predictions
            preds = (probs >= threshold).long()

            all_labels.extend(labels.cpu().numpy())
            all_preds.extend(preds.cpu().numpy())
            all_probs.extend(probs.cpu().numpy())

    # For annotation-only mode, create and save annotations
    if annotation_only:
        print("Annotation Summary:")
        print(f"Total predictions: {len(all_preds)}")
        print(f"Predictions == 0: {sum(np.array(all_preds) == 0)}")
        print(f"Predictions == 1: {sum(np.array(all_preds) == 1)}")

        # Calculate correct average confidence: for class 1 use prob, for class 0 use 1-prob
        correct_confidences = [
            all_probs[i] if all_preds[i] == 1 else 1 - all_probs[i]
            for i in range(len(all_preds))
        ]
        print(f"Average confidence: {np.mean(correct_confidences):.4f}")

        # Create annotated data
        annotated_data = []
        for i in range(len(all_preds)):
            # Calculate correct confidence: for class 1 predictions use prob, for class 0 use 1-prob
            confidence = (
                float(all_probs[i]) if all_preds[i] == 1 else float(1 - all_probs[i])
            )

            item = {
                "prediction": int(all_preds[i]),
                "confidence": confidence,
            }
            # Add original data if provided
            if original_data and i < len(original_data):
                item.update(original_data[i])
            annotated_data.append(item)

        # Save to file if path provided
        if output_file_path:
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            with open(output_file_path, "w", encoding="utf-8") as f:
                for item in annotated_data:
                    # Convert numpy types to native Python types for JSON serialization
                    item_converted = convert_numpy_types(item)
                    f.write(json.dumps(item_converted) + "\n")
            print(f"Annotations saved to: {output_file_path}")

        return annotated_data

    # Calculate metrics (evaluation mode)
    avg_loss = np.mean(losses)

    # Convert to numpy arrays for easier calculation
    all_labels = np.array(all_labels)
    all_preds = np.array(all_preds)
    all_probs = np.array(all_probs)

    # Calculate TP, FP, TN, FN
    tp = np.sum((all_labels == 1) & (all_preds == 1))
    fp = np.sum((all_labels == 0) & (all_preds == 1))
    tn = np.sum((all_labels == 0) & (all_preds == 0))
    fn = np.sum((all_labels == 1) & (all_preds == 0))

    # Calculate rates
    total = len(all_labels)
    tp_rate = tp / total if total > 0 else 0
    fp_rate = fp / total if total > 0 else 0
    tn_rate = tn / total if total > 0 else 0
    fn_rate = fn / total if total > 0 else 0

    # Calculate traditional metrics
    accuracy = (tp + tn) / total if total > 0 else 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    return {
        "avg_loss": float(avg_loss),
        "accuracy": float(accuracy),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
        "tp_rate": float(tp_rate),
        "fp_rate": float(fp_rate),
        "tn_rate": float(tn_rate),
        "fn_rate": float(fn_rate),
        "threshold": float(threshold),
        "total_samples": int(total),
    }


def evaluate_instruction_tuned_model(
    model,
    tokenizer,
    dataloader,
    device,
    system_prompt=None,
    use_thinking=True,
    return_annotated_data=False,
    sample_metadata=None,
    annotation_only=False,
    output_file_path=None,
):
    """
    Evaluate an instruction-tuned model with text generation.

    Args:
        model: The instruction-tuned model
        tokenizer: Model tokenizer
        dataloader: DataLoader containing the test data
        device: Device to run on
        system_prompt: System prompt to use
        use_thinking: Whether to use thinking mode
        return_annotated_data: Whether to return the annotated data with predictions
        sample_metadata: Optional list of metadata dicts for each sample (e.g., subreddit info)
        annotation_only: If True, skip evaluation metrics calculation and just annotate
        output_file_path: If provided (and annotation_only=True), save annotations to this file
        sample_metadata: Optional list of metadata dicts for each sample (e.g., subreddit info)

    Returns:
        dict: Evaluation metrics (confusion matrix, accuracy, precision, recall, F1)
        If return_annotated_data=True, also returns annotated_data list
    """
    model.eval()
    all_predictions = []
    all_labels = []
    all_texts = []  # Store original texts for annotation
    debug_count = 0

    with torch.no_grad():
        for batch in dataloader:
            texts = batch["text"]
            labels = batch["labels"].cpu().numpy()

            batch_predictions = []

            for text in texts:
                # Store original text for annotation
                all_texts.append(text)
                # Create a simple, direct prompt with standardized output format
                user_content = f"""Evaluate this discussion and determine if it's constructive or not constructive.

Discussion: {text}

Please analyze the discussion based on the criteria provided in the system prompt and provide your final answer in the following standardized format:
{{"answer": "1"}} for constructive discussions
{{"answer": "0"}} for not constructive discussions

Only output the JSON with your final classification."""

                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ]

                # Apply chat template with proper thinking mode support
                prompt = tokenizer.apply_chat_template(
                    messages,
                    tokenize=False,
                    add_generation_prompt=True,
                    enable_thinking=use_thinking,  # Use official thinking parameter
                )

                # Debug: Print first few prompts to see what's being sent to the model
                if debug_count < 3:
                    print(f"\n=== DEBUG PROMPT {debug_count + 1} ===")
                    print(f"Input text length: {len(text)}")
                    print(f"Full prompt length: {len(prompt)}")
                    print(f"Thinking mode enabled: {use_thinking}")
                    print("Last 500 chars of prompt:")
                    print(prompt[-500:])
                    print("=" * 50)

                # Tokenize
                inputs = tokenizer(
                    prompt,
                    return_tensors="pt",
                    truncation=True,
                    max_length=MAX_LEN,
                    padding=True,
                ).to(device)

                # Generate response with optimized parameters based on thinking mode
                generation_params = {
                    "max_new_tokens": 32768,  # Adequate output length as recommended
                    "pad_token_id": tokenizer.eos_token_id,
                    "eos_token_id": tokenizer.eos_token_id,
                }

                if use_thinking:
                    # Best practices for thinking mode
                    generation_params.update(
                        {
                            "do_sample": True,
                            "temperature": 0.6,
                            "top_p": 0.95,
                            "top_k": 20,
                            "min_p": 0.0,
                        }
                    )
                else:
                    # Best practices for non-thinking mode
                    generation_params.update(
                        {
                            "do_sample": True,
                            "temperature": 0.7,
                            "top_p": 0.8,
                            "top_k": 20,
                            "min_p": 0.0,
                        }
                    )

                with torch.no_grad():
                    outputs = model.generate(**inputs, **generation_params)

                # Extract generated text (only the new tokens)
                output_ids = outputs[0][inputs["input_ids"].shape[1] :].tolist()

                # Parse thinking content if thinking mode is enabled
                if use_thinking:
                    try:
                        # Find the end of thinking token (151668 is </think>)
                        think_end_index = len(output_ids) - output_ids[::-1].index(
                            151668
                        )
                    except ValueError:
                        think_end_index = 0

                    # Extract thinking content and actual response
                    thinking_content = tokenizer.decode(
                        output_ids[:think_end_index], skip_special_tokens=True
                    ).strip()
                    actual_response = tokenizer.decode(
                        output_ids[think_end_index:], skip_special_tokens=True
                    ).strip()

                    # Debug: Print thinking content for first few
                    if debug_count < 10:
                        print(f"\nDEBUG GENERATION {debug_count + 1}:")
                        print(
                            f"Thinking content: '{thinking_content[:200]}{'...' if len(thinking_content) > 200 else ''}'"
                        )
                        print(f"Actual response: '{actual_response}'")
                        print(f"Response length: {len(actual_response)}")
                        debug_count += 1

                    # Use the actual response for parsing
                    generated_text = actual_response
                else:
                    # No thinking mode - use full generated text
                    generated_text = tokenizer.decode(
                        output_ids, skip_special_tokens=True
                    ).strip()

                    # Debug: Print first few generations
                    if debug_count < 10:
                        print(f"\nDEBUG GENERATION {debug_count + 1}:")
                        print(f"Generated text: '{generated_text}'")
                        print(f"Generated text length: {len(generated_text)}")
                        debug_count += 1

                # Parse prediction (extract 0 or 1)
                prediction = parse_label_from_response(generated_text)
                if debug_count <= 10:
                    print(f"Parsed prediction: {prediction}")

                batch_predictions.append(prediction)

            all_predictions.extend(batch_predictions)
            all_labels.extend(labels)

    # Convert to numpy arrays
    all_predictions = np.array(all_predictions)
    all_labels = np.array(all_labels)

    # For annotation-only mode, we might not have true labels
    if annotation_only:
        print("\nAnnotation Summary:")
        print(f"Total predictions: {len(all_predictions)}")
        print(f"Predictions == 0: {sum(all_predictions == 0)}")
        print(f"Predictions == 1: {sum(all_predictions == 1)}")

        # Create annotated data with predictions and confidence (1.0 for instruction tuning)
        annotated_data = []
        for i in range(len(all_texts)):
            item = {
                "text": all_texts[i],
                "label": int(all_predictions[i]),
                "confidence": 1.0,  # Instruction tuning gives discrete predictions
            }
            # Add metadata if provided
            if sample_metadata and i < len(sample_metadata) and sample_metadata[i]:
                item.update(sample_metadata[i])
            annotated_data.append(item)

        # Save to file if path provided
        if output_file_path:
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            with open(output_file_path, "w", encoding="utf-8") as f:
                for item in annotated_data:
                    # Convert numpy types to native Python types for JSON serialization
                    item_converted = convert_numpy_types(item)
                    f.write(json.dumps(item_converted) + "\n")
            print(f"Annotations saved to: {output_file_path}")

        if return_annotated_data:
            return annotated_data
        else:
            return None

    # Print debug summary (for evaluation mode)
    print("\nDEBUG SUMMARY:")
    print(f"Total predictions: {len(all_predictions)}")
    print(f"Predictions == 0: {sum(all_predictions == 0)}")
    print(f"Predictions == 1: {sum(all_predictions == 1)}")
    print(f"True labels == 0: {sum(all_labels == 0)}")
    print(f"True labels == 1: {sum(all_labels == 1)}")

    # Calculate metrics
    accuracy = accuracy_score(all_labels, all_predictions)
    precision = precision_score(
        all_labels, all_predictions, average="binary", zero_division=0
    )
    recall = recall_score(
        all_labels, all_predictions, average="binary", zero_division=0
    )
    f1 = f1_score(all_labels, all_predictions, average="binary", zero_division=0)

    # Calculate confusion matrix components
    cm = confusion_matrix(all_labels, all_predictions, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (0, 0, 0, 0)

    metrics = {
        "accuracy": float(accuracy),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
        "total_samples": int(len(all_labels)),
    }

    # Return annotated data if requested
    if return_annotated_data:
        annotated_data = []
        for i in range(len(all_texts)):
            item = {
                "text": all_texts[i],
                "label": int(all_labels[i]),
                "prediction": int(all_predictions[i]),
            }
            # Add metadata if provided
            if sample_metadata and i < len(sample_metadata) and sample_metadata[i]:
                item.update(sample_metadata[i])
            annotated_data.append(item)
        return metrics, annotated_data

    return metrics


def parse_label_from_response(response_text):
    """
    Parse label (0 or 1) from model response.

    Args:
        response_text: Generated response from the instruction-tuned model

    Returns:
        int: Predicted label (0 or 1), defaults to 0 if parsing fails
    """
    # Clean the response
    response = response_text.strip()

    # Try to parse JSON format first (standardized output)
    try:
        # Look for JSON-like structure
        json_match = re.search(r'\{[^}]*"answer"[^}]*\}', response)
        if json_match:
            json_str = json_match.group(0)
            parsed = json.loads(json_str)
            if "answer" in parsed:
                answer = str(parsed["answer"]).strip()
                if answer == "1":
                    return 1
                elif answer == "0":
                    return 0
    except (json.JSONDecodeError, KeyError):
        pass

    # Fallback to original parsing logic
    response_lower = response.lower().strip()

    # Remove common prefixes/suffixes that might interfere
    response_lower = response_lower.replace("<think>", "").replace("</think>", "")
    response_lower = response_lower.replace("okay, let's see.", "").replace(
        "the user", ""
    )
    response_lower = response_lower.strip()

    # Look for explicit "0" or "1" first (most direct)
    if response_lower == "1":
        return 1
    elif response_lower == "0":
        return 0
    elif response_lower.startswith("1"):
        return 1
    elif response_lower.startswith("0"):
        return 0

    # Look for patterns like "answer: 1" or "verdict: 0"
    number_match = re.search(r"\b([01])\b", response_lower)
    if number_match:
        return int(number_match.group(1))

    # Look for keywords (order matters - check negative first)
    if any(
        word in response_lower
        for word in ["not constructive", "non-constructive", "no", "negative"]
    ):
        return 0
    elif any(word in response_lower for word in ["constructive", "yes", "positive"]):
        return 1

    # Default to 0 if unclear
    return 0


def test_all_checkpoints(
    checkpoint_folder, test_thresholds, tokenizer, device="cuda:0"
):
    """
    Test all model checkpoints in a folder with multiple thresholds.

    Args:
        checkpoint_folder: Path to folder containing model checkpoints
        test_thresholds: List of thresholds to test
        tokenizer: Tokenizer for the model
        device: Device to run evaluation on

    Returns:
        dict: Results for all checkpoints and thresholds with timing information
    """
    overall_start_time = time.time()
    print("Loading test datasets...", flush=True)

    # Determine model type and QLoRA usage from folder name
    folder_name = checkpoint_folder.lower()
    use_qlora = "qlora" in folder_name and "non_qlora" not in folder_name

    # Determine model name from folder
    if "qwen" in folder_name:
        model_name = "Qwen/Qwen3-Embedding-0.6B"
    elif "modernbert" in folder_name:
        model_name = "answerdotai/ModernBERT-base"
    else:
        # Default to current config
        model_name = MODEL_NAME

    print(f"Detected model: {model_name}, QLoRA: {use_qlora}")

    # Load test datasets
    df_reddit_test = load_jsonl(
        REDDIT_TEST_FILE_PATH, tokenizer, MAX_LEN, filter_max_len=False, rank=0
    )

    # Separate test sets if they exist (mimic the training script logic)
    try:
        df_ynacc_raw = load_jsonl(YNACC_FILE_PATH, tokenizer, MAX_LEN, rank=0)
        df_iac_raw = load_jsonl(IAC_FILE_PATH, tokenizer, MAX_LEN, rank=0)

        # Create test sets (100 samples each, same as training)
        df_ynacc_train, df_ynacc_test = train_test_split(
            df_ynacc_raw,
            test_size=100,
            random_state=RANDOM_SEED,
            stratify=df_ynacc_raw["label"],
        )
        df_iac_train, df_iac_test = train_test_split(
            df_iac_raw,
            test_size=100,
            random_state=RANDOM_SEED,
            stratify=df_iac_raw["label"],
        )

        print(f"YNACC test set: {len(df_ynacc_test)} samples")
        print(f"IAC test set: {len(df_iac_test)} samples")

    except Exception as e:
        print(f"Warning: Could not create YNACC/IAC test sets: {e}")
        df_ynacc_test = None
        df_iac_test = None

    print(f"Reddit test set: {len(df_reddit_test)} samples")

    # Create test datasets and dataloaders
    test_datasets = {}
    test_dataloaders = {}

    if df_ynacc_test is not None:
        test_datasets["ynacc"] = CommentDataset(
            df_ynacc_test["text"].tolist(),
            df_ynacc_test["label"].tolist(),
            tokenizer,
            MAX_LEN,
        )
        test_dataloaders["ynacc"] = DataLoader(
            test_datasets["ynacc"], batch_size=BATCH_SIZE
        )

    if df_iac_test is not None:
        test_datasets["iac"] = CommentDataset(
            df_iac_test["text"].tolist(),
            df_iac_test["label"].tolist(),
            tokenizer,
            MAX_LEN,
        )
        test_dataloaders["iac"] = DataLoader(
            test_datasets["iac"], batch_size=BATCH_SIZE
        )

    test_datasets["reddit"] = CommentDataset(
        df_reddit_test["text"].tolist(),
        df_reddit_test["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    test_dataloaders["reddit"] = DataLoader(
        test_datasets["reddit"], batch_size=BATCH_SIZE
    )

    # Find all checkpoint folders
    checkpoint_paths = []
    if os.path.exists(checkpoint_folder):
        for item in os.listdir(checkpoint_folder):
            item_path = os.path.join(checkpoint_folder, item)
            # Check if it's a directory and contains model files
            if os.path.isdir(item_path):
                # For QLoRA models, check for adapter files
                if use_qlora:
                    if validate_peft_files(item_path):
                        checkpoint_paths.append(item_path)
                else:
                    # For regular models, check for .pt files inside the directory
                    # Look for any .pt file in the directory
                    pt_files = [f for f in os.listdir(item_path) if f.endswith(".pt")]
                    if pt_files:
                        checkpoint_paths.append(item_path)
            elif item.endswith(".pt") and not use_qlora:
                # For non-QLoRA models, also check for standalone .pt files
                checkpoint_paths.append(item_path)

    if not checkpoint_paths:
        print(f"No valid checkpoints found in {checkpoint_folder}")
        return {}

    print(f"Found {len(checkpoint_paths)} checkpoints to test")

    # Results storage
    all_results = {}
    timing_info = {}

    # Test each checkpoint
    for checkpoint_path in sorted(checkpoint_paths):
        checkpoint_name = os.path.basename(checkpoint_path)
        print(f"\nTesting checkpoint: {checkpoint_name}")

        checkpoint_start_time = time.time()
        checkpoint_timing = {}

        try:
            # Create model-specific configurations for QLoRA
            if use_qlora:
                # Create QLoRA configs specific to the detected model
                if "qwen" in model_name.lower():
                    target_modules = [
                        "q_proj",
                        "k_proj",
                        "v_proj",
                        "o_proj",
                        "gate_proj",
                        "up_proj",
                        "down_proj",
                    ]
                    modules_to_save = ["score"]
                elif "modernbert" in model_name.lower():
                    target_modules = ["Wqkv", "Wo", "Wi", "dense"]
                    modules_to_save = ["classifier"]
                else:
                    target_modules = ["query", "value", "key", "dense"]
                    modules_to_save = None

                # Create model-specific LoRA config
                model_lora_config = LoraConfig(
                    r=16,
                    lora_alpha=32,
                    target_modules=target_modules,
                    modules_to_save=modules_to_save,
                    lora_dropout=0.1,
                    bias="none",
                    task_type="SEQ_CLS",
                )

                # Create model-specific BitsAndBytes config
                model_bnb_config = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4",
                    bnb_4bit_compute_dtype=torch.bfloat16,
                    llm_int8_skip_modules=modules_to_save,
                )
            else:
                model_lora_config = None
                model_bnb_config = None

            # Load the model
            if use_qlora:
                model = load_base_model_and_apply_peft(
                    model_name,
                    2,
                    use_qlora,
                    model_bnb_config,
                    model_lora_config,
                    0,  # rank 0 for single GPU
                    tokenizer,
                    peft_model_path=checkpoint_path,
                )
            else:
                model = load_base_model_and_apply_peft(
                    model_name,
                    2,
                    False,
                    None,
                    None,
                    0,
                    tokenizer,
                    peft_model_path=None,
                )
                # Handle both directory-based and standalone .pt files
                if os.path.isdir(checkpoint_path):
                    # Find and load the .pt file in the checkpoint directory
                    pt_files = [
                        f for f in os.listdir(checkpoint_path) if f.endswith(".pt")
                    ]
                    if pt_files:
                        pt_file_path = os.path.join(checkpoint_path, pt_files[0])
                        model.load_state_dict(
                            torch.load(pt_file_path, map_location=device)
                        )
                    else:
                        print(f"No .pt file found in {checkpoint_path}")
                        continue
                elif checkpoint_path.endswith(".pt"):
                    # Load standalone .pt file
                    model.load_state_dict(
                        torch.load(checkpoint_path, map_location=device)
                    )
                else:
                    print(f"Invalid checkpoint format: {checkpoint_path}")
                    continue

            model.to(device)
            model.eval()

            # Test with all thresholds on all datasets
            checkpoint_results = {}

            for dataset_name, dataloader in test_dataloaders.items():
                print(f"  Testing on {dataset_name} dataset...")
                dataset_start_time = time.time()
                dataset_results = {}
                dataset_timing = {}

                for threshold in test_thresholds:
                    threshold_start_time = time.time()
                    print(f"    Testing with threshold {threshold}...")

                    metrics = evaluate_model_with_threshold(
                        model, dataloader, device, threshold=threshold
                    )

                    threshold_end_time = time.time()
                    threshold_duration = threshold_end_time - threshold_start_time

                    # Add timing information to metrics
                    metrics["timing"] = {
                        "duration_seconds": round(threshold_duration, 2),
                        "duration_minutes": round(threshold_duration / 60, 2),
                    }

                    dataset_results[f"threshold_{threshold}"] = metrics
                    dataset_timing[f"threshold_{threshold}"] = {
                        "duration_seconds": round(threshold_duration, 2),
                        "duration_minutes": round(threshold_duration / 60, 2),
                    }

                dataset_end_time = time.time()
                dataset_duration = dataset_end_time - dataset_start_time
                dataset_timing["total"] = {
                    "duration_seconds": round(dataset_duration, 2),
                    "duration_minutes": round(dataset_duration / 60, 2),
                }

                checkpoint_results[dataset_name] = dataset_results
                checkpoint_timing[dataset_name] = dataset_timing

            checkpoint_end_time = time.time()
            checkpoint_duration = checkpoint_end_time - checkpoint_start_time
            checkpoint_timing["total"] = {
                "duration_seconds": round(checkpoint_duration, 2),
                "duration_minutes": round(checkpoint_duration / 60, 2),
            }

            all_results[checkpoint_name] = checkpoint_results
            timing_info[checkpoint_name] = checkpoint_timing

            print(
                f"  Checkpoint {checkpoint_name} completed in {checkpoint_duration:.2f}s ({checkpoint_duration/60:.2f}min)"
            )

            # Clean up model to free memory
            del model
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        except Exception as e:
            print(f"Error testing checkpoint {checkpoint_name}: {e}")
            continue

    overall_end_time = time.time()
    overall_duration = overall_end_time - overall_start_time
    timing_info["overall"] = {
        "duration_seconds": round(overall_duration, 2),
        "duration_minutes": round(overall_duration / 60, 2),
        "duration_hours": round(overall_duration / 3600, 2),
    }

    print(
        f"\nAll checkpoint testing completed in {overall_duration:.2f}s ({overall_duration/60:.2f}min)"
    )

    # Combine results and timing
    final_results = {
        "results": all_results,
        "timing": timing_info,
        "model_info": {
            "checkpoint_folder": checkpoint_folder,
            "test_thresholds": test_thresholds,
            "total_checkpoints_tested": len(all_results),
        },
    }

    return final_results


def run_checkpoint_testing():
    """
    Main function to run testing on all checkpoints.
    """
    print("Starting checkpoint testing mode...")

    # Initialize device (single GPU, no distributed training needed)
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Login to HuggingFace
    config.login_to_huggingface(rank=0)

    # Check if we should use instruction tuning
    if USE_LANGUAGE_MODEL:
        print("Using instruction tuning mode with QWEN model")
        print(f"Instruction model: {INSTRUCTION_MODEL_NAME}")
        print(f"Thinking mode: {INSTRUCTION_THINKING_MODE}")
        run_instruction_tuning_testing(device)
        return

    print(f"Testing checkpoints in: {CHECKPOINT_FOLDERS}")
    print(f"Using thresholds: {TEST_THRESHOLDS}")

    for checkpoint_folder in CHECKPOINT_FOLDERS:
        # Determine model type from folder name
        folder_name = checkpoint_folder.lower()
        if "qwen" in folder_name:
            model_name_for_tokenizer = "Qwen/Qwen3-Embedding-0.6B"
        elif "modernbert" in folder_name:
            model_name_for_tokenizer = "answerdotai/ModernBERT-base"
        else:
            # Default to current config
            model_name_for_tokenizer = MODEL_NAME

        # Initialize tokenizer appropriate for this checkpoint folder
        tokenizer = AutoTokenizer.from_pretrained(model_name_for_tokenizer)
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token

        # Run testing
        results = test_all_checkpoints(
            checkpoint_folder, TEST_THRESHOLDS, tokenizer, device
        )

        # Save results
        checkpoint_folder_name = os.path.basename(checkpoint_folder.rstrip("/"))
        results_file = (
            f"../training_data/checkpoint_test_results_{checkpoint_folder_name}.json"
        )

        # Convert numpy types to Python native types for JSON serialization
        results_converted = convert_numpy_types(results)

        with open(results_file, "w") as f:
            json.dump(results_converted, f, indent=4)

        print(f"\nTesting complete! Results saved to: {results_file}")

        # Print timing summary
        if "timing" in results and "overall" in results["timing"]:
            overall_timing = results["timing"]["overall"]
            print(
                f"Overall testing duration: {overall_timing['duration_seconds']}s ({overall_timing['duration_minutes']:.2f}min)"
            )

            if "duration_hours" in overall_timing:
                print(
                    f"                        ({overall_timing['duration_hours']:.2f}h)"
                )

        # Print summary
        print("\nSummary of results:")
        if "results" in results:
            for checkpoint_name, checkpoint_results in results["results"].items():
                print(f"\nCheckpoint: {checkpoint_name}")

                # Print timing for this checkpoint
                if "timing" in results and checkpoint_name in results["timing"]:
                    checkpoint_timing = results["timing"][checkpoint_name]
                    if "total" in checkpoint_timing:
                        total_time = checkpoint_timing["total"]
                        print(
                            f"  Duration: {total_time['duration_seconds']}s ({total_time['duration_minutes']:.2f}min)"
                        )

                for dataset_name, dataset_results in checkpoint_results.items():
                    print(f"  {dataset_name}:")

                    # Print timing for this dataset
                    if (
                        "timing" in results
                        and checkpoint_name in results["timing"]
                        and dataset_name in results["timing"][checkpoint_name]
                    ):
                        dataset_timing = results["timing"][checkpoint_name][
                            dataset_name
                        ]
                        if "total" in dataset_timing:
                            dataset_total_time = dataset_timing["total"]
                            print(
                                f"    Dataset duration: {dataset_total_time['duration_seconds']}s ({dataset_total_time['duration_minutes']:.2f}min)"
                            )

                    for threshold_name, metrics in dataset_results.items():
                        f1 = metrics["f1"]
                        accuracy = metrics["accuracy"]

                        # Print timing for this threshold
                        timing_str = ""
                        if "timing" in metrics:
                            timing = metrics["timing"]
                            timing_str = f" (in {timing['duration_seconds']}s)"

                        print(
                            f"    {threshold_name}: F1={f1:.4f}, Acc={accuracy:.4f}{timing_str}"
                        )
        else:
            # Fallback for old format without timing
            for checkpoint_name, checkpoint_results in results.items():
                print(f"\nCheckpoint: {checkpoint_name}")
                for dataset_name, dataset_results in checkpoint_results.items():
                    print(f"  {dataset_name}:")
                    for threshold_name, metrics in dataset_results.items():
                        f1 = metrics["f1"]
                        accuracy = metrics["accuracy"]
                        print(f"    {threshold_name}: F1={f1:.4f}, Acc={accuracy:.4f}")


def run_instruction_tuning_testing(device):
    """
    Test instruction-tuned QWEN model on the test datasets.

    Args:
        device: Device to run testing on
    """

    overall_start_time = time.time()
    print("Loading instruction-tuned QWEN model...")

    # Load the instruction-tuned model and tokenizer
    tokenizer = AutoTokenizer.from_pretrained(INSTRUCTION_MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        INSTRUCTION_MODEL_NAME, torch_dtype=torch.bfloat16, device_map="auto"
    )
    model.eval()

    print("Loading test datasets...")

    # Load test datasets (same as in regular testing)
    df_reddit_test = load_jsonl(
        REDDIT_TEST_FILE_PATH, tokenizer, MAX_LEN, filter_max_len=False, rank=0
    )

    # Separate test sets if they exist
    try:
        df_ynacc_raw = load_jsonl(YNACC_FILE_PATH, tokenizer, MAX_LEN, rank=0)
        df_iac_raw = load_jsonl(IAC_FILE_PATH, tokenizer, MAX_LEN, rank=0)

        # Create test sets (100 samples each, same as training)
        df_ynacc_train, df_ynacc_test = train_test_split(
            df_ynacc_raw,
            test_size=100,
            stratify=df_ynacc_raw["label"],
            random_state=RANDOM_SEED,
        )
        df_iac_train, df_iac_test = train_test_split(
            df_iac_raw,
            test_size=100,
            stratify=df_iac_raw["label"],
            random_state=RANDOM_SEED,
        )

        print(f"YNACC test set: {len(df_ynacc_test)} samples")
        print(f"IAC test set: {len(df_iac_test)} samples")

    except Exception as e:
        print(f"Warning: Could not create YNACC/IAC test sets: {e}")
        df_ynacc_test = None
        df_iac_test = None

    print(f"Reddit test set: {len(df_reddit_test)} samples")

    # Create test datasets and dataloaders
    test_datasets = {}
    test_dataloaders = {}

    if df_ynacc_test is not None:
        test_datasets["ynacc"] = CommentDataset(
            df_ynacc_test["text"].tolist(),
            df_ynacc_test["label"].tolist(),
            tokenizer,
            MAX_LEN,
        )
        test_dataloaders["ynacc"] = DataLoader(
            test_datasets["ynacc"], batch_size=BATCH_SIZE
        )

    if df_iac_test is not None:
        test_datasets["iac"] = CommentDataset(
            df_iac_test["text"].tolist(),
            df_iac_test["label"].tolist(),
            tokenizer,
            MAX_LEN,
        )
        test_dataloaders["iac"] = DataLoader(
            test_datasets["iac"], batch_size=BATCH_SIZE
        )

    test_datasets["reddit"] = CommentDataset(
        df_reddit_test["text"].tolist(),
        df_reddit_test["label"].tolist(),
        tokenizer,
        MAX_LEN,
    )
    test_dataloaders["reddit"] = DataLoader(
        test_datasets["reddit"], batch_size=BATCH_SIZE
    )

    # Test the instruction-tuned model on all datasets with both thinking modes
    results = {}
    timing_info = {}

    for thinking_mode in [True, False]:
        thinking_mode_key = "with_thinking" if thinking_mode else "without_thinking"
        print(f"\n{'='*60}")
        print(f"Testing with thinking mode: {thinking_mode}")
        print(f"{'='*60}")

        thinking_start_time = time.time()
        results[thinking_mode_key] = {}
        timing_info[thinking_mode_key] = {}

        for dataset_name, dataloader in test_dataloaders.items():
            print(f"\nTesting on {dataset_name} dataset (thinking={thinking_mode})...")

            dataset_start_time = time.time()

            # Create metadata for the dataset (additional fields beyond text/label)
            sample_metadata = None
            if dataset_name == "reddit":
                # Extract metadata for Reddit data (subreddit, sdid, and any other fields)
                sample_metadata = []
                for i in range(len(df_reddit_test)):
                    metadata = {}
                    # Add any additional fields that aren't 'text' or 'label'
                    for col in df_reddit_test.columns:
                        if col not in ["text", "label"]:
                            value = df_reddit_test.iloc[i][col]
                            # Convert to native Python type to avoid JSON serialization issues
                            if hasattr(value, "item"):  # numpy scalar
                                metadata[col] = value.item()
                            else:
                                metadata[col] = (
                                    str(value) if value is not None else None
                                )
                    sample_metadata.append(metadata)
            elif dataset_name == "ynacc" and df_ynacc_test is not None:
                # Extract metadata for YNACC data if any additional fields exist
                sample_metadata = []
                for i in range(len(df_ynacc_test)):
                    metadata = {}
                    # Add any additional fields that aren't 'text' or 'label'
                    for col in df_ynacc_test.columns:
                        if col not in ["text", "label"]:
                            value = df_ynacc_test.iloc[i][col]
                            # Convert to native Python type
                            if hasattr(value, "item"):  # numpy scalar
                                metadata[col] = value.item()
                            else:
                                metadata[col] = value
                    (
                        sample_metadata.append(metadata)
                        if metadata
                        else sample_metadata.append({})
                    )
            elif dataset_name == "iac" and df_iac_test is not None:
                # Extract metadata for IAC data if any additional fields exist
                sample_metadata = []
                for i in range(len(df_iac_test)):
                    metadata = {}
                    # Add any additional fields that aren't 'text' or 'label'
                    for col in df_iac_test.columns:
                        if col not in ["text", "label"]:
                            value = df_iac_test.iloc[i][col]
                            # Convert to native Python type
                            if hasattr(value, "item"):  # numpy scalar
                                metadata[col] = value.item()
                            else:
                                metadata[col] = value
                    (
                        sample_metadata.append(metadata)
                        if metadata
                        else sample_metadata.append({})
                    )

            # Get metrics and annotated data
            metrics, annotated_data = evaluate_instruction_tuned_model(
                model,
                tokenizer,
                dataloader,
                device,
                system_prompt=INSTRUCTION_SYSTEM_PROMPT,
                use_thinking=thinking_mode,
                return_annotated_data=True,
                sample_metadata=sample_metadata,
            )

            # Save annotated data to file
            thinking_suffix = "with_thinking" if thinking_mode else "without_thinking"
            annotated_file = f"../training_data/inst_annotated_data_{dataset_name}_{thinking_suffix}.jsonl"

            # Ensure directory exists
            os.makedirs(os.path.dirname(annotated_file), exist_ok=True)

            with open(annotated_file, "w") as f:
                for item in annotated_data:
                    # Convert numpy types to native Python types for JSON serialization
                    item_converted = convert_numpy_types(item)
                    f.write(json.dumps(item_converted) + "\n")

            print(
                f"  Saved {len(annotated_data)} annotated samples to: {annotated_file}"
            )

            dataset_end_time = time.time()
            dataset_duration = dataset_end_time - dataset_start_time
            timing_info[thinking_mode_key][dataset_name] = {
                "duration_seconds": round(dataset_duration, 2),
                "duration_minutes": round(dataset_duration / 60, 2),
                "samples_per_second": round(
                    len(test_datasets[dataset_name]) / dataset_duration, 2
                ),
            }

            results[thinking_mode_key][dataset_name] = {"instruction_tuning": metrics}

            print(f"  {dataset_name} results (thinking={thinking_mode}):")
            print(f"    Accuracy: {metrics['accuracy']:.4f}")
            print(f"    Precision: {metrics['precision']:.4f}")
            print(f"    Recall: {metrics['recall']:.4f}")
            print(f"    F1-score: {metrics['f1']:.4f}")
            print(
                f"    Duration: {dataset_duration:.2f}s ({dataset_duration/60:.2f}min)"
            )
            print(
                f"    Speed: {len(test_datasets[dataset_name])/dataset_duration:.2f} samples/sec"
            )

        thinking_end_time = time.time()
        thinking_duration = thinking_end_time - thinking_start_time
        timing_info[thinking_mode_key]["total"] = {
            "duration_seconds": round(thinking_duration, 2),
            "duration_minutes": round(thinking_duration / 60, 2),
        }

    overall_end_time = time.time()
    overall_duration = overall_end_time - overall_start_time
    timing_info["overall"] = {
        "duration_seconds": round(overall_duration, 2),
        "duration_minutes": round(overall_duration / 60, 2),
        "duration_hours": round(overall_duration / 3600, 2),
    }

    # Combine results and timing
    final_results = {
        "results": results,
        "timing": timing_info,
        "generation_params": {
            "thinking_mode": {
                "temperature": 0.6,
                "top_p": 0.95,
                "top_k": 20,
                "min_p": 0.0,
                "max_new_tokens": 32768,
            },
            "non_thinking_mode": {
                "temperature": 0.7,
                "top_p": 0.8,
                "top_k": 20,
                "min_p": 0.0,
                "max_new_tokens": 32768,
            },
        },
        "model_info": {
            "model_name": INSTRUCTION_MODEL_NAME,
            "batch_size": BATCH_SIZE,
            "max_length": MAX_LEN,
        },
    }

    # Save results
    results_file = "../training_data/instruction_tuning_test_results.json"

    # Convert numpy types to Python native types for JSON serialization
    results_converted = convert_numpy_types(final_results)

    with open(results_file, "w") as f:
        json.dump(results_converted, f, indent=4)

    print(f"\nInstruction tuning testing complete! Results saved to: {results_file}")

    # Print final summary comparing both modes
    print("\nFinal Summary Comparison:")
    print(
        f"Overall testing duration: {overall_duration:.2f}s ({overall_duration/60:.2f}min)"
    )

    for dataset_name in test_dataloaders.keys():
        print(f"\n{dataset_name.upper()} Dataset:")

        if "with_thinking" in results and dataset_name in results["with_thinking"]:
            metrics_thinking = results["with_thinking"][dataset_name][
                "instruction_tuning"
            ]
            timing_thinking = timing_info["with_thinking"][dataset_name]
            print("  With Thinking:")
            print(f"    Accuracy: {metrics_thinking['accuracy']:.4f}")
            print(f"    Precision: {metrics_thinking['precision']:.4f}")
            print(f"    Recall: {metrics_thinking['recall']:.4f}")
            print(f"    F1-score: {metrics_thinking['f1']:.4f}")
            print(f"    Duration: {timing_thinking['duration_seconds']}s")
            print(f"    Speed: {timing_thinking['samples_per_second']} samples/sec")

        if (
            "without_thinking" in results
            and dataset_name in results["without_thinking"]
        ):
            metrics_no_thinking = results["without_thinking"][dataset_name][
                "instruction_tuning"
            ]
            timing_no_thinking = timing_info["without_thinking"][dataset_name]
            print("  Without Thinking:")
            print(f"    Accuracy: {metrics_no_thinking['accuracy']:.4f}")
            print(f"    Precision: {metrics_no_thinking['precision']:.4f}")
            print(f"    Recall: {metrics_no_thinking['recall']:.4f}")
            print(f"    F1-score: {metrics_no_thinking['f1']:.4f}")
            print(f"    Duration: {timing_no_thinking['duration_seconds']}s")
            print(f"    Speed: {timing_no_thinking['samples_per_second']} samples/sec")


def train_epoch(
    model,
    train_data_loader,
    val_data_loader,  # Kept for backward compatibility, but we'll use individual dataloaders
    optimizer,
    scheduler,  # Add scheduler parameter for in-epoch LR scheduling
    device,
    rank=0,  # Add rank parameter for output control
    eval_every_steps=20,
    class_weights=None,
    train_sampler=None,
    epoch=None,
    best_val_loss_in_epoch=None,  # For in-epoch early stopping
    epochs_no_improve_in_epoch=0,  # For in-epoch early stopping
    val_ynacc_dataloader=None,  # Individual validation dataloaders for weighted loss
    val_iac_dataloader=None,
    val_reddit_dataloader=None,
    iteration=0,  # Current curriculum iteration
    epoch_in_iter=0,  # Current epoch within iteration
    # New parameters for immediate model saving
    save_best_model_immediately=False,  # Whether to save model immediately when best val loss is found
    model_save_path_template=None,  # Template for model save path (e.g., "path/best_model_iter_{iteration}_epoch_{epoch}")
    use_qlora=True,  # Whether to use QLoRA saving
    # Mean Teacher parameters
    teacher_model=None,  # Teacher model for Mean Teacher approach (None if not using)
    use_mean_teacher=False,  # Whether to use Mean Teacher
    ema_decay=0.999,  # EMA decay factor for teacher updates
):
    """Performs one training epoch with in-epoch evaluation."""
    # Set epoch for distributed sampler to ensure proper shuffling
    if train_sampler is not None and epoch is not None:
        train_sampler.set_epoch(epoch)

    model.train()
    losses = []
    correct_predictions = 0

    # Store metrics for plotting later
    train_losses_batch = []
    train_accuracies_batch = []
    val_losses_batch = []
    val_accuracies_batch = []
    val_f1_batch = []
    val_precision_batch = []
    val_recall_batch = []

    # Keep track of total samples processed for accurate in-epoch accuracy
    total_samples_processed_in_epoch = 0

    # In-epoch early stopping variables
    if best_val_loss_in_epoch is None:
        best_val_loss_in_epoch = float("inf")
    current_epochs_no_improve = epochs_no_improve_in_epoch
    early_stop_triggered = False

    # Track the best model path for immediate saving
    best_model_path_in_epoch = None

    # Define loss function - Use Focal Loss with auto-computed alpha
    loss_fct = create_focal_loss(class_weights, device, rank=rank)
    # Log the computed alpha values for debugging
    if rank == 0 and hasattr(loss_fct, "alpha") and torch.is_tensor(loss_fct.alpha):
        print(f"Focal Loss - Computed alpha values: {loss_fct.alpha.tolist()}")
        print(f"Focal Loss - Gamma: {loss_fct.gamma}")
        if class_weights is not None:
            print(f"Focal Loss - Class weights: {class_weights.tolist()}")

    if rank == 0:
        print(
            "Step      | Train Loss | Train Acc | Val Loss | Val Acc | Val Prec | Val Rec| Val F1 |"
        )
    for step, batch in enumerate(train_data_loader):
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["labels"].to(device)

        # Validate input tensors
        if not torch.all(torch.isfinite(input_ids.float())):
            if rank == 0:
                print(
                    f"WARNING: Non-finite input_ids detected at step {step}. Skipping batch.",
                    flush=True,
                )
            continue

        if not torch.all(torch.isfinite(attention_mask.float())):
            if rank == 0:
                print(
                    f"WARNING: Non-finite attention_mask detected at step {step}. Skipping batch.",
                    flush=True,
                )
            continue

        try:
            # Forward pass: get logits
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits

            # Compute loss using defined loss_fct
            loss = loss_fct(logits, labels)

            # Check for numerical issues
            if not torch.isfinite(loss):
                if rank == 0:
                    print(
                        f"WARNING: Non-finite loss detected: {loss.item()}", flush=True
                    )
                # Skip this batch to avoid crashing
                continue

            losses.append(loss.item())

            _, preds = torch.max(logits, dim=1)
            correct_predictions += torch.sum(preds == labels)
            total_samples_processed_in_epoch += labels.size(0)

            # Backward pass with error handling
            loss.backward()

            # Check for gradient issues
            total_norm = 0
            param_count = 0
            for p in model.parameters():
                if p.grad is not None:
                    param_norm = p.grad.data.norm(2)
                    total_norm += param_norm.item() ** 2
                    param_count += 1
            total_norm = total_norm ** (1.0 / 2)

            if not torch.isfinite(torch.tensor(total_norm)):
                if rank == 0:
                    print(
                        "WARNING: Non-finite gradients detected. Skipping update.",
                        flush=True,
                    )
                optimizer.zero_grad()
                continue

            # Gradient clipping to prevent exploding gradients
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            optimizer.zero_grad()

            # Update teacher model via EMA if using Mean Teacher
            if use_mean_teacher and teacher_model is not None:
                update_teacher_ema(teacher_model, model, ema_decay, rank)

        except RuntimeError as e:
            if rank == 0:
                print(f"WARNING: Training step failed: {e}", flush=True)
                print("Attempting to recover...", flush=True)

            # Clear gradients and try to recover
            optimizer.zero_grad()
            torch.cuda.empty_cache()

            # Skip this batch
            continue

        except Exception as e:
            if rank == 0:
                print(f"ERROR: Unexpected error in training step: {e}", flush=True)

            # Ensure all ranks reach the same point
            if torch.distributed.is_initialized():
                safe_barrier()
            raise e

        # Periodic model synchronization check (every 50 steps)
        if (step + 1) % 50 == 0 and torch.distributed.is_initialized():
            try:
                # Quick synchronization check to ensure all ranks are still in sync
                dummy_tensor = torch.tensor(float(step), device=device)
                dist.all_reduce(dummy_tensor, op=dist.ReduceOp.SUM)
                expected_sum = float(step * dist.get_world_size())

                if abs(dummy_tensor.item() - expected_sum) > 1e-6:
                    if rank == 0:
                        print(
                            f"WARNING: Rank synchronization issue detected at step {step}",
                            flush=True,
                        )
            except Exception:
                pass  # Don't let sync checks crash training

        # In-epoch evaluation (kept as per your request)
        if (step + 1) % IN_EPOCH_EVAL_STEPS == 0 or step == len(train_data_loader) - 1:

            # Calculate weighted validation loss like the end-of-epoch logic
            if (USE_IN_EPOCH_EARLY_STOPPING or IN_EPOCH_LR_SCHEDULING) and all(
                [
                    val_ynacc_dataloader is not None,
                    val_iac_dataloader is not None,
                    val_reddit_dataloader is not None,
                ]
            ):
                try:
                    # Evaluate on individual validation sets for weighted loss
                    (
                        val_loss_ynacc,
                        val_acc_ynacc,
                        val_prec_ynacc,
                        val_rec_ynacc,
                        val_f1_ynacc,
                        _,
                        _,
                    ) = evaluate_model(
                        model, val_ynacc_dataloader, device, class_weights
                    )
                    (
                        val_loss_iac,
                        val_acc_iac,
                        val_prec_iac,
                        val_rec_iac,
                        val_f1_iac,
                        _,
                        _,
                    ) = evaluate_model(model, val_iac_dataloader, device, class_weights)
                    (
                        val_loss_reddit,
                        val_acc_reddit,
                        val_prec_reddit,
                        val_rec_reddit,
                        val_f1_reddit,
                        _,
                        _,
                    ) = evaluate_model(
                        model, val_reddit_dataloader, device, class_weights
                    )

                    # Check for numerical issues in validation metrics
                    if not all(
                        torch.isfinite(
                            torch.tensor(
                                [val_loss_ynacc, val_loss_iac, val_loss_reddit]
                            )
                        )
                    ):
                        if rank == 0:
                            print(
                                "WARNING: Non-finite validation loss detected. Skipping in-epoch evaluation.",
                                flush=True,
                            )
                        continue

                    # Calculate weighted validation loss (same as end-of-epoch logic)
                    # Reddit gets 2x weight: (0.5 * ynacc + 0.5 * iac + reddit) / 2
                    weighted_val_loss = (
                        0.5 * val_loss_ynacc + 0.5 * val_loss_iac + val_loss_reddit
                    ) / 2

                    # Use weighted loss for display and logic
                    val_loss = weighted_val_loss
                    val_acc = (
                        0.5 * val_acc_ynacc + 0.5 * val_acc_iac + val_acc_reddit
                    ) / 2  # Average accuracy
                    val_f1 = (
                        0.5 * val_f1_ynacc + 0.5 * val_f1_iac + val_f1_reddit
                    ) / 2  # Average F1
                    val_precision = (
                        0.5 * val_prec_ynacc + 0.5 * val_prec_iac + val_prec_reddit
                    ) / 2  # Average precision
                    val_recall = (
                        0.5 * val_rec_ynacc + 0.5 * val_rec_iac + val_rec_reddit
                    ) / 2  # Average recall

                except Exception as e:
                    if rank == 0:
                        print(
                            f"WARNING: In-epoch evaluation failed: {e}. Skipping.",
                            flush=True,
                        )
                    # Clear any CUDA memory and continue
                    torch.cuda.empty_cache()
                    continue

                if rank == 0:
                    print(
                        f"{step + 1:04d}/{len(train_data_loader):04d} | {np.mean(losses[-IN_EPOCH_EVAL_STEPS:]):.4f}     | "
                        f"{(correct_predictions.double() / total_samples_processed_in_epoch).item():.4f}    | "
                        f"{val_loss:.4f}   | {val_acc:.4f}  |  {val_precision:.4f}  | {val_recall:.4f} | {val_f1:.4f} | {"(weighted)" if SUPERVISED_TRAINING_ONLY else ""}  "
                    )
                    print(
                        f"    → Individual losses: YNACC={val_loss_ynacc:.4f}, IAC={val_loss_iac:.4f}, Reddit={val_loss_reddit:.4f}"
                    )
            else:
                # Fallback to single validation set evaluation
                val_loss, val_acc, val_precision, val_recall, val_f1, _, _ = (
                    evaluate_model(
                        model,
                        val_data_loader,
                        device,
                        class_weights,
                    )
                )

                if rank == 0:
                    print(
                        f"{step + 1:04d}/{len(train_data_loader):04d} | {np.mean(losses[-IN_EPOCH_EVAL_STEPS:]):.4f}     | "
                        f"{(correct_predictions.double() / total_samples_processed_in_epoch).item():.4f}    | "
                        f"{val_loss:.4f}   | {val_acc:.4f}  |  {val_precision:.4f}  | {val_recall:.4f} | {val_f1:.4f}"
                    )

            # In-epoch early stopping logic
            if USE_IN_EPOCH_EARLY_STOPPING:
                if val_loss < best_val_loss_in_epoch:
                    best_val_loss_in_epoch = val_loss
                    current_epochs_no_improve = 0

                    # Save model immediately if enabled
                    if save_best_model_immediately and model_save_path_template:
                        best_model_path_in_epoch = model_save_path_template.format(
                            iteration=iteration, epoch=epoch_in_iter + 1
                        )
                        save_ddp_model(model, best_model_path_in_epoch, use_qlora)
                        if rank == 0:
                            print(
                                f"    → New best in-epoch weighted val loss: {best_val_loss_in_epoch:.4f}"
                            )
                            print(
                                f"    → Saved best model to: {best_model_path_in_epoch}"
                            )
                    else:
                        if rank == 0:
                            print(
                                f"    → New best in-epoch weighted val loss: {best_val_loss_in_epoch:.4f}"
                            )
                else:
                    current_epochs_no_improve += 1
                    if rank == 0:
                        print(
                            f"    → No improvement for {current_epochs_no_improve} eval steps"
                        )

                    if current_epochs_no_improve >= IN_EPOCH_EARLY_STOPPING_PATIENCE:
                        early_stop_triggered = True
                        if rank == 0:
                            print(f"    → Early stopping triggered at step {step + 1}")

            # In-epoch LR scheduling (using weighted validation loss)
            if IN_EPOCH_LR_SCHEDULING and scheduler is not None:
                scheduler.step(val_loss)
                if rank == 0:
                    current_lr = scheduler.optimizer.param_groups[0]["lr"]
                    print(f"    → LR updated to: {current_lr:.2e}")

            train_losses_batch.append(np.mean(losses))
            # Calculate accuracy based on samples processed SO FAR in this epoch
            train_accuracies_batch.append(
                (correct_predictions.double() / total_samples_processed_in_epoch).item()
            )

            val_losses_batch.append(val_loss)
            val_accuracies_batch.append(val_acc.item())
            val_f1_batch.append(val_f1)
            val_precision_batch.append(val_precision)
            val_recall_batch.append(val_recall)

            model.train()  # Set model back to train mode after validation

            # Break if early stopping is triggered
            if early_stop_triggered:
                break

    # Ensure all ranks complete the epoch together
    if torch.distributed.is_initialized():
        safe_barrier()

    # Return epoch-level averages for summary, lists for plotting, and early stopping info
    return (
        np.mean(losses),
        correct_predictions.double()
        / total_samples_processed_in_epoch,  # Fixed: use actual samples processed by this rank
        train_losses_batch,
        train_accuracies_batch,
        val_losses_batch,
        val_accuracies_batch,
        val_f1_batch,
        val_precision_batch,  # Add precision tracking
        val_recall_batch,  # Add recall tracking
        best_val_loss_in_epoch,  # Best validation loss achieved in this epoch
        current_epochs_no_improve,  # Number of evaluation steps without improvement
        early_stop_triggered,  # Whether early stopping was triggered
        best_model_path_in_epoch,  # Path to the best model saved during this epoch (or None)
    )


def evaluate_model(model, data_loader, device, class_weights=None):
    """Evaluates the model on a given data loader."""
    model.eval()
    losses = []
    correct_predictions = 0
    all_labels = []
    all_preds = []

    # Define loss function - Use Focal Loss with auto-computed alpha
    loss_fct = create_focal_loss(class_weights, device, rank=0)

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

    # In distributed training, all ranks compute the same thing since they process the same validation data
    # To avoid confusion with identical results, we ensure proper synchronization
    if torch.distributed.is_initialized():
        # Ensure all ranks finish evaluation before proceeding
        safe_barrier()

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
    rank,
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
    # Mean Teacher parameters
    teacher_model=None,
    use_mean_teacher=False,
    # Training epochs parameter
    epochs_for_this_phase=None,  # Number of epochs for this training phase
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

    # Initialize lists for new precision and recall metrics
    in_epoch_val_precision_batch_from_train_epoch = []
    in_epoch_val_recall_batch_from_train_epoch = []

    # Inner early stopping for student within this iteration and save best model
    best_val_loss_student_in_iter = float("inf")
    epochs_no_improve_student_in_iter = 0
    model_save_path_in_iter = None

    # In-epoch early stopping variables
    best_val_loss_in_epoch = float("inf")
    epochs_no_improve_in_epoch = 0

    # Determine number of epochs for this training phase
    if epochs_for_this_phase is None:
        epochs_for_this_phase = STUDENT_TEACHER_EPOCHS_PER_ITERATION

    # Training loop for the current curriculum iteration
    for epoch_in_iter in range(epochs_for_this_phase):
        if rank == 0:
            print(
                f"\n--- {phase_description} Epoch {epoch_in_iter + 1}/{epochs_for_this_phase}"
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
            batch_val_precision_from_te,  # Add precision tracking
            batch_val_recall_from_te,  # Add recall tracking
            best_val_loss_in_epoch,
            epochs_no_improve_in_epoch,
            early_stop_triggered,
            best_model_path_in_epoch,
        ) = train_epoch(
            model,
            current_train_dataloader,
            combined_val_dataloader,  # Used by train_epoch for batch-level val metrics
            optimizer,
            (
                scheduler if IN_EPOCH_LR_SCHEDULING else None
            ),  # Pass scheduler for in-epoch LR scheduling
            device,
            rank=rank,  # Add rank parameter
            eval_every_steps=IN_EPOCH_EVAL_STEPS,
            class_weights=class_weights,
            train_sampler=getattr(current_train_dataloader, "sampler", None),
            epoch=epoch_in_iter,
            best_val_loss_in_epoch=best_val_loss_in_epoch,
            epochs_no_improve_in_epoch=epochs_no_improve_in_epoch,
            val_ynacc_dataloader=val_ynacc_dataloader,  # Pass individual validation dataloaders
            val_iac_dataloader=val_iac_dataloader,
            val_reddit_dataloader=val_reddit_dataloader,
            iteration=iteration,
            epoch_in_iter=epoch_in_iter,
            # Enable immediate model saving for in-epoch early stopping
            save_best_model_immediately=USE_IN_EPOCH_EARLY_STOPPING,
            model_save_path_template=(
                f"{OUT_DIR}best_model_iter_{{iteration}}_epoch_{{epoch}}"
                if USE_IN_EPOCH_EARLY_STOPPING
                else None
            ),
            use_qlora=USE_QLORA,
            # Mean Teacher parameters
            teacher_model=teacher_model,
            use_mean_teacher=use_mean_teacher,
            ema_decay=EMA_DECAY,
        )

        # Collect batch-wise metrics
        in_epoch_train_losses_batch.extend(batch_train_losses)
        in_epoch_train_accuracies_batch.extend(batch_train_accuracies)
        in_epoch_val_losses_batch_from_train_epoch.extend(batch_val_losses_from_te)
        in_epoch_val_accuracies_batch_from_train_epoch.extend(
            batch_val_accuracies_from_te
        )
        in_epoch_val_f1_batch_from_train_epoch.extend(batch_val_f1s_from_te)
        in_epoch_val_precision_batch_from_train_epoch.extend(
            batch_val_precision_from_te
        )
        in_epoch_val_recall_batch_from_train_epoch.extend(batch_val_recall_from_te)

        # Load the best model from this epoch if one was saved during in-epoch evaluation
        # IMPORTANT: Only load for end-of-epoch evaluation, then continue training from current state
        best_model_for_eval = None
        if USE_IN_EPOCH_EARLY_STOPPING and best_model_path_in_epoch is not None:
            if rank == 0:
                print(
                    "\n--- Loading best model from epoch for end-of-epoch evaluation ---"
                )
                print(f"Loading model from: {best_model_path_in_epoch}")

            # Synchronize before loading
            if torch.distributed.is_initialized():
                barrier_success = safe_barrier()
                if not barrier_success:
                    if rank == 0:
                        print(
                            "ERROR: Failed to synchronize ranks before model loading. Aborting training.",
                            flush=True,
                        )
                    # Exit gracefully instead of continuing with inconsistent state
                    cleanup()
                    return float("inf"), None, None

            # Load the best model temporarily for evaluation
            if USE_QLORA:
                # Create a separate model instance for evaluation
                best_model_for_eval = load_ddp_model(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    rank,
                    tokenizer,
                    model_path=best_model_path_in_epoch,
                )
            else:
                # Load the full model state_dict into a separate instance for evaluation
                fresh_model = load_base_model_and_apply_peft(
                    MODEL_NAME,
                    2,
                    False,  # No QLoRA for this path
                    None,
                    None,
                    rank,
                    tokenizer,
                    peft_model_path=None,
                )
                # Load the saved state dict
                fresh_model.load_state_dict(
                    torch.load(
                        f"{best_model_path_in_epoch}.pt", map_location=f"cuda:{rank}"
                    )
                )
                # Now wrap with DDP
                best_model_for_eval = DDP(
                    fresh_model, device_ids=[rank], find_unused_parameters=False
                )

            if rank == 0:
                print("Successfully loaded best model for end-of-epoch evaluation.")

        # --- Perform full evaluation on all validation sets at the end of the epoch ---
        if rank == 0:
            print("\n--- Evaluating on all development sets ---")

        # Use the best model for evaluation if available, otherwise use current training model
        eval_model = best_model_for_eval if best_model_for_eval is not None else model

        # Evaluate on YNACC Dev Set
        (
            val_loss_ynacc,
            val_acc_ynacc,
            val_prec_ynacc,
            val_rec_ynacc,
            val_f1_ynacc,
            _,
            _,
        ) = evaluate_model(eval_model, val_ynacc_dataloader, device, class_weights)
        if rank == 0:
            print(
                f"YNACC Dev Loss: {val_loss_ynacc:.4f}, Acc: {val_acc_ynacc:.4f}, Prec: {val_prec_ynacc:.4f}, Rec: {val_rec_ynacc:.4f}, F1: {val_f1_ynacc:.4f}"
            )

        # Evaluate on IAC Dev Set
        val_loss_iac, val_acc_iac, val_prec_iac, val_rec_iac, val_f1_iac, _, _ = (
            evaluate_model(eval_model, val_iac_dataloader, device, class_weights)
        )
        if rank == 0:
            print(
                f"IAC Dev Loss: {val_loss_iac:.4f}, Acc: {val_acc_iac:.4f}, Prec: {val_prec_iac:.4f}, Rec: {val_rec_iac:.4f}, F1: {val_f1_iac:.4f}"
            )

        # Evaluate on Reddit Validation Set
        (
            val_loss_reddit,
            val_acc_reddit,
            val_prec_reddit,
            val_rec_reddit,
            val_f1_reddit,
            _,
            _,
        ) = evaluate_model(eval_model, val_reddit_dataloader, device, class_weights)
        if rank == 0:
            print(
                f"Reddit Dev Loss: {val_loss_reddit:.4f}, Acc: {val_acc_reddit:.4f}, Prec: {val_prec_reddit:.4f}, Rec: {val_rec_reddit:.4f}, F1: {val_f1_reddit:.4f}"
            )

        # Clean up the evaluation model if we created one
        if best_model_for_eval is not None:
            del best_model_for_eval
            gc.collect()
            torch.cuda.empty_cache()

        # Restore current training model state (no action needed since we didn't modify `model`)

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

        if rank == 0:
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
        # Only append Reddit metrics if this is not the initial training phase
        all_curriculum_epoch_val_losses_reddit.append(val_loss_reddit)
        all_curriculum_epoch_val_accuracies_reddit.append(val_acc_reddit.item())
        all_curriculum_epoch_val_f1s_reddit.append(val_f1_reddit)

        # --- Scheduler Step (based on combined validation loss for inner loop) ---
        if not IN_EPOCH_LR_SCHEDULING:
            scheduler.step(current_combined_val_loss_for_scheduler)

        # --- Inner Early Stopping Logic for Student within this Iteration ---
        if not USE_IN_EPOCH_EARLY_STOPPING:
            # Use traditional end-of-epoch early stopping
            if current_combined_val_loss_for_scheduler < best_val_loss_student_in_iter:
                best_val_loss_student_in_iter = current_combined_val_loss_for_scheduler
                epochs_no_improve_student_in_iter = 0
                # Save the best model state within this particular curriculum iteration
                model_save_path_in_iter = (
                    f"{OUT_DIR}best_model_iter_{iteration}_epoch_{epoch_in_iter+1}"
                )
                # Use helper function to save DDP model correctly
                save_ddp_model(model, model_save_path_in_iter, USE_QLORA)

                phase_name = (
                    "initial training phase"
                    if iteration == 0
                    else f"iteration {iteration}"
                )
                if rank == 0:
                    print(
                        f"Saved best model for {phase_name} to {model_save_path_in_iter} with {loss_description}: {best_val_loss_student_in_iter:.4f}",
                        flush=True,
                    )
            else:
                epochs_no_improve_student_in_iter += 1
                phase_name = (
                    "initial training" if iteration == 0 else f"iteration {iteration}"
                )
                if rank == 0:
                    print(
                        f"No improvement in {loss_description} for student for {epochs_no_improve_student_in_iter} epochs in {phase_name}.",
                        flush=True,
                    )
                if epochs_no_improve_student_in_iter >= EARLY_STOPPING_PATIENCE:
                    phase_name = (
                        "initial training"
                        if iteration == 0
                        else f"iteration {iteration}"
                    )
                    if rank == 0:
                        print(
                            f"Inner early stopping triggered during {phase_name} after {epoch_in_iter + 1} epochs.",
                            flush=True,
                        )
                    break
        else:
            # Use in-epoch early stopping - update best model if this epoch achieved better validation loss
            if best_val_loss_in_epoch < best_val_loss_student_in_iter:
                best_val_loss_student_in_iter = best_val_loss_in_epoch

                # Use the best model path from the epoch if it was saved during in-epoch evaluation
                if best_model_path_in_epoch is not None:
                    model_save_path_in_iter = best_model_path_in_epoch
                else:
                    # Fallback: Save the current model state (shouldn't happen with immediate saving enabled)
                    model_save_path_in_iter = (
                        f"{OUT_DIR}best_model_iter_{iteration}_epoch_{epoch_in_iter+1}"
                    )
                    save_ddp_model(model, model_save_path_in_iter, USE_QLORA)

                phase_name = (
                    "initial training phase"
                    if iteration == 0
                    else f"iteration {iteration}"
                )
                if rank == 0:
                    print(
                        f"Best model for {phase_name} is at {model_save_path_in_iter} with best in-epoch val loss: {best_val_loss_student_in_iter:.4f}",
                        flush=True,
                    )

            # Check if in-epoch early stopping was triggered
            if early_stop_triggered:
                phase_name = (
                    "initial training" if iteration == 0 else f"iteration {iteration}"
                )
                if rank == 0:
                    print(
                        f"In-epoch early stopping triggered during {phase_name} at epoch {epoch_in_iter + 1}.",
                        flush=True,
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
                # Delete current model and load the best one
                del model
                gc.collect()
                torch.cuda.empty_cache()
                # Use helper function to load and wrap with DDP
                model = load_ddp_model(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    rank,
                    tokenizer,
                    model_path=model_save_path_in_iter,
                )
                if rank == 0:
                    print(
                        f"Loaded best QLoRA model from {model_save_path_in_iter} for curriculum phase initialization.",
                        flush=True,
                    )
            else:
                # Load the full model state_dict
                # First load fresh model
                fresh_model = load_base_model_and_apply_peft(
                    MODEL_NAME,
                    2,
                    False,  # No QLoRA for this path
                    None,
                    None,
                    rank,
                    tokenizer,
                    peft_model_path=None,
                )
                # Load the saved state dict
                fresh_model.load_state_dict(
                    torch.load(model_path_to_load, map_location=f"cuda:{rank}")
                )
                # Now wrap with DDP
                model = DDP(
                    fresh_model, device_ids=[rank], find_unused_parameters=False
                )
                if rank == 0:
                    print(
                        f"Loaded best model from {model_path_to_load} for curriculum phase initialization.",
                        flush=True,
                    )
        else:
            phase_name = (
                "initial training"
                if iteration == 0
                else f"Curriculum iteration {iteration}"
            )
            if rank == 0:
                print(
                    f"No best model saved from {phase_name}. Continuing with the last trained model from {phase_name}.",
                    flush=True,
                )
    else:
        phase_name = (
            "initial training"
            if iteration == 0
            else f"Curriculum iteration {iteration}"
        )
        if rank == 0:
            print(
                f"No best model saved from {phase_name}. Continuing with the last trained model from {phase_name}.",
                flush=True,
            )

    return best_val_loss_student_in_iter, model_save_path_in_iter, model


def training(rank, world_size):
    setup(rank, world_size)

    # Force stdout to be unbuffered for real-time output
    sys.stdout.reconfigure(line_buffering=True)

    # Login to HuggingFace (only prints from rank 0)
    config.login_to_huggingface(rank=rank)

    # Set device to current rank
    device = torch.device(f"cuda:{rank}" if torch.cuda.is_available() else "cpu")

    # Create output directory if it doesn't exist (only rank 0 to avoid race conditions)
    if rank == 0:
        if not os.path.exists(OUT_DIR):
            print(f"Creating output directory: {OUT_DIR}", flush=True)
            os.makedirs(OUT_DIR, exist_ok=True)

    # Synchronize to ensure directory is created before proceeding
    if torch.distributed.is_initialized():
        safe_barrier()

    if rank == 0:
        print("Using device: cuda", flush=True)
        print(f"Output directory: {OUT_DIR}", flush=True)
        if USE_QLORA:
            if bnb_bits == 4:
                print(
                    "Using 4-bit quantization with NF4 and double quantization",
                    flush=True,
                )
            elif bnb_bits == 8:
                print("Using 8-bit quantization with int8 threshold=6.0", flush=True)
            print(
                f"LoRA config: rank={lora_rank}, alpha={lora_alpha}, dropout=0.1",
                flush=True,
            )

    # Scale learning rate based on world size (optional, but often beneficial)
    effective_learning_rate = LEARNING_RATE * world_size
    if rank == 0:
        print(f"Base learning rate: {LEARNING_RATE}", flush=True)
        print(
            f"Effective learning rate (scaled by {world_size} GPUs): {effective_learning_rate}",
            flush=True,
        )
        print(f"Per-GPU batch size: {BATCH_SIZE}", flush=True)
        print(f"Effective batch size: {BATCH_SIZE * world_size}", flush=True)
        print(f"Classifier dropout: {CLASSIFIER_DROPOUT}", flush=True)
        print(f"Classification head weight decay: {WEIGHT_DECAY}", flush=True)
        print(f"Backbone weight decay: {BACKBONE_WEIGHT_DECAY}", flush=True)
        print(f"Early stopping patience: {EARLY_STOPPING_PATIENCE}", flush=True)
        print(
            f"LR scheduler patience: {max(1, EARLY_STOPPING_PATIENCE - 1)}", flush=True
        )
        print(f"In-epoch early stopping: {USE_IN_EPOCH_EARLY_STOPPING}", flush=True)
        if USE_IN_EPOCH_EARLY_STOPPING:
            print(f"In-epoch evaluation steps: {IN_EPOCH_EVAL_STEPS}", flush=True)
            print(
                f"In-epoch early stopping patience: {IN_EPOCH_EARLY_STOPPING_PATIENCE}",
                flush=True,
            )
            print(
                "In-epoch validation uses weighted loss (Reddit 2x weight)", flush=True
            )
        print(f"In-epoch LR scheduling: {IN_EPOCH_LR_SCHEDULING}", flush=True)
        if IN_EPOCH_LR_SCHEDULING:
            print(
                "In-epoch LR scheduling uses weighted loss (Reddit 2x weight)",
                flush=True,
            )

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    # --- Load Data ---
    if CORPUS_ANNOTATION_MODE:
        # For annotation mode, load the corpus data without filtering
        if rank == 0:
            print("=== CORPUS ANNOTATION MODE ===", flush=True)
            print(
                f"Loading corpus for annotation from: {ANNOTATION_CORPUS_PATH}",
                flush=True,
            )
            print(
                f"Starting from iteration: {ANNOTATION_STARTING_ITERATION}", flush=True
            )
            print(f"Output will be saved to: {ANNOTATION_OUTPUT_PATH}", flush=True)

        # Load the original corpus data with all fields preserved
        df_corpus_original = load_corpus_for_annotation(
            ANNOTATION_CORPUS_PATH, rank=rank
        )

        if df_corpus_original.empty:
            if rank == 0:
                print("Error: No corpus data loaded. Exiting annotation mode.")
            return

        # Use the corpus text as the unlabeled data for pseudo-labeling
        # Create a simple DataFrame with just text for the pseudo-labeling process
        df_unlabeled_reddit_raw = pd.DataFrame(
            {"text": df_corpus_original["text"].tolist()}
        )

        # We don't need the other datasets for annotation mode, but create empty ones for consistency
        df_ynacc_raw = pd.DataFrame(columns=["text", "label"])
        df_iac_raw = pd.DataFrame(columns=["text", "label"])
        df_reddit_val = pd.DataFrame(columns=["text", "label"])
        df_reddit_test = pd.DataFrame(columns=["text", "label"])

        if rank == 0:
            print(f"Corpus data loaded: {len(df_corpus_original)} samples", flush=True)
    else:
        # Original data loading for training mode
        df_corpus_original = None  # Initialize for non-annotation mode
        if rank == 0:
            print("Loading YNACC data...", flush=True)
        df_ynacc_raw = load_jsonl(YNACC_FILE_PATH, tokenizer, MAX_LEN, rank=rank)
        if rank == 0:
            print("Loading IAC data...", flush=True)
        df_iac_raw = load_jsonl(IAC_FILE_PATH, tokenizer, MAX_LEN, rank=rank)
        if rank == 0 and not SUPERVISED_TRAINING_ONLY:
            print("Loading Unlabeled Reddit data...", flush=True)
        if not SUPERVISED_TRAINING_ONLY:
            df_unlabeled_reddit_raw = load_jsonl(
                REDDIT_UNLABELED_FILE_PATH, tokenizer, MAX_LEN, rank=rank
            )  # No labels needed initially
        else:
            df_unlabeled_reddit_raw = pd.DataFrame(
                columns=["text", "label"]
            )  # Empty DataFrame for consistency
        if rank == 0:
            print("Loading Reddit Validation data...")
        df_reddit_val = load_jsonl(
            REDDIT_VAL_FILE_PATH, tokenizer, MAX_LEN, filter_max_len=False, rank=rank
        )
        if rank == 0:
            print("Loading Reddit Test data...")
        df_reddit_test = load_jsonl(
            REDDIT_TEST_FILE_PATH, tokenizer, MAX_LEN, filter_max_len=False, rank=rank
        )

    # Validate loaded data (skip for annotation mode)
    if not CORPUS_ANNOTATION_MODE and (
        df_ynacc_raw.empty or df_iac_raw.empty or df_unlabeled_reddit_raw.empty
        if not SUPERVISED_TRAINING_ONLY
        else False or df_reddit_val.empty or df_reddit_test.empty
    ):
        if rank == 0:
            print(
                "Exiting: One or more datasets could not be loaded or are empty after filtering."
            )
        return

    # For annotation mode, skip training data preparation and go directly to pseudo-labeling
    if CORPUS_ANNOTATION_MODE:
        # Skip to curriculum learning section for annotation
        if rank == 0:
            print("Skipping training data preparation for annotation mode...")
            print(
                f"Will start annotation from iteration {ANNOTATION_STARTING_ITERATION}"
            )
    else:
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

        if rank == 0:
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

        if rank == 0:
            print(
                f"\nCombined initial training data size: {len(df_train_initial)} samples."
            )
            print("Combined initial training data label distribution:")
            print(df_train_initial["label"].value_counts().to_dict())

        # --- Calculate Class Weights for the Initial Training Set ---
        if rank == 0:
            print("\n--- Calculating Class Weights ---")
        train_labels_initial = df_train_initial["label"].tolist()
        class_counts_initial = Counter(train_labels_initial)
        num_classes = len(class_counts_initial)

        if num_classes > 0:
            sorted_class_counts = sorted(class_counts_initial.items())
            total_samples = sum(count for _, count in sorted_class_counts)

            # Use balanced class weights: inverse of class frequency
            # This gives more reasonable weights than the sklearn 'balanced' formula
            weights = []
            for class_label, count in sorted_class_counts:
                weight = total_samples / count  # Inverse frequency
                weights.append(weight)

            # Normalize weights so they average to 1.0
            avg_weight = sum(weights) / len(weights)
            weights = [w / avg_weight for w in weights]

            class_weights = torch.tensor(weights, dtype=torch.float)
            if rank == 0:
                print(f"Class counts: {dict(sorted_class_counts)}")
                print(
                    f"Calculated Class Weights (normalized inverse frequency): {class_weights.tolist()}"
                )
                print(
                    f"Class 0 weight: {class_weights[0]:.4f}, Class 1 weight: {class_weights[1]:.4f}"
                )
        else:
            class_weights = None
            if rank == 0:
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
            if rank == 0:
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
            df_iac_val["text"].tolist(),
            df_iac_val["label"].tolist(),
            tokenizer,
            MAX_LEN,
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
            df_iac_test["text"].tolist(),
            df_iac_test["label"].tolist(),
            tokenizer,
            MAX_LEN,
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
    if CORPUS_ANNOTATION_MODE:
        # For annotation mode, load model from specified iteration using checkpoint logic
        if rank == 0:
            print(
                f"Loading model from iteration {ANNOTATION_STARTING_ITERATION} for annotation..."
            )

        checkpoint_path = load_checkpoint_from_iteration(
            ANNOTATION_STARTING_ITERATION, rank
        )

        if checkpoint_path is None:
            if rank == 0:
                print(
                    f"ERROR: No checkpoint found for iteration {ANNOTATION_STARTING_ITERATION}"
                )
            return

        if rank == 0:
            print(f"Loading model from checkpoint: {checkpoint_path}")

        try:
            if USE_QLORA:
                # Load the base model and then apply the saved PEFT adapters
                model = load_base_model_and_apply_peft(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    rank,
                    tokenizer,
                    peft_model_path=checkpoint_path,
                )
                # Wrap with DDP
                model = DDP(
                    model,
                    device_ids=[rank],
                    find_unused_parameters=False,
                )
            else:
                # Load non-QLoRA checkpoint
                model = load_ddp_model(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    rank,
                    tokenizer,
                    model_path=None,
                )
                checkpoint_path_with_ext = f"{checkpoint_path}.pt"
                if os.path.exists(checkpoint_path_with_ext):
                    checkpoint = torch.load(
                        checkpoint_path_with_ext, map_location=device
                    )
                    model.load_state_dict(checkpoint["model_state_dict"])
                    if rank == 0:
                        print(f"Loaded model state from {checkpoint_path_with_ext}")
                else:
                    if rank == 0:
                        print(
                            f"ERROR: Checkpoint file not found: {checkpoint_path_with_ext}"
                        )
                    return

            if rank == 0:
                print("Model loaded successfully for annotation mode!")

        except Exception as e:
            if rank == 0:
                print(f"ERROR: Failed to load checkpoint: {e}")
            return
    else:
        # Normal training mode - initialize fresh model
        model = load_ddp_model(
            MODEL_NAME,
            2,
            USE_QLORA,
            bnb_config,
            lora_config,
            rank,
            tokenizer,
            model_path=None,
        )

        optimizer = setup_optimizer_with_weight_decay(
            model, effective_learning_rate, WEIGHT_DECAY, BACKBONE_WEIGHT_DECAY
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", patience=SCHEDULER_PATIENCE
        )

    # --- Mean Teacher Initialization ---
    teacher_model = None
    if USE_MEAN_TEACHER:
        if rank == 0:
            print("Initializing Mean Teacher approach...", flush=True)
        teacher_model = create_teacher_model(model, rank=rank)
        # Move teacher to same device as student
        teacher_model.to(device)
        if rank == 0:
            print(f"Teacher model created with EMA decay: {EMA_DECAY}", flush=True)
    else:
        if rank == 0:
            print("Using standard curriculum learning (no Mean Teacher)", flush=True)

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
    in_epoch_val_precision_batch_from_train_epoch = []  # Add precision tracking
    in_epoch_val_recall_batch_from_train_epoch = []  # Add recall tracking

    # Iteration-level metadata tracking
    iteration_pseudo_label_stats = []  # Pseudo-labeling statistics per iteration
    iteration_confidence_thresholds = []  # Confidence threshold used per iteration
    iteration_training_data_sizes = []  # Training set size per iteration

    if rank == 0:
        print("\n--- Starting Curriculum Training Loop ---")
    best_combined_val_loss_overall = float("inf")
    epochs_no_improve_overall = 0  # For overall curriculum early stopping
    overall_best_model_save_path = None
    current_confidence_threshold = CONFIDENCE_THRESHOLD_START

    # Handle checkpoint resumption
    start_iteration = 1  # Default start for curriculum iterations
    if CONTINUE_FROM_CHECKPOINT:
        if rank == 0:
            print(f"\n--- Resuming from checkpoint: iteration {STARTING_ITERATION} ---")

        # Load the specified checkpoint
        checkpoint_path = load_checkpoint_from_iteration(STARTING_ITERATION, rank)

        if checkpoint_path is not None:
            # Load the model from checkpoint
            if rank == 0:
                print(f"Loading model from checkpoint: {checkpoint_path}")

            try:
                if USE_QLORA:
                    # Clean up current model
                    del model
                    gc.collect()
                    torch.cuda.empty_cache()

                    # Add distributed barrier before loading
                    if torch.distributed.is_initialized():
                        safe_barrier()

                    # Load the base model and then apply the saved PEFT adapters
                    model = load_base_model_and_apply_peft(
                        MODEL_NAME,
                        2,
                        USE_QLORA,
                        bnb_config,
                        lora_config,
                        rank,
                        tokenizer,
                        peft_model_path=checkpoint_path,
                    )

                    # Wrap with DDP
                    model = DDP(
                        model,
                        device_ids=[rank],
                        find_unused_parameters=False,
                    )
                else:
                    # Load non-QLoRA checkpoint
                    checkpoint_path_with_ext = f"{checkpoint_path}.pt"
                    if os.path.exists(checkpoint_path_with_ext):
                        checkpoint = torch.load(
                            checkpoint_path_with_ext, map_location=device
                        )
                        model.load_state_dict(checkpoint["model_state_dict"])
                        if rank == 0:
                            print(f"Loaded model state from {checkpoint_path_with_ext}")
                    else:
                        if rank == 0:
                            print(
                                f"ERROR: Checkpoint file not found: {checkpoint_path_with_ext}"
                            )
                        raise FileNotFoundError(
                            f"Checkpoint file not found: {checkpoint_path_with_ext}"
                        )

                # Recreate teacher model if using Mean Teacher
                if USE_MEAN_TEACHER:
                    if rank == 0:
                        print(
                            "Recreating teacher model from loaded checkpoint...",
                            flush=True,
                        )
                    teacher_model = create_teacher_model(model, rank=rank)
                    teacher_model.to(device)

                # Set the overall best model path
                overall_best_model_save_path = checkpoint_path

                # Adjust confidence threshold based on starting iteration
                if STARTING_ITERATION > 0:
                    # Calculate what the confidence threshold should be at this iteration
                    threshold_reduction = STARTING_ITERATION * CONFIDENCE_DECAY_FACTOR
                    current_confidence_threshold = max(
                        CONFIDENCE_THRESHOLD_END,
                        CONFIDENCE_THRESHOLD_START - threshold_reduction,
                    )
                    if rank == 0:
                        print(
                            f"Adjusted confidence threshold to {current_confidence_threshold:.3f} for iteration {STARTING_ITERATION}"
                        )

                # Set the starting iteration for the curriculum loop
                start_iteration = STARTING_ITERATION + 1

                if rank == 0:
                    print(
                        f"Successfully loaded checkpoint. Will start curriculum from iteration {start_iteration}"
                    )

                # Skip initial training since we loaded from a checkpoint
                skip_initial_training = True

            except Exception as e:
                if rank == 0:
                    print(f"ERROR: Failed to load checkpoint: {e}")
                    print("Continuing with normal training...")
                skip_initial_training = False
                start_iteration = 1
        else:
            if rank == 0:
                print("Checkpoint not found. Starting from beginning...")
            skip_initial_training = False
            start_iteration = 1
    else:
        skip_initial_training = False

    # Initial training phase (Curriculum Iteration 0) - only if not skipping
    if not skip_initial_training:
        if rank == 0:
            print("\n--- Curriculum Iteration 0: Initial Supervised Training ---")
        current_train_dataset = CommentDataset(
            df_train_initial["text"].tolist(),
            df_train_initial["label"].tolist(),
            tokenizer,
            MAX_LEN,
        )
        # Create distributed sampler for training data
        train_sampler = DistributedSampler(
            current_train_dataset, num_replicas=world_size, rank=rank, shuffle=True
        )
        current_train_dataloader = DataLoader(
            current_train_dataset, batch_size=BATCH_SIZE, sampler=train_sampler
        )

        if rank == 0:
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
                combined_val_dataloader=(
                    combined_val_dataloader_3
                    if not SUPERVISED_TRAINING_ONLY
                    else combined_val_dataloader_2
                ),
                optimizer=optimizer,
                scheduler=scheduler,
                device=device,
                class_weights=class_weights,
                tokenizer=tokenizer,
                rank=rank,
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
                # Mean Teacher parameters
                teacher_model=teacher_model,
                use_mean_teacher=USE_MEAN_TEACHER,
                # Use more epochs for initial training
                epochs_for_this_phase=INITIAL_TRAINING_EPOCHS,
            )
        )

        # Update the overall best model and path if this initial training achieved a new best
        if best_val_loss_student_in_iter < best_combined_val_loss_overall:
            best_combined_val_loss_overall = best_val_loss_student_in_iter
            overall_best_model_save_path = model_save_path_in_iter
            if rank == 0:
                print(
                    f"Overall best model updated after initial training to {overall_best_model_save_path}",
                    flush=True,
                )

    if not SUPERVISED_TRAINING_ONLY or CORPUS_ANNOTATION_MODE:
        # --- Curriculum Learning Iterations (or Annotation Mode) ---
        if CORPUS_ANNOTATION_MODE:
            # For annotation mode, do just one iteration of pseudo-labeling
            if rank == 0:
                print("\n=== ANNOTATION MODE: Pseudo-labeling corpus ===")
                print(f"Processing {len(df_unlabeled_reddit_raw)} samples from corpus")

            # Set up for single annotation iteration
            max_iterations = 1
            start_iter = 1
            # For annotation mode, we don't use confidence thresholding - just predict all samples
            if rank == 0:
                print(
                    "Annotation mode: Will predict all samples without confidence thresholding"
                )
        else:
            # Normal curriculum learning
            max_iterations = MAX_CURRICULUM_ITERATIONS
            start_iter = start_iteration

        for iteration in range(start_iter, max_iterations + 1):
            if CORPUS_ANNOTATION_MODE:
                if rank == 0:
                    print("\n--- Annotating Corpus (Pseudo-labeling) ---")
            else:
                if rank == 0:
                    print(
                        f"\n--- Curriculum Iteration {iteration}/{max_iterations} ---"
                    )

            # Track iteration metadata (skip for annotation mode)
            if not CORPUS_ANNOTATION_MODE:
                iteration_confidence_thresholds.append(current_confidence_threshold)

            # 1. Teacher Pseudo-Labeling
            if rank == 0:
                if CORPUS_ANNOTATION_MODE:
                    print("Pseudo-labeling entire corpus (no confidence thresholding)")
                    print(
                        f"Processing all {len(df_unlabeled_reddit_raw)} samples from corpus"
                    )
                else:
                    print(
                        f"Teacher pseudo-labeling unlabeled Reddit data with confidence threshold: {current_confidence_threshold:.3f}"
                    )
                    print(
                        f"Using temperature scaling: {PSEUDO_LABELING_TEMPERATURE} (reduces overconfidence)"
                    )
                    print(
                        f"Sampling {UNLABELED_DATA_FRACTION_PER_STEP:.2%} of unlabeled data for pseudo-labeling"
                    )

            # Choose model for pseudo-labeling based on Mean Teacher setting
            pseudo_labeling_model = None

            if USE_MEAN_TEACHER and teacher_model is not None:
                # Use the stable teacher model for pseudo-labeling
                pseudo_labeling_model = teacher_model
                if rank == 0:
                    print(
                        "Using Mean Teacher approach: Teacher model for pseudo-labeling",
                        flush=True,
                    )
            else:
                # Use the current student model (standard approach)
                # Model loading for pseudo-labeling (all ranks participate)
                if overall_best_model_save_path:
                    if USE_QLORA:
                        model_path_to_load = overall_best_model_save_path
                        # Wait for model to be fully written and validate it
                        model_exists = wait_for_model_file(
                            model_path_to_load, max_wait_time=60
                        )
                    else:
                        model_path_to_load = f"{overall_best_model_save_path}.pt"
                        model_exists = wait_for_model_file(
                            model_path_to_load, max_wait_time=60
                        )

                    if model_exists:
                        if USE_QLORA:
                            # Save current model state for Mean Teacher
                            current_model_state = None
                            if USE_MEAN_TEACHER and teacher_model is not None:
                                # We'll reload the model, so update teacher first
                                update_teacher_ema(
                                    teacher_model, model, EMA_DECAY, rank
                                )

                            del model  # Ensure previous model is gone
                            gc.collect()
                            torch.cuda.empty_cache()

                            # Add distributed barrier before loading
                            if torch.distributed.is_initialized():
                                safe_barrier()

                            # Load the base model and then apply the saved PEFT adapters
                            model = load_base_model_and_apply_peft(
                                MODEL_NAME,
                                2,
                                USE_QLORA,
                                bnb_config,
                                lora_config,
                                rank,
                                tokenizer,
                                peft_model_path=overall_best_model_save_path,
                            )
                            model = DDP(
                                model, device_ids=[rank], find_unused_parameters=False
                            )
                            # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
                            if hasattr(model.module, "gradient_checkpointing_enable"):
                                model.module.gradient_checkpointing_enable(
                                    gradient_checkpointing_kwargs={
                                        "use_reentrant": False
                                    }
                                )
                        else:
                            # Load the overall best model state dict
                            model = AutoModelForSequenceClassification.from_pretrained(
                                MODEL_NAME, num_labels=2
                            )
                            model.config.pad_token_id = tokenizer.pad_token_id

                            # Set classifier dropout if the model supports it
                            if hasattr(model.config, "classifier_dropout"):
                                model.config.classifier_dropout = CLASSIFIER_DROPOUT
                            elif hasattr(model.config, "hidden_dropout_prob"):
                                # For some models, classifier dropout is controlled by hidden_dropout_prob
                                model.config.hidden_dropout_prob = max(
                                    model.config.hidden_dropout_prob, CLASSIFIER_DROPOUT
                                )

                            model.load_state_dict(
                                torch.load(
                                    model_path_to_load, map_location=f"cuda:{rank}"
                                )
                            )
                            model.to(rank)
                            model = DDP(
                                model, device_ids=[rank], find_unused_parameters=False
                            )
                        model.eval()  # Set to eval mode for inference
                        pseudo_labeling_model = model
                        if rank == 0:
                            print(
                                f"Loaded overall best model from {model_path_to_load} for pseudo-labeling."
                            )
                    else:
                        pseudo_labeling_model = model
                        if rank == 0:
                            print(
                                "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for pseudo-labeling."
                            )
                else:
                    pseudo_labeling_model = model
                    if rank == 0:
                        print(
                            "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for pseudo-labeling."
                        )

            # Ensure all ranks are synchronized before pseudo-labeling
            if torch.distributed.is_initialized():
                safe_barrier()

            time_start = time.time()
            # First, sample the fraction of unlabeled data to process in this iteration
            if CORPUS_ANNOTATION_MODE:
                # For annotation mode, process all corpus data
                df_unlabeled_sampled = df_unlabeled_reddit_raw.copy()
                if rank == 0:
                    print(
                        f"Annotation mode: Processing all {len(df_unlabeled_sampled)} corpus samples"
                    )
            elif UNLABELED_DATA_FRACTION_PER_STEP < 1.0:
                df_unlabeled_sampled = df_unlabeled_reddit_raw.sample(
                    frac=UNLABELED_DATA_FRACTION_PER_STEP,
                    random_state=RANDOM_SEED
                    + iteration,  # Different seed per iteration
                ).reset_index(drop=True)
            else:
                df_unlabeled_sampled = df_unlabeled_reddit_raw.copy()

            if rank == 0 and not CORPUS_ANNOTATION_MODE:
                print(
                    f"Processing {len(df_unlabeled_sampled)} unlabeled samples for pseudo-labeling"
                )

            # Multi-GPU distributed pseudo-labeling approach
            unlabeled_texts = df_unlabeled_sampled["text"].tolist()
            unlabeled_dataset = CommentDataset(
                unlabeled_texts, [0] * len(unlabeled_texts), tokenizer, MAX_LEN
            )  # Dummy labels

            # Create distributed sampler for multi-GPU pseudo-labeling
            # Use drop_last=False to ensure we don't lose any samples, but this can cause size differences
            unlabeled_sampler = DistributedSampler(
                unlabeled_dataset,
                num_replicas=world_size,
                rank=rank,
                shuffle=False,
                drop_last=False,
            )
            unlabeled_dataloader = DataLoader(
                unlabeled_dataset,
                batch_size=PSEUDO_LABEL_BATCH_SIZE,
                sampler=unlabeled_sampler,
            )

            if rank == 0 and CORPUS_ANNOTATION_MODE:
                print(f"SAFETY: Total dataset size: {len(unlabeled_dataset)}")
                print(
                    f"SAFETY: Expected samples per rank: ~{len(unlabeled_dataset) / world_size:.1f}"
                )
                print(
                    f"SAFETY: This rank will process ~{len(unlabeled_sampler)} samples"
                )

            # Each rank processes its portion of the data
            local_pseudo_labels = []
            local_confidences = []
            local_texts = []

            # Set the model for pseudo-labeling to eval mode
            pseudo_labeling_model.eval()

            with torch.no_grad():
                total_batches = len(unlabeled_dataloader)
                processed_batches = 0

                for batch in unlabeled_dataloader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)
                    texts = batch["text"]  # Get the text data from the batch

                    outputs = pseudo_labeling_model(
                        input_ids=input_ids, attention_mask=attention_mask
                    )
                    logits = outputs.logits

                    # Apply temperature scaling to reduce overconfidence
                    scaled_logits = logits / PSEUDO_LABELING_TEMPERATURE
                    probabilities = torch.softmax(scaled_logits, dim=-1)
                    max_confidences, predicted_labels = torch.max(probabilities, dim=-1)

                    local_pseudo_labels.extend(predicted_labels.cpu().tolist())
                    local_confidences.extend(max_confidences.cpu().tolist())
                    local_texts.extend(texts)

                    processed_batches += 1

                    # Progress logging and periodic safety saves for annotation mode
                    if (
                        CORPUS_ANNOTATION_MODE
                        and rank == 0
                        and processed_batches % max(1, total_batches // 10) == 0
                    ):
                        progress = 100 * processed_batches / total_batches
                        samples_processed = (
                            processed_batches * PSEUDO_LABEL_BATCH_SIZE * world_size
                        )
                        print(
                            f"Annotation progress: {progress:.1f}% ({samples_processed}/{len(df_unlabeled_sampled)} samples)"
                        )

                        # SAFETY GUARD 2: Periodic saves during processing
                        if (
                            processed_batches
                            % max(1, total_batches // ANNOTATION_CHECKPOINT_FREQUENCY)
                            == 0
                        ):  # Configurable frequency
                            checkpoint_path = ANNOTATION_OUTPUT_PATH.replace(
                                ".jsonl", f"_checkpoint_{progress:.0f}pct.jsonl"
                            )
                            # Create intermediate results
                            temp_df = pd.DataFrame(
                                {
                                    "text": local_texts[: len(local_pseudo_labels)],
                                    "pseudo_label": local_pseudo_labels,
                                    "confidence": local_confidences,
                                }
                            )
                            temp_df.to_json(
                                checkpoint_path, orient="records", lines=True
                            )
                            print(
                                f"SAFETY: Checkpoint saved at {progress:.0f}% to {checkpoint_path}"
                            )

            if CORPUS_ANNOTATION_MODE and rank == 0:
                print("Pseudo-labeling completed, gathering results from all GPUs...")
                # Log final local statistics before gathering
                print(f"Local pseudo-labeling stats for rank {rank}:")
                print(f"  - Local samples processed: {len(local_pseudo_labels)}")
                print(
                    f"  - Expected samples per rank: ~{len(df_unlabeled_sampled) // world_size}"
                )
                if len(local_pseudo_labels) > 0:
                    local_pred_dist = (
                        pd.Series(local_pseudo_labels).value_counts().to_dict()
                    )
                    print(f"  - Local prediction distribution: {local_pred_dist}")

            # Gather results from all ranks to rank 0
            # First, gather the sizes to know how much data each rank has
            local_size = torch.tensor(len(local_pseudo_labels), device=device)
            all_sizes = [torch.zeros_like(local_size) for _ in range(world_size)]
            dist.all_gather(all_sizes, local_size)

            # Convert to list for easier handling
            sizes = [size.item() for size in all_sizes]
            max_size = max(sizes)

            # Pad local results to max_size for consistent gathering
            if len(local_pseudo_labels) < max_size:
                padding_size = max_size - len(local_pseudo_labels)
                local_pseudo_labels.extend([-1] * padding_size)  # Use -1 as padding
                local_confidences.extend([-1.0] * padding_size)  # Use -1.0 as padding
                local_texts.extend([""] * padding_size)  # Use empty string as padding

            # Convert to tensors for gathering (only labels and confidences)
            local_pseudo_labels_tensor = torch.tensor(
                local_pseudo_labels, dtype=torch.long, device=device
            )
            local_confidences_tensor = torch.tensor(
                local_confidences, dtype=torch.float, device=device
            )

            # Gather tensors to all ranks (we need all data on all ranks for the next steps)
            gathered_pseudo_labels = [
                torch.zeros_like(local_pseudo_labels_tensor) for _ in range(world_size)
            ]
            gathered_confidences = [
                torch.zeros_like(local_confidences_tensor) for _ in range(world_size)
            ]

            dist.all_gather(gathered_pseudo_labels, local_pseudo_labels_tensor)
            dist.all_gather(gathered_confidences, local_confidences_tensor)

            # Gather texts using all_gather_object (handles variable-length strings)
            all_local_texts = [None for _ in range(world_size)]
            dist.all_gather_object(all_local_texts, local_texts)

            # Combine results from all ranks (done on all ranks for consistency)
            all_pseudo_labels = []
            all_confidences = []
            all_texts = []

            for rank_idx in range(world_size):
                rank_size = sizes[rank_idx]

                # Get valid (non-padded) results from this rank
                rank_labels = (
                    gathered_pseudo_labels[rank_idx][:rank_size].cpu().tolist()
                )
                rank_confs = gathered_confidences[rank_idx][:rank_size].cpu().tolist()
                rank_texts = all_local_texts[rank_idx][:rank_size]

                all_pseudo_labels.extend(rank_labels)
                all_confidences.extend(rank_confs)
                all_texts.extend(rank_texts)

            # Create DataFrame with pseudo-labeled results (all ranks have the same data)
            df_pseudo_labeled = pd.DataFrame(
                {
                    "text": all_texts,
                    "pseudo_label": all_pseudo_labels,
                    "confidence": all_confidences,
                }
            )

            # SAFETY GUARD 3: Remove any potential duplicates from distributed sampling
            if CORPUS_ANNOTATION_MODE and rank == 0:
                original_pseudo_size = len(df_pseudo_labeled)
                df_pseudo_labeled = df_pseudo_labeled.drop_duplicates(
                    subset=["text"], keep="first"
                )
                if len(df_pseudo_labeled) != original_pseudo_size:
                    print(
                        f"SAFETY: Removed {original_pseudo_size - len(df_pseudo_labeled)} duplicate entries from pseudo-labeled data"
                    )

            # For annotation mode, merge pseudo-labels with original corpus data
            if CORPUS_ANNOTATION_MODE:
                if df_corpus_original is None:
                    if rank == 0:
                        print(
                            "ERROR: Original corpus data not available for annotation mode!"
                        )
                    return

                if rank == 0:
                    print("Merging pseudo-labels with original corpus data...")
                    print(f"Original corpus has {len(df_corpus_original)} samples")
                    print(f"Pseudo-labeled data has {len(df_pseudo_labeled)} samples")

                # SAFETY GUARD 1: Save intermediate pseudo-labeled results
                if rank == 0:
                    intermediate_save_path = ANNOTATION_OUTPUT_PATH.replace(
                        ".jsonl", "_intermediate_pseudo_labels.jsonl"
                    )
                    print(
                        f"SAFETY: Saving intermediate pseudo-labeled results to {intermediate_save_path}"
                    )
                    df_pseudo_labeled.to_json(
                        intermediate_save_path, orient="records", lines=True
                    )

                # Handle size mismatch robustly - distributed sampling can cause slight differences
                original_size = len(df_corpus_original)
                pseudo_size = len(df_pseudo_labeled)

                if original_size != pseudo_size:
                    if rank == 0:
                        print(
                            f"WARNING: Size mismatch detected (original: {original_size}, pseudo: {pseudo_size})"
                        )
                        print(
                            "This can happen due to distributed sampling. Attempting to fix..."
                        )

                    if pseudo_size > original_size:
                        # Trim excess samples (likely duplicates from DistributedSampler)
                        if rank == 0:
                            print(
                                f"Trimming {pseudo_size - original_size} excess samples from pseudo-labeled data"
                            )
                        df_pseudo_labeled = df_pseudo_labeled.iloc[
                            :original_size
                        ].copy()
                    elif pseudo_size < original_size:
                        # This shouldn't happen, but handle it safely
                        if rank == 0:
                            print(
                                f"ERROR: Pseudo-labeled data is missing {original_size - pseudo_size} samples!"
                            )
                            print(
                                "This indicates a serious issue with the pseudo-labeling process."
                            )
                            print(
                                "Saving what we have and continuing with partial results..."
                            )

                        # Create a partial annotated dataset
                        df_annotated_partial = df_corpus_original.iloc[
                            :pseudo_size
                        ].copy()
                        df_annotated_partial["prediction"] = df_pseudo_labeled[
                            "pseudo_label"
                        ]
                        df_annotated_partial["confidence"] = df_pseudo_labeled[
                            "confidence"
                        ]

                        # Save partial results
                        partial_save_path = ANNOTATION_OUTPUT_PATH.replace(
                            ".jsonl", "_partial_results.jsonl"
                        )
                        if rank == 0:
                            print(
                                f"SAFETY: Saving partial results ({len(df_annotated_partial)} samples) to {partial_save_path}"
                            )
                        df_annotated_partial.to_json(
                            partial_save_path, orient="records", lines=True
                        )
                        return

                    if rank == 0:
                        print(
                            f"Size mismatch resolved. Final sizes: original={len(df_corpus_original)}, pseudo={len(df_pseudo_labeled)}"
                        )

                # IMPROVED APPROACH: Use text-based merging instead of order-based
                # This is more robust to distributed sampling issues
                if rank == 0:
                    print("Using robust text-based merging...")

                # Merge based on text content rather than assuming order preservation
                df_annotated = df_corpus_original.merge(
                    df_pseudo_labeled[["text", "pseudo_label", "confidence"]],
                    on="text",
                    how="left",
                )

                # Check for any unmatched samples
                unmatched_mask = df_annotated["pseudo_label"].isna()
                num_unmatched = unmatched_mask.sum()

                if num_unmatched > 0:
                    if rank == 0:
                        print(f"WARNING: {num_unmatched} samples could not be matched!")
                        print(
                            "This suggests some texts were modified during processing."
                        )

                        # SAFETY GUARD 4: Save unmatched samples for debugging
                        unmatched_samples = df_corpus_original[unmatched_mask]
                        unmatched_save_path = ANNOTATION_OUTPUT_PATH.replace(
                            ".jsonl", "_unmatched_samples.jsonl"
                        )
                        unmatched_samples.to_json(
                            unmatched_save_path, orient="records", lines=True
                        )
                        print(
                            f"SAFETY: Saved unmatched samples to {unmatched_save_path}"
                        )

                    # Fill unmatched with default values (label as uncertain)
                    df_annotated.loc[unmatched_mask, "pseudo_label"] = -1  # Uncertain
                    df_annotated.loc[unmatched_mask, "confidence"] = 0.0

                # Rename pseudo_label to prediction for consistency
                df_annotated = df_annotated.rename(
                    columns={"pseudo_label": "prediction"}
                )

                # Add logging about annotation results
                if rank == 0:
                    total_samples = len(df_annotated)
                    prediction_dist = (
                        df_annotated["prediction"].value_counts().to_dict()
                    )
                    avg_confidence = df_annotated["confidence"].mean()

                    print("\n=== ANNOTATION COMPLETED ===")
                    print(f"Total samples annotated: {total_samples}")
                    print(f"Prediction distribution: {prediction_dist}")
                    print(f"Average confidence: {avg_confidence:.4f}")

                    # Show confidence distribution
                    conf_ranges = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
                    print("\nConfidence distribution:")
                    for i in range(len(conf_ranges) - 1):
                        low, high = conf_ranges[i], conf_ranges[i + 1]
                        count = len(
                            df_annotated[
                                (df_annotated["confidence"] >= low)
                                & (df_annotated["confidence"] < high)
                            ]
                        )
                        print(
                            f"  {low:.1f}-{high:.1f}: {count} samples ({100*count/total_samples:.1f}%)"
                        )

                    print(f"\nSaving annotated corpus to: {ANNOTATION_OUTPUT_PATH}")

                    # SAFETY GUARD 5: Save backup before final save
                    backup_path = ANNOTATION_OUTPUT_PATH.replace(
                        ".jsonl", "_backup.jsonl"
                    )
                    print(f"SAFETY: Creating backup at {backup_path}")

                # Save the annotated DataFrame to a JSONL file
                if rank == 0:
                    # Create backup first
                    backup_path = ANNOTATION_OUTPUT_PATH.replace(
                        ".jsonl", "_backup.jsonl"
                    )
                    df_annotated.to_json(backup_path, orient="records", lines=True)

                # Save main file (all ranks to ensure synchronization)
                df_annotated.to_json(
                    ANNOTATION_OUTPUT_PATH, orient="records", lines=True
                )

                if rank == 0:
                    print("Annotation completed successfully!")
                    print(f"Main output saved to: {ANNOTATION_OUTPUT_PATH}")
                    print(f"Backup saved to: {backup_path}")
                    print(f"Output contains columns: {list(df_annotated.columns)}")
                    print(
                        f"SAFETY: Total of {5} safety files created during annotation process"
                    )

                return

            # Apply dynamic threshold lowering to ensure we get enough balanced samples for training
            min_samples_required = MIN_PSEUDO_SAMPLES_REQUIRED
            dynamic_threshold = current_confidence_threshold
            final_threshold = current_confidence_threshold

            if rank == 0:
                print(
                    f"Applying dynamic threshold starting at {current_confidence_threshold:.3f}"
                )
                print(
                    f"Target: At least {min_samples_required} samples after balancing"
                )

            high_confidence_pseudo_labeled_df = None

            # Keep lowering threshold until we have enough balanced samples or reach minimum
            while dynamic_threshold > CONFIDENCE_THRESHOLD_END:
                # Apply current threshold
                temp_high_conf_df = df_pseudo_labeled[
                    df_pseudo_labeled["confidence"] >= dynamic_threshold
                ]

                if rank == 0:
                    print(
                        f"  Threshold {dynamic_threshold:.3f}: Found {len(temp_high_conf_df)} samples before balancing"
                    )

                # Apply balancing to this threshold's results
                balanced_df = (
                    temp_high_conf_df.copy()
                    if len(temp_high_conf_df) > 0
                    else pd.DataFrame(columns=["text", "pseudo_label", "confidence"])
                )

                if len(balanced_df) > 0:
                    label_counts = balanced_df["pseudo_label"].value_counts()

                    # Simple balancing: drop samples from majority class if ratio > 2:1
                    if len(label_counts) >= 2:  # Need at least two classes
                        class_0_count = label_counts.get(0, 0)
                        class_1_count = label_counts.get(1, 0)

                        if class_0_count > 0 and class_1_count > 0:
                            # Determine majority and minority classes
                            if class_0_count > class_1_count:
                                majority_class = 0
                                minority_class = 1
                                majority_count = class_0_count
                                minority_count = class_1_count
                            else:
                                majority_class = 1
                                minority_class = 0
                                majority_count = class_1_count
                                minority_count = class_0_count

                            # Check if we need to balance (ratio > 2:1)
                            if majority_count > 2 * minority_count:
                                # Downsample majority class to exactly 2 * minority_count
                                max_majority_samples = 2 * minority_count

                                # Keep all minority samples
                                minority_samples = balanced_df[
                                    balanced_df["pseudo_label"] == minority_class
                                ]

                                # Randomly sample majority samples
                                majority_samples = balanced_df[
                                    balanced_df["pseudo_label"] == majority_class
                                ].sample(
                                    n=max_majority_samples,
                                    random_state=RANDOM_SEED + iteration,
                                )

                                # Combine and shuffle
                                balanced_df = (
                                    pd.concat([minority_samples, majority_samples])
                                    .sample(
                                        frac=1, random_state=RANDOM_SEED + iteration
                                    )
                                    .reset_index(drop=True)
                                )

                                if rank == 0:
                                    print(
                                        f"    Balanced: reduced class {majority_class} from {majority_count} to {max_majority_samples}"
                                    )
                    elif len(label_counts) == 1:
                        # Only one class - this violates 2:1 ratio, so select 0 samples
                        single_class = list(label_counts.keys())[0]
                        single_class_count = label_counts[single_class]

                        if rank == 0:
                            print(
                                f"    WARNING: Only class {single_class} found ({single_class_count} samples), setting to 0 for balance"
                            )

                        # Set to empty DataFrame to select 0 samples
                        balanced_df = pd.DataFrame(
                            columns=["text", "pseudo_label", "confidence"]
                        )

                num_balanced_samples = len(balanced_df)

                if rank == 0:
                    print(f"    After balancing: {num_balanced_samples} samples")

                # Check if we have enough samples after balancing
                if num_balanced_samples >= min_samples_required:
                    final_threshold = dynamic_threshold
                    high_confidence_pseudo_labeled_df = balanced_df
                    if rank == 0:
                        print(
                            f"    ✓ Sufficient samples found at threshold {dynamic_threshold:.3f}"
                        )
                    break

                # Lower the threshold
                dynamic_threshold = max(
                    CONFIDENCE_THRESHOLD_END,
                    dynamic_threshold - CONFIDENCE_DECAY_FACTOR,
                )
                final_threshold = dynamic_threshold

            # Try the minimum threshold as a final attempt
            if high_confidence_pseudo_labeled_df is None:
                if rank == 0:
                    print(f"  Trying minimum threshold {CONFIDENCE_THRESHOLD_END:.3f}")

                temp_high_conf_df = df_pseudo_labeled[
                    df_pseudo_labeled["confidence"] >= CONFIDENCE_THRESHOLD_END
                ]

                if rank == 0:
                    print(
                        f"  Threshold {CONFIDENCE_THRESHOLD_END:.3f}: Found {len(temp_high_conf_df)} samples before balancing"
                    )

                # Apply balancing to minimum threshold results
                balanced_df = (
                    temp_high_conf_df.copy()
                    if len(temp_high_conf_df) > 0
                    else pd.DataFrame(columns=["text", "pseudo_label", "confidence"])
                )

                if len(balanced_df) > 0:
                    label_counts = balanced_df["pseudo_label"].value_counts()

                    # Simple balancing: drop samples from majority class if ratio > 2:1
                    if len(label_counts) >= 2:  # Need at least two classes
                        class_0_count = label_counts.get(0, 0)
                        class_1_count = label_counts.get(1, 0)

                        if class_0_count > 0 and class_1_count > 0:
                            # Determine majority and minority classes
                            if class_0_count > class_1_count:
                                majority_class = 0
                                minority_class = 1
                                majority_count = class_0_count
                                minority_count = class_1_count
                            else:
                                majority_class = 1
                                minority_class = 0
                                majority_count = class_1_count
                                minority_count = class_0_count

                            # Check if we need to balance (ratio > 2:1)
                            if majority_count > 2 * minority_count:
                                # Downsample majority class to exactly 2 * minority_count
                                max_majority_samples = 2 * minority_count

                                # Keep all minority samples
                                minority_samples = balanced_df[
                                    balanced_df["pseudo_label"] == minority_class
                                ]

                                # Randomly sample majority samples
                                majority_samples = balanced_df[
                                    balanced_df["pseudo_label"] == majority_class
                                ].sample(
                                    n=max_majority_samples,
                                    random_state=RANDOM_SEED + iteration,
                                )

                                # Combine and shuffle
                                balanced_df = (
                                    pd.concat([minority_samples, majority_samples])
                                    .sample(
                                        frac=1, random_state=RANDOM_SEED + iteration
                                    )
                                    .reset_index(drop=True)
                                )

                                if rank == 0:
                                    print(
                                        f"    Balanced: reduced class {majority_class} from {majority_count} to {max_majority_samples}"
                                    )
                    elif len(label_counts) == 1:
                        # Only one class - this violates 2:1 ratio, so select 0 samples
                        single_class = list(label_counts.keys())[0]
                        single_class_count = label_counts[single_class]

                        if rank == 0:
                            print(
                                f"    WARNING: Only class {single_class} found ({single_class_count} samples), setting to 0 for balance"
                            )

                        # Set to empty DataFrame to select 0 samples
                        balanced_df = pd.DataFrame(
                            columns=["text", "pseudo_label", "confidence"]
                        )

                high_confidence_pseudo_labeled_df = balanced_df
                final_threshold = CONFIDENCE_THRESHOLD_END

                if rank == 0:
                    print(
                        f"    Final attempt at minimum threshold: {len(balanced_df)} samples after balancing"
                    )

            if rank == 0:
                if final_threshold != current_confidence_threshold:
                    print(
                        f"Threshold lowered from {current_confidence_threshold:.3f} to {final_threshold:.3f}",
                        flush=True,
                    )
                print(
                    f"Final result: {len(high_confidence_pseudo_labeled_df)} high-confidence pseudo-labeled samples (threshold: {final_threshold:.3f})",
                    flush=True,
                )
                print("Final pseudo-labeled data label distribution:")
                pseudo_label_dist = (
                    high_confidence_pseudo_labeled_df["pseudo_label"]
                    .value_counts()
                    .to_dict()
                )
                print(pseudo_label_dist)

            # Track pseudo-labeling statistics (before renaming columns)
            iteration_pseudo_label_stats.append(
                {
                    "iteration": iteration,
                    "original_confidence_threshold": current_confidence_threshold,
                    "final_confidence_threshold": final_threshold,
                    "threshold_was_lowered": final_threshold
                    != current_confidence_threshold,
                    "num_pseudo_labels": len(high_confidence_pseudo_labeled_df),
                    "label_distribution": high_confidence_pseudo_labeled_df[
                        "pseudo_label"
                    ]
                    .value_counts()
                    .to_dict(),
                    "avg_confidence": (
                        high_confidence_pseudo_labeled_df["confidence"].mean()
                        if len(high_confidence_pseudo_labeled_df) > 0
                        else 0.0
                    ),
                    "min_confidence": (
                        high_confidence_pseudo_labeled_df["confidence"].min()
                        if len(high_confidence_pseudo_labeled_df) > 0
                        else 0.0
                    ),
                    "max_confidence": (
                        high_confidence_pseudo_labeled_df["confidence"].max()
                        if len(high_confidence_pseudo_labeled_df) > 0
                        else 0.0
                    ),
                }
            )

            # Check if we should stop curriculum (all ranks check the same condition)
            should_stop = (
                len(high_confidence_pseudo_labeled_df) == 0
                and final_threshold == CONFIDENCE_THRESHOLD_END
            )

            if should_stop:
                if rank == 0:
                    print(
                        "No new high-confidence pseudo-labeled samples found and minimum threshold reached. Ending curriculum.",
                        flush=True,
                    )
                break  # No new data to learn from, stop.

            # Rename pseudo_label to label for consistency (all ranks do this)
            if len(high_confidence_pseudo_labeled_df) > 0:
                high_confidence_pseudo_labeled_df = (
                    high_confidence_pseudo_labeled_df.copy()
                )
                high_confidence_pseudo_labeled_df["label"] = (
                    high_confidence_pseudo_labeled_df["pseudo_label"]
                )
                high_confidence_pseudo_labeled_df = (
                    high_confidence_pseudo_labeled_df.drop(
                        ["pseudo_label", "confidence"], axis=1
                    )
                )
            else:
                # Empty DataFrame with correct columns
                high_confidence_pseudo_labeled_df = pd.DataFrame(
                    columns=["text", "label"]
                )

            # Combine training data based on Mean Teacher setting
            if USE_MEAN_TEACHER:
                # For Mean Teacher: Only use pseudo-labeled data (model already converged on initial data)
                if len(high_confidence_pseudo_labeled_df) > 0:
                    current_train_df = high_confidence_pseudo_labeled_df.sample(
                        frac=1, random_state=RANDOM_SEED
                    ).reset_index(drop=True)
                    if rank == 0:
                        print(
                            f"Mean Teacher: Using only {len(current_train_df)} pseudo-labeled samples (no initial data)",
                            flush=True,
                        )
                else:
                    # If no pseudo-labels, skip this iteration
                    if rank == 0:
                        print(
                            "Mean Teacher: No pseudo-labeled data available, skipping this iteration",
                            flush=True,
                        )
                    continue  # Skip to next iteration
            else:
                # Standard approach: Combine initial labeled training data with selected pseudo-labeled data
                current_train_df = (
                    pd.concat(
                        [
                            df_train_initial,
                            high_confidence_pseudo_labeled_df,  # Already has "label" column
                        ]
                    )
                    .sample(frac=1, random_state=RANDOM_SEED)
                    .reset_index(drop=True)
                )
            if rank == 0:
                end_time = time.time()
                elapsed = end_time - time_start
                if elapsed < 60:
                    print(
                        f"Teacher pseudo-labeling completed in {elapsed:.2f} seconds."
                    )
                elif elapsed < 3600:
                    print(
                        f"Teacher pseudo-labeling completed in {elapsed/60:.2f} minutes."
                    )
                else:
                    print(
                        f"Teacher pseudo-labeling completed in {elapsed/3600:.2f} hours."
                    )

            # Update class weights based on the new combined training set
            if len(current_train_df) == 0:
                if rank == 0:
                    print(
                        "Warning: No training data available for this iteration, skipping..."
                    )
                continue  # Skip to next iteration

            train_labels_current = current_train_df["label"].tolist()
            class_counts_current = Counter(train_labels_current)
            num_classes_current = len(class_counts_current)

            if num_classes_current > 0:
                sorted_class_counts = sorted(class_counts_current.items())
                total_samples_current = sum(count for _, count in sorted_class_counts)

                # Use balanced class weights: inverse of class frequency
                weights_current = []
                for class_label, count in sorted_class_counts:
                    weight = total_samples_current / count  # Inverse frequency
                    weights_current.append(weight)

                # Normalize weights so they average to 1.0
                avg_weight = sum(weights_current) / len(weights_current)
                weights_current = [w / avg_weight for w in weights_current]

                class_weights = torch.tensor(weights_current, dtype=torch.float)
                if rank == 0:
                    print(f"Current class counts: {dict(sorted_class_counts)}")
                    print(
                        f"Updated Class Weights (normalized inverse frequency): {class_weights.tolist()}"
                    )
                    if len(weights_current) >= 2:
                        print(
                            f"Class 0 weight: {class_weights[0]:.4f}, Class 1 weight: {class_weights[1]:.4f}"
                        )
            else:
                class_weights = None
                if rank == 0:
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
            # Create distributed sampler for curriculum training data
            train_sampler = DistributedSampler(
                current_train_dataset, num_replicas=world_size, rank=rank, shuffle=True
            )
            current_train_dataloader = DataLoader(
                current_train_dataset, batch_size=BATCH_SIZE, sampler=train_sampler
            )

            # Track training data size for this iteration
            iteration_training_data_sizes.append(len(current_train_dataset))

            if rank == 0:
                print(
                    f"Current Training DataLoader batches: {len(current_train_dataloader)}"
                )

            # 2. Student Training on Current Curriculum
            if USE_MEAN_TEACHER:
                # Mean Teacher: Load best model to accumulate knowledge across curriculum steps
                if rank == 0:
                    if overall_best_model_save_path:
                        print(
                            "Mean Teacher: Loading best student model for knowledge accumulation...",
                            flush=True,
                        )
                    else:
                        print(
                            "Mean Teacher: No best model available, continuing with current model...",
                            flush=True,
                        )

                # For Mean Teacher, load the best model (no reinitialization)
                if overall_best_model_save_path:
                    # Robust model loading with retry logic
                    model_loaded = False
                    max_retries = 3
                    retry_count = 0

                    while not model_loaded and retry_count < max_retries:
                        try:
                            # Add robust distributed barriers and cleanup
                            barrier_success = safe_barrier()
                            if not barrier_success:
                                if rank == 0:
                                    print(
                                        "ERROR: Failed to synchronize ranks during model loading. Aborting.",
                                        flush=True,
                                    )
                                break  # Exit the retry loop

                            # Clean up previous model completely
                            if "model" in locals():
                                del model
                                gc.collect()
                                torch.cuda.empty_cache()

                            # Additional barrier after cleanup
                            safe_barrier()

                            # Give processes more time to synchronize on retries
                            time.sleep(1 + retry_count)

                            # Load the best model for this curriculum iteration (accumulate knowledge)
                            model = load_ddp_model(
                                MODEL_NAME,
                                2,
                                USE_QLORA,
                                bnb_config,
                                lora_config,
                                rank,
                                tokenizer,
                                model_path=overall_best_model_save_path,  # Continue from best model
                            )

                            # Final barrier after model loading to ensure all ranks are ready
                            safe_barrier()
                            model_loaded = True

                            if rank == 0:
                                print(
                                    "Mean Teacher: Student model loaded successfully from best checkpoint!",
                                    flush=True,
                                )

                        except Exception as e:
                            retry_count += 1
                            if rank == 0:
                                print(
                                    f"Model loading attempt {retry_count} failed: {e}",
                                    flush=True,
                                )
                                if retry_count < max_retries:
                                    print(
                                        f"Retrying... ({retry_count}/{max_retries})",
                                        flush=True,
                                    )
                                else:
                                    print(
                                        "Max retries reached. Continuing with current model.",
                                        flush=True,
                                    )

                            # Clean up on failed attempt
                            if "model" in locals():
                                try:
                                    del model
                                    gc.collect()
                                    torch.cuda.empty_cache()
                                except Exception:
                                    pass

                            # Wait longer before retry
                            time.sleep(2 + retry_count)

                    if not model_loaded and rank == 0:
                        print(
                            "WARNING: Failed to load best model, continuing with current student model.",
                            flush=True,
                        )

                # Teacher model continues to be updated via EMA during training
                # No need to reset teacher model here - it will be updated gradually
                if rank == 0:
                    print(
                        "Mean Teacher: Teacher model will be updated via EMA during training",
                        flush=True,
                    )

            else:
                # Original approach: Reinitialize student model for each curriculum iteration
                if rank == 0:
                    print(
                        "Standard approach: Reinitializing student model for curriculum iteration...",
                        flush=True,
                    )

                # Robust model reinitialization with retry logic
                model_initialized = False
                max_retries = 3
                retry_count = 0

                while not model_initialized and retry_count < max_retries:
                    try:
                        # Add robust distributed barriers and cleanup
                        barrier_success = safe_barrier()
                        if not barrier_success:
                            if rank == 0:
                                print(
                                    "ERROR: Failed to synchronize ranks during model reinitialization. Aborting.",
                                    flush=True,
                                )
                            break  # Exit the retry loop

                        # Clean up previous model completely
                        if "model" in locals():
                            del model
                            gc.collect()
                            torch.cuda.empty_cache()

                        # Additional barrier after cleanup
                        safe_barrier()

                        # Give processes more time to synchronize on retries
                        time.sleep(1 + retry_count)

                        # Reinitialize model from scratch (original approach)
                        model = load_ddp_model(
                            MODEL_NAME,
                            2,
                            USE_QLORA,
                            bnb_config,
                            lora_config,
                            rank,
                            tokenizer,
                            model_path=None,  # Start fresh
                        )

                        # Final barrier after model loading to ensure all ranks are ready
                        safe_barrier()
                        model_initialized = True

                        if rank == 0:
                            print(
                                "Standard approach: Student model reinitialized successfully!",
                                flush=True,
                            )

                    except Exception as e:
                        retry_count += 1
                        if rank == 0:
                            print(
                                f"Model reinitialization attempt {retry_count} failed: {e}",
                                flush=True,
                            )
                            if retry_count < max_retries:
                                print(
                                    f"Retrying... ({retry_count}/{max_retries})",
                                    flush=True,
                                )
                            else:
                                print(
                                    "Max retries reached. This may cause training instability.",
                                    flush=True,
                                )

                        # Clean up on failed attempt
                        if "model" in locals():
                            try:
                                del model
                                gc.collect()
                                torch.cuda.empty_cache()
                            except Exception:
                                pass

                        # Wait longer before retry
                        time.sleep(2 + retry_count)

                if not model_initialized:
                    if rank == 0:
                        print(
                            "ERROR: Model reinitialization failed completely. Cannot continue curriculum training.",
                            flush=True,
                        )
                    # Stop curriculum learning if model loading fails
                    cleanup()
                    break  # Exit curriculum loop

            optimizer = setup_optimizer_with_weight_decay(
                model, effective_learning_rate, WEIGHT_DECAY, BACKBONE_WEIGHT_DECAY
            )
            scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer, mode="min", patience=SCHEDULER_PATIENCE
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
                    rank=rank,
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
                    # Mean Teacher parameters
                    teacher_model=teacher_model,
                    use_mean_teacher=USE_MEAN_TEACHER,
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
                # Use helper function to save DDP model correctly
                save_ddp_model(model, overall_best_model_save_path, USE_QLORA)

                # Also save teacher model if using Mean Teacher
                if USE_MEAN_TEACHER and teacher_model is not None:
                    teacher_save_path = (
                        f"{OUT_DIR}best_teacher_overall_iter_{iteration}"
                    )
                    save_teacher_model(teacher_model, teacher_save_path, USE_QLORA)
                    if rank == 0:
                        print(f"Saved teacher model to {teacher_save_path}")

                if rank == 0:
                    print(
                        f"Saved OVERALL best model to {overall_best_model_save_path} with Combined Dev Loss: {best_combined_val_loss_overall:.4f}"
                    )
            else:
                epochs_no_improve_overall += 1
                if rank == 0:
                    print(
                        f"No improvement in Overall Combined Dev Loss for {epochs_no_improve_overall} curriculum iterations."
                    )
                if epochs_no_improve_overall >= EARLY_STOPPING_PATIENCE:
                    if rank == 0:
                        print(
                            f"Overall curriculum early stopping triggered after {iteration} iterations."
                        )
                    break

            # 3. Adjust Curriculum Difficulty
            current_confidence_threshold = max(
                CONFIDENCE_THRESHOLD_END,
                current_confidence_threshold - CONFIDENCE_DECAY_FACTOR,
            )
            if rank == 0:
                print(
                    f"Next confidence threshold for pseudo-labeling: {current_confidence_threshold:.2f}"
                )

        if rank == 0:
            print("\n--- Curriculum Training Complete ---")

    if rank == 0:
        print("\n--- Final Evaluation on Test Sets ---")
    # Check for the correct file extension based on USE_QLORA setting
    if overall_best_model_save_path:
        if USE_QLORA:
            model_path_to_load = overall_best_model_save_path
            # Wait for model to be fully written and validate it
            model_exists = wait_for_model_file(model_path_to_load, max_wait_time=60)
        else:
            model_path_to_load = f"{overall_best_model_save_path}.pt"
            model_exists = wait_for_model_file(model_path_to_load, max_wait_time=60)

        if model_exists:
            if USE_QLORA:
                del model  # Ensure previous model is gone
                gc.collect()
                torch.cuda.empty_cache()

                # Add distributed barrier before loading
                if torch.distributed.is_initialized():
                    safe_barrier()

                # Load the base model and then apply the saved PEFT adapters
                model = load_base_model_and_apply_peft(
                    MODEL_NAME,
                    2,
                    USE_QLORA,
                    bnb_config,
                    lora_config,
                    rank,
                    tokenizer,
                    peft_model_path=overall_best_model_save_path,
                )
                model = DDP(model, device_ids=[rank], find_unused_parameters=False)
                # Explicitly set gradient checkpointing with use_reentrant=False to avoid warning
                if hasattr(model.module, "gradient_checkpointing_enable"):
                    model.module.gradient_checkpointing_enable(
                        gradient_checkpointing_kwargs={"use_reentrant": False}
                    )
            else:
                # Load the overall best model state dict
                model = AutoModelForSequenceClassification.from_pretrained(
                    MODEL_NAME, num_labels=2
                )
                model.config.pad_token_id = tokenizer.pad_token_id

                # Set classifier dropout if the model supports it
                if hasattr(model.config, "classifier_dropout"):
                    model.config.classifier_dropout = CLASSIFIER_DROPOUT
                elif hasattr(model.config, "hidden_dropout_prob"):
                    # For some models, classifier dropout is controlled by hidden_dropout_prob
                    model.config.hidden_dropout_prob = max(
                        model.config.hidden_dropout_prob, CLASSIFIER_DROPOUT
                    )

                model.load_state_dict(
                    torch.load(model_path_to_load, map_location=f"cuda:{rank}")
                )
                model.to(rank)
                model = DDP(model, device_ids=[rank], find_unused_parameters=False)
            model.eval()  # Set to eval mode for inference
            if rank == 0:
                print(
                    f"Loaded overall best model from {model_path_to_load} for final test evaluation."
                )
        else:
            if rank == 0:
                print(
                    "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for test evaluation."
                )
    else:
        if rank == 0:
            print(
                "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for test evaluation."
            )

    # Ensure all ranks are synchronized before evaluation
    if torch.distributed.is_initialized():
        safe_barrier()

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
    if rank == 0:
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
    if rank == 0:
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
    if rank == 0:
        print(
            f"Reddit Final Test Loss: {test_loss_reddit:.4f}, Test Accuracy: {test_acc_reddit:.4f}, Test Precision: {test_precision_reddit:.4f}, Test Recall: {test_recall_reddit:.4f}, Test F1-score: {test_f1_reddit:.4f}"
        )
    end_time = time.time()
    if rank == 0:
        print(f"Final evaluation completed in {end_time - start_time:.2f} seconds.")

    # --- Save Performance Metrics to JSON ---
    performance_metrics = {
        "curriculum_iteration_metadata": {  # Information about each curriculum iteration
            "iteration_boundaries": {
                "epoch_start_indices": [0]
                + [
                    10 * i for i in range(1, len(iteration_pseudo_label_stats) + 1)
                ],  # Approximate based on 10 epochs per iteration
                "num_iterations_completed": len(iteration_pseudo_label_stats)
                + 1,  # +1 for initial training
            },
            "pseudo_labeling_stats": iteration_pseudo_label_stats,  # Stats per iteration
            "confidence_thresholds_used": iteration_confidence_thresholds,  # Confidence threshold per iteration
            "training_data_sizes": [len(current_train_dataset)]
            + iteration_training_data_sizes,  # Include initial + curriculum sizes
        },
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
            ),
        },
        "model_details": {
            "model_name": MODEL_NAME,
            "max_len": MAX_LEN,
            "batch_size": BATCH_SIZE,
            "learning_rate": LEARNING_RATE,
            "initial_training_epochs": INITIAL_TRAINING_EPOCHS,
            "epochs_per_curriculum_iteration": STUDENT_TEACHER_EPOCHS_PER_ITERATION,
            "max_curriculum_iterations": MAX_CURRICULUM_ITERATIONS,
            "confidence_threshold_start": CONFIDENCE_THRESHOLD_START,
            "confidence_threshold_end": CONFIDENCE_THRESHOLD_END,
            "confidence_decay_factor": CONFIDENCE_DECAY_FACTOR,
            "pseudo_labeling_temperature": PSEUDO_LABELING_TEMPERATURE,
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

    # --- Save Performance Metrics to JSON (only from rank 0) ---
    if rank == 0:
        os.makedirs(
            os.path.dirname(PERFORMANCE_FILE) or ".", exist_ok=True
        )  # Ensure directory exists, or create in current if no path
        with open(PERFORMANCE_FILE, "w") as f:
            json.dump(performance_metrics, f, indent=4)
        if rank == 0:
            print(f"\nAll performance metrics saved to {PERFORMANCE_FILE}")

        if rank == 0:
            print("\nCurriculum Learning Training and Evaluation Complete.")
    cleanup()


def main():
    """Main function to launch distributed training or testing"""
    # Force unbuffered output
    os.environ["PYTHONUNBUFFERED"] = "1"

    # Check command line arguments for mode
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == "test":
            print("Running in testing mode - skipping training", flush=True)
            run_checkpoint_testing()
            return
        elif mode == "annotate":
            print("Running in corpus annotation mode", flush=True)
        elif mode == "train":
            print("Running in training mode", flush=True)
            # Continue with normal training
        else:
            print(
                f"Unknown mode: {mode}. Valid modes are: train, test, annotate",
                flush=True,
            )
            return

    # Check if we're in testing mode (from config)
    if TESTING_MODE_ONLY:
        print("Running in testing mode - skipping training", flush=True)
        run_checkpoint_testing()
        return

    # Detect if running on Kaggle
    is_kaggle = os.path.exists("/kaggle")

    # Set environment variables for distributed training
    if is_kaggle:
        # Kaggle-specific environment setup
        os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
        os.environ.setdefault("MASTER_PORT", "12355")
        # Kaggle typically has 2 GPUs (T4 or P100)
        os.environ.setdefault("NCCL_DEBUG", "WARN")  # Less verbose on Kaggle
        print("Detected Kaggle environment", flush=True)
    else:
        # HPC/Local environment setup
        os.environ.setdefault("MASTER_ADDR", "localhost")
        os.environ.setdefault("MASTER_PORT", "12355")
        os.environ.setdefault("NCCL_DEBUG", "INFO")
        os.environ.setdefault("TORCH_DISTRIBUTED_DEBUG", "DETAIL")
        print("Detected HPC/Local environment", flush=True)

    # Number of GPUs to use
    world_size = torch.cuda.device_count()
    print(f"Starting distributed training on {world_size} GPUs", flush=True)

    if world_size < 2:
        print("Warning: Less than 2 GPUs available. Running on single GPU.", flush=True)
        try:
            training(0, 1)
        except Exception as e:
            print(f"Training failed: {e}", flush=True)
            raise e
    else:
        try:
            mp.spawn(training, args=(world_size,), nprocs=world_size, join=True)
        except Exception as e:
            print(f"Distributed training failed: {e}", flush=True)
            print(
                "This might be due to NCCL communication issues or GPU memory problems.",
                flush=True,
            )
            raise e


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTraining interrupted by user", flush=True)
    except Exception as e:
        print(f"\nTraining failed with error: {e}", flush=True)
        import traceback

        traceback.print_exc()
        sys.exit(1)
