import json
import time
import sys
import gc
from datetime import timedelta
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
import torch
from torch.utils.data import Dataset, DataLoader, ConcatDataset
import transformers
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
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

# Regularization parameters
CLASSIFIER_DROPOUT = config.classifier_dropout
WEIGHT_DECAY = config.weight_decay
BACKBONE_WEIGHT_DECAY = config.backbone_weight_decay

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

    # Keep track of total samples processed for accurate in-epoch accuracy
    total_samples_processed_in_epoch = 0

    # In-epoch early stopping variables
    if best_val_loss_in_epoch is None:
        best_val_loss_in_epoch = float("inf")
    current_epochs_no_improve = epochs_no_improve_in_epoch
    early_stop_triggered = False

    # Track the best model path for immediate saving
    best_model_path_in_epoch = None

    # Define loss function with class weights if provided
    loss_fct = nn.CrossEntropyLoss(
        weight=class_weights.to(device) if class_weights is not None else None
    )

    if rank == 0:
        print("Step    | Train Loss | Train Acc | Val Loss | Val Acc | Val F1")
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
                    val_loss_ynacc, val_acc_ynacc, _, _, val_f1_ynacc, _, _ = (
                        evaluate_model(
                            model, val_ynacc_dataloader, device, class_weights
                        )
                    )
                    val_loss_iac, val_acc_iac, _, _, val_f1_iac, _, _ = evaluate_model(
                        model, val_iac_dataloader, device, class_weights
                    )
                    val_loss_reddit, val_acc_reddit, _, _, val_f1_reddit, _, _ = (
                        evaluate_model(
                            model, val_reddit_dataloader, device, class_weights
                        )
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
                        f"{step + 1:03d}/{len(train_data_loader)} | {np.mean(losses[-IN_EPOCH_EVAL_STEPS:]):.4f}     | "
                        f"{(correct_predictions.double() / total_samples_processed_in_epoch).item():.4f}    | "
                        f"{val_loss:.4f}   | {val_acc:.4f}  | {val_f1:.4f} (weighted)"
                    )
                    print(
                        f"    → Individual losses: YNACC={val_loss_ynacc:.4f}, IAC={val_loss_iac:.4f}, Reddit={val_loss_reddit:.4f}"
                    )
            else:
                # Fallback to single validation set evaluation
                val_loss, val_acc, _, _, val_f1, _, _ = evaluate_model(
                    model,
                    val_data_loader,
                    device,
                    class_weights,
                )

                if rank == 0:
                    print(
                        f"{step + 1:03d}/{len(train_data_loader)} | {np.mean(losses[-IN_EPOCH_EVAL_STEPS:]):.4f}     | "
                        f"{(correct_predictions.double() / total_samples_processed_in_epoch).item():.4f}    | "
                        f"{val_loss:.4f}   | {val_acc:.4f}  | {val_f1:.4f}"
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

    # In-epoch early stopping variables
    best_val_loss_in_epoch = float("inf")
    epochs_no_improve_in_epoch = 0

    # Training loop for the current curriculum iteration
    for epoch_in_iter in range(STUDENT_TEACHER_EPOCHS_PER_ITERATION):
        if rank == 0:
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
        )

        # Collect batch-wise metrics
        in_epoch_train_losses_batch.extend(batch_train_losses)
        in_epoch_train_accuracies_batch.extend(batch_train_accuracies)
        in_epoch_val_losses_batch_from_train_epoch.extend(batch_val_losses_from_te)
        in_epoch_val_accuracies_batch_from_train_epoch.extend(
            batch_val_accuracies_from_te
        )
        in_epoch_val_f1_batch_from_train_epoch.extend(batch_val_f1s_from_te)

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
        val_loss_ynacc, val_acc_ynacc, _, _, val_f1_ynacc, _, _ = evaluate_model(
            eval_model, val_ynacc_dataloader, device, class_weights
        )
        if rank == 0:
            print(
                f"YNACC Dev Loss: {val_loss_ynacc:.4f}, Acc: {val_acc_ynacc:.4f}, F1: {val_f1_ynacc:.4f}"
            )

        # Evaluate on IAC Dev Set
        val_loss_iac, val_acc_iac, _, _, val_f1_iac, _, _ = evaluate_model(
            eval_model, val_iac_dataloader, device, class_weights
        )
        if rank == 0:
            print(
                f"IAC Dev Loss: {val_loss_iac:.4f}, Acc: {val_acc_iac:.4f}, F1: {val_f1_iac:.4f}"
            )

        # Evaluate on Reddit Validation Set
        val_loss_reddit, val_acc_reddit, _, _, val_f1_reddit, _, _ = evaluate_model(
            eval_model, val_reddit_dataloader, device, class_weights
        )
        if rank == 0:
            print(
                f"Reddit Dev Loss: {val_loss_reddit:.4f}, Acc: {val_acc_reddit:.4f}, F1: {val_f1_reddit:.4f}"
            )

        # Clean up the evaluation model if we created one
        if best_model_for_eval is not None:
            del best_model_for_eval
            gc.collect()
            torch.cuda.empty_cache()

        # Restore current training model state (no action needed since we didn't modify `model`)

        """
        # Calculate combined validation loss based on iteration
        if iteration == 0:
            # For initial training (iteration 0), use only YNACC and IAC
            current_combined_val_loss_for_scheduler = (
                val_loss_ynacc + val_loss_iac
            ) / 2
            loss_description = "Combined Dev Loss (YNACC+IAC)"
        else:
            """
        # For curriculum iterations, use all three dev sets
        current_combined_val_loss_for_scheduler = (
            0.5 * val_loss_ynacc + 0.5 * val_loss_iac + val_loss_reddit
        ) / 2
        loss_description = "Combined Dev Loss (YNACC+IAC+Reddit) (reddit weighted 2x)"

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

    if (
        df_ynacc_raw.empty or df_iac_raw.empty or df_unlabeled_reddit_raw.empty
        if not SUPERVISED_TRAINING_ONLY
        else False or df_reddit_val.empty or df_reddit_test.empty
    ):
        if rank == 0:
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
        weights = [
            total_samples / (num_classes * count) for _, count in sorted_class_counts
        ]
        class_weights = torch.tensor(weights, dtype=torch.float)
        if rank == 0:
            print(
                f"Calculated Class Weights (based on initial training data): {class_weights.tolist()}"
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

    # Initial training phase (Curriculum Iteration 0)
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
            combined_val_dataloader=combined_val_dataloader_3,
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

    if not SUPERVISED_TRAINING_ONLY:
        # --- Curriculum Learning Iterations ---
        for iteration in range(1, MAX_CURRICULUM_ITERATIONS + 1):
            if rank == 0:
                print(
                    f"\n--- Curriculum Iteration {iteration}/{MAX_CURRICULUM_ITERATIONS} ---"
                )

            # Track iteration metadata
            iteration_confidence_thresholds.append(current_confidence_threshold)

            # 1. Teacher Pseudo-Labeling
            if rank == 0:
                print(
                    f"Teacher pseudo-labeling unlabeled Reddit data with confidence threshold: {current_confidence_threshold:.2f}"
                )
                print(
                    f"Sampling {UNLABELED_DATA_FRACTION_PER_STEP:.2%} of unlabeled data for pseudo-labeling"
                )

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
                        model = DDP(
                            model, device_ids=[rank], find_unused_parameters=False
                        )
                    model.eval()  # Set to eval mode for inference
                    if rank == 0:
                        print(
                            f"Loaded overall best model from {model_path_to_load} for pseudo-labeling."
                        )
                else:
                    if rank == 0:
                        print(
                            "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for pseudo-labeling."
                        )
            else:
                if rank == 0:
                    print(
                        "No overall best model saved or path is invalid. Using the model from the last curriculum epoch for pseudo-labeling."
                    )

            # Ensure all ranks are synchronized before pseudo-labeling
            if torch.distributed.is_initialized():
                safe_barrier()

            time_start = time.time()
            # First, sample the fraction of unlabeled data to process in this iteration
            if UNLABELED_DATA_FRACTION_PER_STEP < 1.0:
                df_unlabeled_sampled = df_unlabeled_reddit_raw.sample(
                    frac=UNLABELED_DATA_FRACTION_PER_STEP,
                    random_state=RANDOM_SEED
                    + iteration,  # Different seed per iteration
                ).reset_index(drop=True)
            else:
                df_unlabeled_sampled = df_unlabeled_reddit_raw.copy()

            if rank == 0:
                print(
                    f"Processing {len(df_unlabeled_sampled)} unlabeled samples for pseudo-labeling"
                )

            # Multi-GPU distributed pseudo-labeling approach
            unlabeled_texts = df_unlabeled_sampled["text"].tolist()
            unlabeled_dataset = CommentDataset(
                unlabeled_texts, [0] * len(unlabeled_texts), tokenizer, MAX_LEN
            )  # Dummy labels

            # Create distributed sampler for multi-GPU pseudo-labeling
            unlabeled_sampler = DistributedSampler(
                unlabeled_dataset, num_replicas=world_size, rank=rank, shuffle=False
            )
            unlabeled_dataloader = DataLoader(
                unlabeled_dataset,
                batch_size=PSEUDO_LABEL_BATCH_SIZE,
                sampler=unlabeled_sampler,
            )

            # Each rank processes its portion of the data
            local_pseudo_labels = []
            local_confidences = []
            local_texts = []

            with torch.no_grad():
                for batch in unlabeled_dataloader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)
                    texts = batch["text"]  # Get the text data from the batch

                    outputs = model(input_ids=input_ids, attention_mask=attention_mask)
                    logits = outputs.logits
                    probabilities = torch.softmax(logits, dim=-1)
                    max_confidences, predicted_labels = torch.max(probabilities, dim=-1)

                    local_pseudo_labels.extend(predicted_labels.cpu().tolist())
                    local_confidences.extend(max_confidences.cpu().tolist())
                    local_texts.extend(texts)

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

            # Select high-confidence pseudo-labeled data for the current curriculum step
            # All ranks now have the complete pseudo-labeled data
            high_confidence_pseudo_labeled_df = df_pseudo_labeled[
                df_pseudo_labeled["confidence"] >= current_confidence_threshold
            ]

            if rank == 0:
                print(
                    f"Selected {len(high_confidence_pseudo_labeled_df)} high-confidence pseudo-labeled samples for training.",
                    flush=True,
                )
                print("Pseudo-labeled data label distribution:")
                pseudo_label_dist = (
                    high_confidence_pseudo_labeled_df["pseudo_label"]
                    .value_counts()
                    .to_dict()
                )
                print(pseudo_label_dist)

            # Track pseudo-labeling statistics
            iteration_pseudo_label_stats.append(
                {
                    "iteration": iteration,
                    "confidence_threshold": current_confidence_threshold,
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
                and current_confidence_threshold == CONFIDENCE_THRESHOLD_END
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

            # Combine initial labeled training data with selected pseudo-labeled data
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
                if rank == 0:
                    print(f"Updated Class Weights: {class_weights.tolist()}")
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
            # Initialize a new model for the student in this iteration to prevent overfitting
            # on easier samples that are included in every curriculum iteration

            if rank == 0:
                print("Reinitializing model for curriculum iteration...", flush=True)

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

                    # Reinitialize model from scratch for this curriculum iteration
                    model = load_ddp_model(
                        MODEL_NAME,
                        2,
                        USE_QLORA,
                        bnb_config,
                        lora_config,
                        rank,
                        tokenizer,
                        model_path=None,  # Start fresh, don't load previous weights
                    )

                    # Final barrier after model loading to ensure all ranks are ready
                    safe_barrier()
                    model_initialized = True

                    if rank == 0:
                        print("Model reinitialization successful!", flush=True)

                except Exception as e:
                    retry_count += 1
                    if rank == 0:
                        print(
                            f"Model reinitialization attempt {retry_count} failed: {e}",
                            flush=True,
                        )
                        if retry_count < max_retries:
                            print(
                                f"Retrying... ({retry_count}/{max_retries})", flush=True
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
                # Stop curriculum learning if model reinitialization fails
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
    """Main function to launch distributed training"""
    # Force unbuffered output
    os.environ["PYTHONUNBUFFERED"] = "1"

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
