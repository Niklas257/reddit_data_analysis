import os
from huggingface_hub import login


class ClassifierConfig:
    """
    Configuration class for the classifier model.
    """

    def __init__(self):
        # --- Configuration ---
        qwen = False  # Set to True if using Qwen model, False for ModernBERT
        if qwen:
            self.MODEL_NAME = "Qwen/Qwen3-0.6B-Base"
        else:
            self.MODEL_NAME = "answerdotai/ModernBERT-base"
        self.MAX_LEN = 4096
        self.BATCH_SIZE = 8
        self.LEARNING_RATE = 5e-7
        self.RANDOM_SEED = 42
        self.EARLY_STOPPING_PATIENCE = 6

        # In-epoch early stopping and LR scheduling
        self.USE_IN_EPOCH_EARLY_STOPPING = (
            True  # Use in-epoch validation for early stopping
        )
        self.IN_EPOCH_EVAL_STEPS = 50  # Evaluate every 50 batches for early stopping
        self.IN_EPOCH_EARLY_STOPPING_PATIENCE = (
            12  # Number of evaluation steps without improvement
        )
        self.IN_EPOCH_LR_SCHEDULING = True  # Use in-epoch validation for LR scheduling
        self.SCHEDULER_PATIENCE = 4

        if qwen:
            DIR = "qwen"
        else:
            DIR = "modernbert"
        self.OUT_DIR = f"../training_data/{DIR}/"  # Output directory for model checkpoints and performance metrics
        self.PERFORMANCE_FILE = f"../training_data/{DIR}/performance_metrics_{DIR}.json"
        self.YNACC_FILE_PATH = "../training_data/ynacc_processed.jsonl"
        self.IAC_FILE_PATH = "../training_data/iac_processed.jsonl"
        self.REDDIT_UNLABELED_FILE_PATH = "../training_data/reddit_train.jsonl"
        self.REDDIT_VAL_FILE_PATH = "../training_data/reddit_val.jsonl"
        self.REDDIT_TEST_FILE_PATH = "../training_data/reddit_test.jsonl"

        self.MAX_CURRICULUM_ITERATIONS = 10  # Max number of curriculum steps
        self.STUDENT_TEACHER_EPOCHS_PER_ITERATION = (
            25  # Number of epochs the student trains on the current curriculum
        )
        self.CONFIDENCE_THRESHOLD_START = (
            0.95  # Initial high confidence for pseudo-labeling
        )
        self.CONFIDENCE_THRESHOLD_END = 0.70  # Final lower confidence threshold
        self.CONFIDENCE_DECAY_FACTOR = (
            0.03  # How much the confidence threshold decreases per iteration
        )
        self.UNLABELED_DATA_FRACTION_PER_STEP = 0.1  # Fraction of unlabeled data to consider for pseudo-labeling in each step, helps with large datasets
        self.PSEUDO_LABEL_BATCH_SIZE = (
            self.BATCH_SIZE * 4
        )  # Larger batch size for pseudo-labeling for efficiency (keep low to avoid OOM issues on smaller GPUs or large models)

        self.SUPERVISED_TRAINING_ONLY = True  # Set to True to skip curriculum learning and only do supervised training
        self.USE_QLORA = False  # Set to True to enable QLoRA
        self.lora_rank = 16  # Rank for QLoRA, can be adjusted based on model capacity
        self.lora_alpha = 32  # Scaling factor for LoRA, typically 2x the rank
        self.bnb_bits = (
            4  # Bits for quantization, can be 4 or 8 depending on model and GPU support
        )

        # Regularization parameters
        self.classifier_dropout = 0.3  # Dropout probability for the classification head
        self.weight_decay = 0.01  # Weight decay for the classification head
        self.backbone_weight_decay = (
            0.001  # Weight decay for backbone parameters (lower than classifier)
        )

        self.hf_token = os.environ.get(
            "HF_TOKEN"
        )  # Hugging Face token for model access

    def login_to_huggingface(self, rank=0):
        """
        Login to Hugging Face Hub using the provided token.
        """
        if self.hf_token:
            try:
                login(token=self.hf_token)
                if rank == 0:
                    print("Successfully logged in to HuggingFace Hub", flush=True)
            except ValueError as e:
                if "not found in" in str(e) and "stored_tokens" in str(e):
                    # Token name provided instead of actual token, or token already active
                    if rank == 0:
                        print(
                            "HuggingFace token already active or using environment variable",
                            flush=True,
                        )
                else:
                    raise
        else:
            raise ValueError(
                "Hugging Face token is not set. Please set the HF_TOKEN environment variable."
            )
