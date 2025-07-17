from huggingface_hub import login
from kaggle_secrets import UserSecretsClient


class ClassifierConfig:
    """
    Configuration class for the classifier model.
    """

    def __init__(self):
        # --- Configuration ---
        self.MODEL_NAME = "answerdotai/ModernBERT-base"
        self.MAX_LEN = 4096
        self.BATCH_SIZE = 16
        self.LEARNING_RATE = 1e-4
        self.RANDOM_SEED = 42
        self.EARLY_STOPPING_PATIENCE = 3
        self.OUT_DIR = "/kaggle/working/"  # Output directory for model checkpoints and performance metrics
        self.PERFORMANCE_FILE = f"{self.OUT_DIR}performance_metrics_model.json"
        self.YNACC_FILE_PATH = "/kaggle/input/ynacc-processed/ynacc_processed.jsonl"
        self.IAC_FILE_PATH = "/kaggle/input/iac-processed/iac_processed.jsonl"
        self.REDDIT_UNLABELED_FILE_PATH = "/kaggle/input/reddit-data/reddit_train.jsonl"
        self.REDDIT_VAL_FILE_PATH = "/kaggle/input/reddit-data/reddit_val.jsonl"
        self.REDDIT_TEST_FILE_PATH = "/kaggle/input/reddit-data/reddit_test.jsonl"

        self.MAX_CURRICULUM_ITERATIONS = 10  # Max number of curriculum steps
        self.STUDENT_TEACHER_EPOCHS_PER_ITERATION = (
            10  # Number of epochs the student trains on the current curriculum
        )
        self.CONFIDENCE_THRESHOLD_START = (
            0.95  # Initial high confidence for pseudo-labeling
        )
        self.CONFIDENCE_THRESHOLD_END = 0.60  # Final lower confidence threshold
        self.CONFIDENCE_DECAY_FACTOR = (
            0.05  # How much the confidence threshold decreases per iteration
        )
        self.UNLABELED_DATA_FRACTION_PER_STEP = 1  # Fraction of unlabeled data to consider for pseudo-labeling in each step, helps with large datasets
        self.PSEUDO_LABEL_BATCH_SIZE = (
            self.BATCH_SIZE
        )  # Larger batch size for pseudo-labeling for efficiency (keep low to avoid OOM issues on smaller GPUs or large models)

        self.SUPERVISED_TRAINING_ONLY = False  # Set to True to skip curriculum learning and only do supervised training
        self.USE_QLORA = True  # Set to True to enable QLoRA
        self.lora_rank = 16  # Rank for QLoRA, can be adjusted based on model capacity
        self.lora_alpha = 32  # Scaling factor for LoRA, typically 2x the rank
        self.bnb_bits = (
            4  # Bits for quantization, can be 4 or 8 depending on model and GPU support
        )
        user_secrets = UserSecretsClient()
        self.hf_token = user_secrets.get_secret("HF_TOKEN")

    def login_to_huggingface(self):
        """
        Login to Hugging Face Hub using the provided token.
        """
        if self.hf_token:
            login(token=self.hf_token)
        else:
            raise ValueError(
                "Hugging Face token is not set. Please set the HF_TOKEN environment variable."
            )
