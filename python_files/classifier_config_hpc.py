import os
from huggingface_hub import login


class ClassifierConfig:
    """
    Configuration class for the classifier model.
    """

    def __init__(self):
        # --- Configuration ---
        qwen = True  # Set to True if using Qwen model, False for ModernBERT
        if qwen:
            self.MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"

        else:
            self.MODEL_NAME = "answerdotai/ModernBERT-base"
        self.MAX_LEN = 4096
        self.BATCH_SIZE = 8
        self.LEARNING_RATE = 5e-7
        self.RANDOM_SEED = 42
        self.EARLY_STOPPING_PATIENCE = 10

        # In-epoch early stopping and LR scheduling
        self.USE_IN_EPOCH_EARLY_STOPPING = (
            True  # Use in-epoch validation for early stopping
        )
        self.IN_EPOCH_EVAL_STEPS = 20  # Evaluate every 20 batches for early stopping
        self.IN_EPOCH_EARLY_STOPPING_PATIENCE = (
            20  # Number of evaluation steps without improvement
        )
        self.IN_EPOCH_LR_SCHEDULING = True  # Use in-epoch validation for LR scheduling
        self.SCHEDULER_PATIENCE = 10

        # Mean Teacher configuration
        self.USE_MEAN_TEACHER = (
            False  # Set to True to use Mean Teacher approach for curriculum learning
        )
        self.EMA_DECAY = 0.999  # Exponential Moving Average decay factor for teacher model (0.999 is common)

        if qwen:
            DIR = "qwen"
        elif self.USE_MEAN_TEACHER:
            DIR = "modernbert_mean_teacher"
        else:
            DIR = "modernbert"
        self.OUT_DIR = f"../training_data/{DIR}/"  # Output directory for model checkpoints and performance metrics
        self.PERFORMANCE_FILE = f"../training_data/{DIR}/performance_metrics_{DIR}.json"
        self.YNACC_FILE_PATH = "../training_data/ynacc_processed.jsonl"
        self.IAC_FILE_PATH = "../training_data/iac_processed.jsonl"
        self.REDDIT_UNLABELED_FILE_PATH = "../training_data/reddit_train.jsonl"
        self.REDDIT_VAL_FILE_PATH = "../training_data/reddit_val.jsonl"
        self.REDDIT_TEST_FILE_PATH = "../training_data/reddit_test.jsonl"

        self.MAX_CURRICULUM_ITERATIONS = 15  # Max number of curriculum steps
        self.INITIAL_TRAINING_EPOCHS = (
            25  # Number of epochs for initial supervised training (iteration 0)
        )
        self.STUDENT_TEACHER_EPOCHS_PER_ITERATION = (
            10  # Number of epochs the student trains on each curriculum iteration (1+)
        )
        self.CONFIDENCE_THRESHOLD_START = (
            0.975  # Initial high confidence for pseudo-labeling (increased from 0.95)
        )
        self.CONFIDENCE_THRESHOLD_END = (
            0.60  # Final lower confidence threshold (increased from 0.60)
        )
        self.CONFIDENCE_DECAY_FACTOR = 0.025  # How much the confidence threshold decreases per iteration (slower decay)
        self.PSEUDO_LABELING_TEMPERATURE = (
            1.0  # Temperature scaling for pseudo-labeling to reduce overconfidence
        )
        self.UNLABELED_DATA_FRACTION_PER_STEP = 0.1  # Fraction of unlabeled data to consider for pseudo-labeling in each step, helps with large datasets

        # Checkpoint resumption configuration
        self.CONTINUE_FROM_CHECKPOINT = (
            True  # Set to True to continue training from a specific iteration
        )
        self.STARTING_ITERATION = (
            2  # The iteration to start from when continuing (0 = skip initial training)
        )

        # Dynamic threshold configuration
        self.MIN_PSEUDO_SAMPLES_REQUIRED = (
            1000  # Minimum number of pseudo-labeled samples required per iteration
        )
        if qwen:
            self.PSEUDO_LABEL_BATCH_SIZE = self.BATCH_SIZE * 2
        else:
            self.PSEUDO_LABEL_BATCH_SIZE = (
                self.BATCH_SIZE * 8
            )  # Larger batch size for pseudo-labeling for efficiency (keep low to avoid OOM issues on smaller GPUs or large models)

        self.SUPERVISED_TRAINING_ONLY = False  # Set to True to skip curriculum learning and only do supervised training

        self.USE_QLORA = True  # Set to True to enable QLoRA
        self.lora_rank = 16  # Rank for QLoRA, can be adjusted based on model capacity
        self.lora_alpha = 32  # Scaling factor for LoRA, typically 2x the rank
        self.bnb_bits = (
            4  # Bits for quantization, can be 4 or 8 depending on model and GPU support
        )

        # Testing configuration
        self.TESTING_MODE_ONLY = (
            False  # Set to True to skip training and only test existing checkpoints
        )

        # Corpus annotation configuration
        self.CORPUS_ANNOTATION_MODE = (
            False  # Set to True to skip training and only annotate a corpus
        )
        self.ANNOTATION_CORPUS_PATH = (
            "../training_data/reddit_train.jsonl"  # Path to corpus to annotate
        )
        self.ANNOTATION_OUTPUT_PATH = "../training_data/reddit_train_annotated.jsonl"  # Path to save annotated corpus

        # Annotation mode with specific starting iteration
        self.ANNOTATION_STARTING_ITERATION = 2  # Starting iteration for annotation mode (loads model from this iteration)

        # Safety configuration for annotation mode
        self.ANNOTATION_CHECKPOINT_FREQUENCY = (
            4  # Save checkpoint every N batches (1/N of total) during annotation
        )
        # E.g., 4 means save at 25%, 50%, 75%, 100%

        self.CHECKPOINT_FOLDERS = [
            "../training_data/best_qwen_qlora/",
            "../training_data/best_modernBERT_qlora/",
        ]  # Folder containing model checkpoints to test
        self.TEST_THRESHOLDS = [0.5]  # Thresholds to test for predictions

        # Instruction tuning configuration (for QWEN models in testing mode)
        self.USE_INSTRUCTION_TUNING = (
            False  # Set to True to use instruction tuning format with QWEN models
        )
        self.INSTRUCTION_MODEL_NAME = (
            "Qwen/Qwen3-0.6B"  # Updated to use the base model name as shown in example
        )
        self.INSTRUCTION_THINKING_MODE = (
            False  # Whether to use thinking mode for instruction tuned model
        )
        self.FEW_SHOT = False  # Set to True to add examples to the instruction prompt
        self.INSTRUCTION_SYSTEM_PROMPT = f"""
    Please evaluate the constructiveness of the following discussion by identifying whether the discussion is an ERIC or not.

    ERICs: Engaging, Respectful, and/or Informative Conversations. They are characterized by:
    - A respectful exchange of ideas, opinions, and/or information in response to a given topic(s).
    - Opinions expressed as an attempt to elicit a dialogue or persuade.
    - Comments that seek to contribute some new information or perspective on the relevant topic.

    Discussions always start with a post and are followed by a series of comments.
    There are tokens in between comments that link them to their author.
    Do not let the length of the discussion, the length of individual comments, or the number
    of authors influence your decision. Be as objective as possible.

    {"""Additionally, here is an example of a discussion that is not an ERIC:
    "[author0] Boyfriend pranks his girlfriend with fake \u00a350,000 scratch and win ticket her reaction to this is priceless [author1] Yeah I wouldn't be laughing.  That amount of money would completely change my life. This is cruel. [author2] Well good thing this video wasn\u2019t about you and was about people who can take a joke. [author3] Have some empathy ya thick cunt [author4] Lol suck a dick u fucking melon [author3] Biiiiiiiitchmaaaaaaaadddeee"
    And here are examples of a discussion that are ERICs:
    "[author0] Former bullies, when or how did you realize you were a bully, did you feel guilty for the things you'd done? [author1] It took a few years. Still feel awful about it. \nI don\u2019t know what it was, she was just so clingy and at one point i\u2019d had enough so I started being just a little mean. It just kinda got worse over time up until the point that i really hurt her.\nLuckily for her i changed schools a bit later. \nAfter a few years i went to apologize, but of course that doesn\u2019t really make what you did better. \nHer reaction was: \u201cyeah, what does that change for me?\u201d\nObviously she was right, but i did hear that the apology helped her get over it. \nIt\u2019s been years since I\u2019ve seen her, but i still think about it from time to time. Must\u2019ve been awful.\n\nAlso, this has impacted my life very much, so i can\u2019t even imagine how it must\u2019ve affected her. [author0] Wow, I'm glad to hear that you apologized and that it helped her move on. [author1] Her parents actually thanked me for doing it. Took a lot of courage i\u2019ll tell you that [author0] And humility."
    "[author0] LEGO: What do you think you're doing?!? [author1] I don't get it did he reveal bionicle reboot or smthn? [author2] Not really, he did announce something but was super vague, seems like a sort of passion project we wants to do with the community, he even said it might not even be bionicle. [author1] So is that image fan made or is it one of his passion projects [author2] Those pictures are real and on his insta, he did a stream talking about it I\u2019m sure you can find somewhere, search up Fabre bionicle stream 2020 or something. [author1] OK thanks"
""" if self.FEW_SHOT else """"""}
    Please directly output your final verdict by strictly following this
    format: '1' if the discussion is constructive, '0' if the discussion is not constructive.
        """

        # Regularization parameters
        self.classifier_dropout = 0.3  # Dropout probability for the classification head
        self.weight_decay = 0.01  # Weight decay for the classification head
        self.backbone_weight_decay = (
            0.001  # Weight decay for backbone parameters (lower than classifier)
        )

        # Focal Loss parameters
        self.focal_loss_alpha = "auto"  # Automatically determine from class weights
        self.focal_loss_gamma = 2.0  # Focusing parameter (typically 1.0-3.0)

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
