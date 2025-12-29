"""
Skills Embeddings Pipeline - Part 2: Train Word2Vec
Purpose: Train skill embeddings using gensim Word2Vec on extracted corpus

Input: 
    - skills_corpus.txt (one posting per line, skills space-separated)
    - skill_metadata.csv (for validation)

Output:
    - skill_embeddings.model (full gensim model)
    - skill_vectors.csv (skill_token, dim_0, dim_1, ..., dim_N)
    - training_log.txt

Requirements:
    pip install gensim pandas numpy

Igor Geyn. December 2025.
"""

import os
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from gensim.models.callbacks import CallbackAny2Vec

################################################################################
# CONFIGURATION
################################################################################

DATA_DIR = Path("C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/data")
OUTPUT_DIR = Path("C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/models")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Word2Vec hyperparameters
EMBEDDING_DIM = 200      # Dimensionality of embeddings
WINDOW_SIZE = 100        # Large window = all skills in posting treated as context
MIN_COUNT = 5            # Minimum frequency (already filtered in extraction, but safety)
WORKERS = 8              # Parallel workers
EPOCHS = 10              # Training epochs
SG = 1                   # 1 = Skip-gram, 0 = CBOW
NEGATIVE = 10            # Negative samples
SEED = 42                # Reproducibility

# Logging setup
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = OUTPUT_DIR / f"training_log_{timestamp}.txt"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


################################################################################
# TRAINING CALLBACK
################################################################################

class EpochLogger(CallbackAny2Vec):
    """Callback to log training progress."""
    
    def __init__(self):
        self.epoch = 0
        self.start_time = None
    
    def on_epoch_begin(self, model):
        self.start_time = datetime.now()
        logger.info(f"Epoch {self.epoch + 1}/{EPOCHS} starting...")
    
    def on_epoch_end(self, model):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"Epoch {self.epoch + 1}/{EPOCHS} complete in {elapsed:.1f}s")
        self.epoch += 1


################################################################################
# CORPUS ITERATOR
################################################################################

class SkillCorpus:
    """Memory-efficient iterator over skill corpus."""
    
    def __init__(self, filepath):
        self.filepath = filepath
    
    def __iter__(self):
        with open(self.filepath, 'r', encoding='utf-8') as f:
            for line in f:
                # Each line is a space-separated list of skill tokens
                skills = line.strip().split()
                if len(skills) >= 2:
                    yield skills


################################################################################
# MAIN TRAINING PIPELINE
################################################################################

def main():
    logger.info("=" * 70)
    logger.info("SKILLS EMBEDDINGS - WORD2VEC TRAINING")
    logger.info("=" * 70)
    
    corpus_file = DATA_DIR / "skills_corpus.txt"
    metadata_file = DATA_DIR / "skill_metadata.csv"
    
    # Verify input files exist
    if not corpus_file.exists():
        raise FileNotFoundError(f"Corpus file not found: {corpus_file}")
    if not metadata_file.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}")
    
    # Log configuration
    logger.info(f"\nConfiguration:")
    logger.info(f"  Embedding dimensions: {EMBEDDING_DIM}")
    logger.info(f"  Window size: {WINDOW_SIZE}")
    logger.info(f"  Min count: {MIN_COUNT}")
    logger.info(f"  Epochs: {EPOCHS}")
    logger.info(f"  Algorithm: {'Skip-gram' if SG else 'CBOW'}")
    logger.info(f"  Negative samples: {NEGATIVE}")
    logger.info(f"  Workers: {WORKERS}")
    
    # Count corpus size
    logger.info(f"\nCounting corpus size...")
    n_postings = sum(1 for _ in open(corpus_file, 'r', encoding='utf-8'))
    logger.info(f"  Postings in corpus: {n_postings:,}")
    
    # Load corpus iterator
    logger.info(f"\nInitializing corpus iterator...")
    corpus = SkillCorpus(corpus_file)
    
    # Build vocabulary first (one pass through data)
    logger.info(f"\nBuilding vocabulary...")
    model = Word2Vec(
        vector_size=EMBEDDING_DIM,
        window=WINDOW_SIZE,
        min_count=MIN_COUNT,
        workers=WORKERS,
        sg=SG,
        negative=NEGATIVE,
        seed=SEED
    )
    
    model.build_vocab(corpus, progress_per=1000000)
    logger.info(f"  Vocabulary size: {len(model.wv):,} skills")
    
    # Train model
    logger.info(f"\nTraining model...")
    epoch_logger = EpochLogger()
    
    model.train(
        corpus,
        total_examples=model.corpus_count,
        epochs=EPOCHS,
        callbacks=[epoch_logger]
    )
    
    logger.info(f"\nTraining complete!")
    
    # Save full model
    model_path = OUTPUT_DIR / f"skill_embeddings_{timestamp}.model"
    model.save(str(model_path))
    logger.info(f"\nModel saved: {model_path}")
    
    # Also save as latest
    latest_path = OUTPUT_DIR / "skill_embeddings_latest.model"
    model.save(str(latest_path))
    
    # Export vectors to CSV for R compatibility
    logger.info(f"\nExporting vectors to CSV...")
    
    vocab = list(model.wv.index_to_key)
    vectors = np.array([model.wv[word] for word in vocab])
    
    # Create dataframe
    col_names = ['skill_token'] + [f'dim_{i}' for i in range(EMBEDDING_DIM)]
    df_vectors = pd.DataFrame(
        np.column_stack([vocab, vectors]),
        columns=col_names
    )
    
    # Convert dimension columns to float
    for col in col_names[1:]:
        df_vectors[col] = df_vectors[col].astype(float)
    
    vectors_path = OUTPUT_DIR / f"skill_vectors_{timestamp}.csv"
    df_vectors.to_csv(vectors_path, index=False)
    logger.info(f"Vectors saved: {vectors_path}")
    
    # Also save as latest
    latest_vectors_path = OUTPUT_DIR / "skill_vectors_latest.csv"
    df_vectors.to_csv(latest_vectors_path, index=False)
    
    # Validation: spot-check nearest neighbors
    logger.info(f"\n" + "=" * 70)
    logger.info("VALIDATION: NEAREST NEIGHBORS")
    logger.info("=" * 70)
    
    test_skills = [
        'python_(programming_language)',
        'javascript_(programming_language)',
        'microsoft_excel',
        'nursing',
        'accounting',
        'project_management',
        'machine_learning',
        'sql',
        'customer_service',
        'data_analysis'
    ]
    
    for skill in test_skills:
        if skill in model.wv:
            neighbors = model.wv.most_similar(skill, topn=5)
            logger.info(f"\n  {skill}:")
            for neighbor, score in neighbors:
                logger.info(f"    {neighbor}: {score:.3f}")
        else:
            # Try to find partial match
            matches = [s for s in vocab if skill.replace('_', '') in s.replace('_', '')]
            if matches:
                logger.info(f"\n  {skill} not found. Similar tokens: {matches[:3]}")
            else:
                logger.info(f"\n  {skill} not found in vocabulary")
    
    # Summary statistics
    logger.info(f"\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    logger.info(f"  Vocabulary size: {len(model.wv):,}")
    logger.info(f"  Embedding dimensions: {EMBEDDING_DIM}")
    logger.info(f"  Training epochs: {EPOCHS}")
    logger.info(f"  Model file: {model_path}")
    logger.info(f"  Vectors CSV: {vectors_path}")
    logger.info(f"  Log file: {log_file}")
    
    logger.info(f"\nReady for occupation profiling (Part 3).\n")
    
    return model


if __name__ == "__main__":
    model = main()