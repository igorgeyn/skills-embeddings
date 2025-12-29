# Skills Embeddings Pipeline

Build ML-based skill similarity from BGI job postings data.

## Overview

This pipeline trains Word2Vec embeddings on skill co-occurrence patterns from 200M+ job postings, then builds occupation-level skill profiles and a similarity matrix validated against actual career transitions.

## Pipeline Structure

```
Phase 1: Infrastructure (this pipeline)
├── Part 1: Data Extraction (R)      → skills_corpus.txt, skill_metadata.csv
├── Part 2: Train Embeddings (Python) → skill_vectors.csv, skill_embeddings.model  
└── Part 3: Occupation Profiles (R)   → occupation_similarity_matrix.rds

Phase 2: Applications (downstream)
├── A: AI Pivot Readiness Index
├── B: Escape Velocity Skills
└── C: Underutilized Career Pathways
```

## Requirements

### R packages
```r
install.packages(c("tidyverse", "DBI", "Matrix"))
```

### Python packages
```bash
pip install gensim pandas numpy
```

### Database
- Snowflake access via `connectToBGI()` function
- Tables: 
  - `EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS`
  - `EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS`
  - `PDL_CLEAN.V5.EXPERIENCE`

## Usage

### Step 1: Extract Data (R)
```r
source("embeddings_01_extract.R")
```

**Runtime:** ~30-60 minutes (depending on Snowflake load)

**Output:**
- `data/skills_corpus.txt` - One posting per line, skill tokens space-separated
- `data/skill_metadata.csv` - Skill vocabulary with frequencies
- `data/posting_occupations.csv` - Posting-to-SOC5 mapping
- `data/corpus_stats.rds` - Summary statistics

### Step 2: Train Embeddings (Python)
```bash
python embeddings_02_train.py
```

**Runtime:** ~15-30 minutes on 8 cores

**Output:**
- `models/skill_embeddings_latest.model` - Full gensim model
- `models/skill_vectors_latest.csv` - Vectors as CSV for R
- `models/training_log_*.txt` - Training log with validation spot-checks

### Step 3: Build Profiles & Similarity (R)
```r
source("embeddings_03_profiles.R")
```

**Runtime:** ~20-40 minutes

**Output:**
- `output/occupation_skill_profiles.csv` - SOC5 → skill TF-IDF profiles
- `output/occupation_vectors.csv` - SOC5 → embedding vectors
- `output/occupation_similarity_matrix.rds` - SOC5 × SOC5 cosine similarity
- `output/top_similar_occupations.csv` - Top 10 similar occupations per SOC5
- `output/validation_results.csv` - Embedding similarity vs. transition rates
- `output/pipeline_summary.rds` - Summary statistics

## Configuration

### Part 1 (Data Extraction)
| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_skill_frequency` | 100 | Exclude skills with fewer postings |
| `n_stopword_skills` | 25 | Exclude top N most frequent skills |
| `start_date` / `end_date` | 2020-2025 | Date range for corpus |

### Part 2 (Training)
| Parameter | Default | Description |
|-----------|---------|-------------|
| `EMBEDDING_DIM` | 200 | Vector dimensionality |
| `WINDOW_SIZE` | 100 | Context window (large = full posting) |
| `EPOCHS` | 10 | Training passes |
| `SG` | 1 | Algorithm: 1=Skip-gram, 0=CBOW |
| `NEGATIVE` | 10 | Negative samples |

### Part 3 (Profiles)
| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_postings_per_occupation` | 100 | Exclude occupations with fewer postings |
| `min_transitions_for_validation` | 50 | Minimum transitions for validation pairs |

## Validation

The pipeline validates embeddings against actual career transitions in PDL:

1. **Spearman correlation** between embedding similarity and transition rates
2. **Quartile analysis**: Do high-similarity occupation pairs have higher transition rates?
3. **Q4/Q1 lift**: How much more likely are transitions between top-quartile similar occupations?

**Target metrics:**
- Correlation > 0.2 (moderate positive relationship)
- Q4/Q1 lift > 2x (top-quartile similarity predicts 2x+ higher transitions)

## Using the Outputs

### Load similarity matrix in R
```r
sim_matrix <- readRDS("output/occupation_similarity_matrix.rds")

# Get similarity between two occupations
sim_matrix["15-1252", "15-1253"]  # Data Scientists vs. Software Developers

# Get most similar occupations to a target
sort(sim_matrix["15-1252", ], decreasing = TRUE)[1:10]
```

### Load occupation vectors in R
```r
occ_vectors <- read_csv("output/occupation_vectors.csv")

# Compute custom similarity
library(lsa)
cosine(
  as.numeric(occ_vectors[occ_vectors$soc5 == "15-1252", 4:203]),
  as.numeric(occ_vectors[occ_vectors$soc5 == "15-1253", 4:203])
)
```

### Load skill vectors in Python
```python
from gensim.models import Word2Vec

model = Word2Vec.load("models/skill_embeddings_latest.model")

# Most similar skills
model.wv.most_similar("python_(programming_language)", topn=10)

# Skill vector
model.wv["sql"]

# Similarity between skills
model.wv.similarity("python_(programming_language)", "r_(programming_language)")
```

## Directory Structure

```
skills_embeddings/
├── data/
│   ├── skills_corpus.txt
│   ├── skill_metadata.csv
│   ├── posting_occupations.csv
│   └── corpus_stats.rds
├── models/
│   ├── skill_embeddings_latest.model
│   ├── skill_vectors_latest.csv
│   └── training_log_*.txt
├── output/
│   ├── occupation_skill_profiles.csv
│   ├── occupation_vectors.csv
│   ├── occupation_similarity_matrix.rds
│   ├── top_similar_occupations.csv
│   ├── validation_results.csv
│   └── pipeline_summary.rds
├── embeddings_01_extract.R
├── embeddings_02_train.py
├── embeddings_03_profiles.R
└── README.md
```

## Known Limitations

1. **Posting-reality gap**: Skills in postings may not reflect actual job requirements
2. **Static embeddings**: Single vector per skill, doesn't capture polysemy
3. **Co-occurrence ambiguity**: Skills may co-occur for different reasons
4. **Long tail**: Rare skills have less reliable vectors

## Next Steps (Phase 2)

With the infrastructure in place, downstream analyses become straightforward:

- **AI Pivot Readiness**: Merge AI exposure scores, compute "adjacency to safety"
- **Escape Velocity Skills**: Regress skill presence on upward wage mobility
- **Underutilized Pathways**: Compare similarity matrix to transition matrix

---

Igor Geyn | Burning Glass Institute | December 2025
