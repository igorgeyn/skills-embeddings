################################################################################
# Skills Embeddings Pipeline - Part 3: Occupation Profiles & Similarity Matrix
# Purpose: Build occupation skill profiles and compute pairwise similarity
#
# Input:
#   - skill_vectors_latest.csv (from Part 2)
#   - skill_metadata.csv (from Part 1)
#   - posting_occupations.csv (from Part 1)
#   - PDL transition data (from Snowflake)
#
# Output:
#   - occupation_skill_profiles.csv (SOC5 -> skill frequency vectors)
#   - occupation_vectors.csv (SOC5 -> embedding vectors)
#   - occupation_similarity_matrix.rds (SOC5 x SOC5 cosine similarity)
#   - validation_results.csv (embedding similarity vs. transition rates)
#
# Igor Geyn. December 2025.
################################################################################

library(tidyverse)
library(Matrix)
library(DBI)

################################################################################
# CONFIGURATION
################################################################################

data_dir <- "C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/data"
model_dir <- "C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/models"
output_dir <- "C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/output"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Minimum postings per occupation for reliable profile
min_postings_per_occupation <- 100

# Minimum transitions for validation
min_transitions_for_validation <- 50

message("\n", strrep("=", 70))
message("SKILLS EMBEDDINGS - OCCUPATION PROFILES & SIMILARITY")
message(strrep("=", 70))
message("Data directory: ", data_dir)
message("Model directory: ", model_dir)
message("Output directory: ", output_dir)
message(strrep("=", 70), "\n")


################################################################################
# LOAD DATA
################################################################################

message("Loading data...")

# Skill vectors from Word2Vec
skill_vectors <- read_csv(file.path(model_dir, "skill_vectors_latest.csv"), 
                          show_col_types = FALSE)
message("  Skill vectors: ", nrow(skill_vectors), " skills x ", 
        ncol(skill_vectors) - 1, " dimensions")

# Skill metadata
skill_metadata <- read_csv(file.path(data_dir, "skill_metadata.csv"),
                           show_col_types = FALSE)
message("  Skill metadata: ", nrow(skill_metadata), " skills")

# Posting-occupation mapping
posting_occupations <- read_csv(file.path(data_dir, "posting_occupations.csv"),
                                show_col_types = FALSE)
message("  Posting-occupation mappings: ", nrow(posting_occupations), " postings")


################################################################################
# STEP 1: BUILD OCCUPATION SKILL PROFILES
################################################################################

message("\nStep 1: Building occupation skill profiles...")

# We need to query skill-posting pairs again to link skills to occupations
# This is more efficient than storing the full join in Part 1

message("  Connecting to database for skill-occupation aggregation...")
con <- connectToBGI()

# Get skill frequencies by SOC5
occ_skill_freq <- dbGetQuery(con, "
  SELECT 
    p.SOC_2021_5 as soc5,
    p.SOC_2021_5_NAME as soc5_name,
    s.SKILL_ID as skill_id,
    s.SKILL_NAME as skill_name,
    COUNT(*) as frequency
  FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p
  INNER JOIN EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s ON p.ID = s.ID
  WHERE p.POSTED >= '2020-01-01' AND p.POSTED < '2025-01-01'
    AND p.SOC_2021_5 IS NOT NULL
  GROUP BY p.SOC_2021_5, p.SOC_2021_5_NAME, s.SKILL_ID, s.SKILL_NAME
") %>%
  rename_all(tolower)

message("  Retrieved ", format(nrow(occ_skill_freq), big.mark = ","), 
        " occupation-skill combinations")

# Get posting counts per occupation
occ_counts <- dbGetQuery(con, "
  SELECT 
    SOC_2021_5 as soc5,
    SOC_2021_5_NAME as soc5_name,
    SOC_2021_2 as soc2,
    SOC_2021_2_NAME as soc2_name,
    COUNT(*) as n_postings
  FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS
  WHERE POSTED >= '2020-01-01' AND POSTED < '2025-01-01'
    AND SOC_2021_5 IS NOT NULL
  GROUP BY SOC_2021_5, SOC_2021_5_NAME, SOC_2021_2, SOC_2021_2_NAME
") %>%
  rename_all(tolower)

message("  Unique SOC5 occupations: ", n_distinct(occ_counts$soc5))

# Filter to occupations with enough postings
valid_occupations <- occ_counts %>%
  filter(n_postings >= min_postings_per_occupation)

message("  Occupations with >= ", min_postings_per_occupation, " postings: ", 
        nrow(valid_occupations))

# Create skill token lookup
skill_token_lookup <- skill_metadata %>%
  select(skill_id, skill_token)

# Join and compute TF-IDF style weights
occ_skill_profiles <- occ_skill_freq %>%
  filter(soc5 %in% valid_occupations$soc5) %>%
  inner_join(skill_token_lookup, by = "skill_id") %>%
  group_by(soc5) %>%
  mutate(
    # Term frequency within occupation
    tf = frequency / sum(frequency),
    # Will compute IDF below
  ) %>%
  ungroup()

# Compute IDF (inverse document frequency across occupations)
skill_doc_freq <- occ_skill_profiles %>%
  group_by(skill_token) %>%
  summarise(
    n_occupations = n_distinct(soc5),
    .groups = "drop"
  )

n_total_occupations <- n_distinct(occ_skill_profiles$soc5)

occ_skill_profiles <- occ_skill_profiles %>%
  left_join(skill_doc_freq, by = "skill_token") %>%
  mutate(
    idf = log(n_total_occupations / n_occupations),
    tf_idf = tf * idf
  )

# Save profiles
write_csv(
  occ_skill_profiles %>%
    select(soc5, soc5_name, skill_id, skill_name, skill_token, frequency, tf, idf, tf_idf),
  file.path(output_dir, "occupation_skill_profiles.csv")
)

message("  Saved occupation skill profiles")


################################################################################
# STEP 2: COMPUTE OCCUPATION EMBEDDING VECTORS
################################################################################

message("\nStep 2: Computing occupation embedding vectors...")

# Get embedding dimensions
embedding_dims <- skill_vectors %>%
  select(starts_with("dim_")) %>%
  names()

n_dims <- length(embedding_dims)

# Convert skill vectors to matrix for fast lookup
skill_vector_matrix <- skill_vectors %>%
  select(skill_token, all_of(embedding_dims)) %>%
  column_to_rownames("skill_token") %>%
  as.matrix()

# Compute TF-IDF weighted average embedding for each occupation
compute_occupation_vector <- function(occ_data, skill_matrix) {
  # Get skills present in both occupation and embedding vocabulary
  valid_skills <- intersect(occ_data$skill_token, rownames(skill_matrix))
  
  if (length(valid_skills) < 5) {
    return(rep(NA_real_, ncol(skill_matrix)))
  }
  
  # Get weights and vectors
  weights <- occ_data %>%
    filter(skill_token %in% valid_skills) %>%
    pull(tf_idf)
  
  vectors <- skill_matrix[valid_skills, , drop = FALSE]
  
  # Weighted average
  weighted_vec <- colSums(vectors * weights) / sum(weights)
  
  return(weighted_vec)
}

message("  Computing vectors for ", n_distinct(occ_skill_profiles$soc5), " occupations...")

occupation_vectors <- occ_skill_profiles %>%
  group_by(soc5, soc5_name) %>%
  group_modify(~ {
    vec <- compute_occupation_vector(.x, skill_vector_matrix)
    tibble(!!!setNames(as.list(vec), embedding_dims))
  }) %>%
  ungroup()

# Remove occupations with NA vectors
occupation_vectors <- occupation_vectors %>%
  filter(!is.na(dim_0))

message("  Valid occupation vectors: ", nrow(occupation_vectors))

# Add metadata
occupation_vectors <- occupation_vectors %>%
  left_join(valid_occupations %>% select(soc5, soc2, soc2_name, n_postings), by = "soc5")

write_csv(occupation_vectors, file.path(output_dir, "occupation_vectors.csv"))
message("  Saved occupation vectors")


################################################################################
# STEP 3: COMPUTE SIMILARITY MATRIX
################################################################################

message("\nStep 3: Computing occupation similarity matrix...")

# Extract just the embedding columns as matrix
occ_matrix <- occupation_vectors %>%
  select(all_of(embedding_dims)) %>%
  as.matrix()

rownames(occ_matrix) <- occupation_vectors$soc5

# Normalize vectors (for cosine similarity)
occ_matrix_norm <- occ_matrix / sqrt(rowSums(occ_matrix^2))

# Compute cosine similarity matrix
similarity_matrix <- occ_matrix_norm %*% t(occ_matrix_norm)

message("  Similarity matrix: ", nrow(similarity_matrix), " x ", ncol(similarity_matrix))

# Save as RDS (efficient for R)
saveRDS(similarity_matrix, file.path(output_dir, "occupation_similarity_matrix.rds"))

# Also save top similarities for each occupation
top_similarities <- map_dfr(rownames(similarity_matrix), function(soc) {
  sims <- similarity_matrix[soc, ]
  sims <- sims[names(sims) != soc]  # Exclude self
  
  top_10 <- sort(sims, decreasing = TRUE)[1:10]
  
  tibble(
    soc5_from = soc,
    soc5_to = names(top_10),
    similarity = as.numeric(top_10),
    rank = 1:10
  )
})

# Add occupation names
top_similarities <- top_similarities %>%
  left_join(occupation_vectors %>% select(soc5, soc5_name), 
            by = c("soc5_from" = "soc5")) %>%
  rename(soc5_from_name = soc5_name) %>%
  left_join(occupation_vectors %>% select(soc5, soc5_name), 
            by = c("soc5_to" = "soc5")) %>%
  rename(soc5_to_name = soc5_name)

write_csv(top_similarities, file.path(output_dir, "top_similar_occupations.csv"))
message("  Saved top similar occupations")


################################################################################
# STEP 4: VALIDATE AGAINST PDL TRANSITIONS
################################################################################

message("\nStep 4: Validating against PDL career transitions...")

# Pull transition data
transition_matrix <- dbGetQuery(con, "
  WITH person_jobs AS (
    SELECT 
      PERSON_ID,
      BGI_SOC5 as soc5,
      BGI_START_DATE,
      ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY BGI_START_DATE) as job_seq
    FROM PDL_CLEAN.V5.EXPERIENCE
    WHERE BGI_START_DATE >= '2020-01-01'
      AND BGI_START_DATE < '2025-01-01'
      AND BGI_SOC5 IS NOT NULL
  ),
  transitions AS (
    SELECT 
      a.soc5 as soc5_from,
      b.soc5 as soc5_to,
      COUNT(*) as n_transitions
    FROM person_jobs a
    INNER JOIN person_jobs b 
      ON a.PERSON_ID = b.PERSON_ID 
      AND a.job_seq = b.job_seq - 1
    WHERE a.soc5 != b.soc5
    GROUP BY a.soc5, b.soc5
  )
  SELECT * FROM transitions
  WHERE n_transitions >= 10
") %>%
  rename_all(tolower)

message("  Transition pairs retrieved: ", format(nrow(transition_matrix), big.mark = ","))

dbDisconnect(con)

# Compute transition rates (normalize by source occupation size)
source_totals <- transition_matrix %>%
  group_by(soc5_from) %>%
  summarise(total_from = sum(n_transitions), .groups = "drop")

transition_matrix <- transition_matrix %>%
  left_join(source_totals, by = "soc5_from") %>%
  mutate(transition_rate = n_transitions / total_from)

# Join with similarity scores
validation_data <- transition_matrix %>%
  filter(
    soc5_from %in% rownames(similarity_matrix),
    soc5_to %in% colnames(similarity_matrix)
  ) %>%
  rowwise() %>%
  mutate(
    embedding_similarity = similarity_matrix[soc5_from, soc5_to]
  ) %>%
  ungroup() %>%
  filter(n_transitions >= min_transitions_for_validation)

message("  Validation pairs (>= ", min_transitions_for_validation, " transitions): ", 
        nrow(validation_data))

# Compute correlation
correlation <- cor(validation_data$embedding_similarity, 
                   validation_data$transition_rate,
                   method = "spearman")

message("\n  VALIDATION RESULT:")
message("  Spearman correlation (similarity vs. transition rate): ", round(correlation, 3))

# Quartile analysis
validation_data <- validation_data %>%
  mutate(
    similarity_quartile = ntile(embedding_similarity, 4)
  )

quartile_summary <- validation_data %>%
  group_by(similarity_quartile) %>%
  summarise(
    n_pairs = n(),
    mean_similarity = mean(embedding_similarity),
    mean_transition_rate = mean(transition_rate),
    median_transitions = median(n_transitions),
    .groups = "drop"
  )

message("\n  Transition rates by similarity quartile:")
for (i in 1:nrow(quartile_summary)) {
  message("    Q", quartile_summary$similarity_quartile[i], 
          " (sim=", round(quartile_summary$mean_similarity[i], 2), "): ",
          "transition rate = ", round(quartile_summary$mean_transition_rate[i] * 100, 2), "%")
}

# Ratio of Q4 to Q1
q4_rate <- quartile_summary$mean_transition_rate[quartile_summary$similarity_quartile == 4]
q1_rate <- quartile_summary$mean_transition_rate[quartile_summary$similarity_quartile == 1]
lift <- q4_rate / q1_rate

message("\n  Q4/Q1 lift: ", round(lift, 2), "x")

# Save validation results
write_csv(validation_data, file.path(output_dir, "validation_results.csv"))
write_csv(quartile_summary, file.path(output_dir, "validation_quartile_summary.csv"))


################################################################################
# STEP 5: SUMMARY STATISTICS
################################################################################

message("\n", strrep("=", 70))
message("PIPELINE COMPLETE")
message(strrep("=", 70))

message("\nOutput files:")
message("  ", file.path(output_dir, "occupation_skill_profiles.csv"))
message("  ", file.path(output_dir, "occupation_vectors.csv"))
message("  ", file.path(output_dir, "occupation_similarity_matrix.rds"))
message("  ", file.path(output_dir, "top_similar_occupations.csv"))
message("  ", file.path(output_dir, "validation_results.csv"))

message("\nKey statistics:")
message("  Occupations with embeddings: ", nrow(occupation_vectors))
message("  Skills in vocabulary: ", nrow(skill_vectors))
message("  Embedding dimensions: ", n_dims)
message("  Validation correlation: ", round(correlation, 3))
message("  Q4/Q1 transition lift: ", round(lift, 2), "x")

# Save summary
summary_stats <- list(
  n_occupations = nrow(occupation_vectors),
  n_skills = nrow(skill_vectors),
  n_dims = n_dims,
  validation_correlation = correlation,
  q4_q1_lift = lift,
  quartile_summary = quartile_summary
)

saveRDS(summary_stats, file.path(output_dir, "pipeline_summary.rds"))

message("\nInfrastructure ready for downstream analysis.\n")