################################################################################
# Skills Embeddings Pipeline - Part 1: Data Extraction
# Purpose: Extract skill-posting pairs from Snowflake for Word2Vec training
#
# Output: 
#   - skills_corpus.txt (one posting per line, skills space-separated)
#   - skill_metadata.csv (skill_id, skill_name, frequency, type, category)
#   - posting_occupations.csv (posting_id, soc5, soc5_name, soc2, soc2_name)
#
# Igor Geyn. December 2025.
################################################################################

library(tidyverse)
library(DBI)

################################################################################
# CONFIGURATION
################################################################################

output_dir <- "C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/data"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Date range
start_date <- "2020-01-01"
end_date <- "2025-01-01"

# Minimum skill frequency (skills below this are excluded from training)
min_skill_frequency <- 100

# Stop-word skills to exclude (top N most frequent)
n_stopword_skills <- 25

# Batch size for extraction (to manage memory)
batch_size <- 5000000

message("\n", strrep("=", 70))
message("SKILLS EMBEDDINGS - DATA EXTRACTION")
message(strrep("=", 70))
message("Output directory: ", output_dir)
message("Date range: ", start_date, " to ", end_date)
message("Min skill frequency: ", min_skill_frequency)
message("Excluding top ", n_stopword_skills, " stop-word skills")
message(strrep("=", 70), "\n")


################################################################################
# DATABASE CONNECTION
################################################################################

message("Connecting to database...")
con <- connectToBGI()
message("Connected.\n")


################################################################################
# STEP 1: GET SKILL VOCABULARY AND IDENTIFY STOP-WORDS
################################################################################

message("Step 1: Building skill vocabulary...")

skill_vocab <- dbGetQuery(con, sprintf("
  SELECT 
    SKILL_ID,
    SKILL_NAME,
    SKILL_TYPE,
    SKILL_CATEGORY_NAME,
    COUNT(*) as frequency
  FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s
  INNER JOIN EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p ON s.ID = p.ID
  WHERE p.POSTED >= '%s' AND p.POSTED < '%s'
  GROUP BY SKILL_ID, SKILL_NAME, SKILL_TYPE, SKILL_CATEGORY_NAME
  ORDER BY frequency DESC
", start_date, end_date)) %>%
  rename_all(tolower)

message("  Total skills in corpus: ", format(nrow(skill_vocab), big.mark = ","))
message("  Total mentions: ", format(sum(skill_vocab$frequency), big.mark = ","))

# Identify stop-words (top N most frequent)
stopword_skills <- skill_vocab %>%
  head(n_stopword_skills) %>%
  pull(skill_id)

message("\n  Stop-word skills (excluded):")
for (i in 1:min(10, n_stopword_skills)) {
  message("    ", i, ". ", skill_vocab$skill_name[i], " (", 
          format(skill_vocab$frequency[i], big.mark = ","), ")")
}
if (n_stopword_skills > 10) {
  message("    ... and ", n_stopword_skills - 10, " more")
}

# Filter vocabulary
skill_vocab_filtered <- skill_vocab %>%
  filter(
    frequency >= min_skill_frequency,
    !skill_id %in% stopword_skills
  ) %>%
  mutate(
    # Create clean token for Word2Vec (replace spaces with underscores)
    skill_token = str_replace_all(skill_name, " ", "_") %>%
      str_replace_all("[^a-zA-Z0-9_]", "") %>%
      str_to_lower()
  )

message("\n  After filtering:")
message("    Skills with freq >= ", min_skill_frequency, ": ", 
        format(nrow(skill_vocab_filtered), big.mark = ","))
message("    Coverage: ", round(100 * sum(skill_vocab_filtered$frequency) / sum(skill_vocab$frequency), 1), 
        "% of mentions")

# Save vocabulary
write_csv(skill_vocab_filtered, file.path(output_dir, "skill_metadata.csv"))

# Create skill_id -> token lookup
skill_lookup <- skill_vocab_filtered %>%
  select(skill_id, skill_token)

skill_ids_quoted <- paste0("'", skill_vocab_filtered$skill_id, "'", collapse = ", ")


################################################################################
# STEP 2: EXTRACT POSTING-OCCUPATION MAPPING
################################################################################

message("\nStep 2: Extracting posting-occupation mapping...")

posting_occupations <- dbGetQuery(con, sprintf("
  SELECT DISTINCT
    p.ID as posting_id,
    p.SOC_2021_5 as soc5,
    p.SOC_2021_5_NAME as soc5_name,
    p.SOC_2021_2 as soc2,
    p.SOC_2021_2_NAME as soc2_name,
    p.NAICS2,
    p.NAICS2_NAME
  FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p
  WHERE p.POSTED >= '%s' AND p.POSTED < '%s'
    AND p.SOC_2021_5 IS NOT NULL
", start_date, end_date)) %>%
  rename_all(tolower)

message("  Postings with occupation: ", format(nrow(posting_occupations), big.mark = ","))
message("  Unique SOC5 codes: ", n_distinct(posting_occupations$soc5))

write_csv(posting_occupations, file.path(output_dir, "posting_occupations.csv"))


################################################################################
# STEP 3: EXTRACT SKILL-POSTING PAIRS AND BUILD CORPUS
################################################################################

message("\nStep 3: Extracting skill-posting pairs...")

# Get total count first
total_pairs <- dbGetQuery(con, sprintf("
  SELECT COUNT(*) as n
  FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s
  INNER JOIN EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p ON s.ID = p.ID
  WHERE p.POSTED >= '%s' AND p.POSTED < '%s'
    AND s.SKILL_ID IN (%s)
", start_date, end_date, skill_ids_quoted))$n

message("  Total pairs to extract: ", format(total_pairs, big.mark = ","))

# Extract in batches and build corpus file
corpus_file <- file.path(output_dir, "skills_corpus.txt")
if (file.exists(corpus_file)) file.remove(corpus_file)

# Get all posting IDs with their skills
message("  Extracting posting skill lists...")

start_time <- Sys.time()

posting_skills <- dbGetQuery(con, sprintf("
  SELECT 
    s.ID as posting_id,
    s.SKILL_ID as skill_id
  FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s
  INNER JOIN EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p ON s.ID = p.ID
  WHERE p.POSTED >= '%s' AND p.POSTED < '%s'
    AND s.SKILL_ID IN (%s)
  ORDER BY s.ID
", start_date, end_date, skill_ids_quoted)) %>%
  rename_all(tolower)

elapsed <- round(difftime(Sys.time(), start_time, units = "mins"), 1)
message("  Extracted ", format(nrow(posting_skills), big.mark = ","), " pairs in ", elapsed, " minutes")

# Join with skill tokens
message("  Mapping skill IDs to tokens...")
posting_skills <- posting_skills %>%
  inner_join(skill_lookup, by = "skill_id")

# Group by posting and create corpus lines
message("  Building corpus file...")

corpus <- posting_skills %>%
  group_by(posting_id) %>%
  summarise(
    skill_line = paste(skill_token, collapse = " "),
    n_skills = n(),
    .groups = "drop"
  ) %>%
  filter(n_skills >= 2)  # Need at least 2 skills for co-occurrence

message("  Postings with 2+ skills: ", format(nrow(corpus), big.mark = ","))

# Write corpus file
writeLines(corpus$skill_line, corpus_file)

message("  Corpus file written: ", corpus_file)


################################################################################
# STEP 4: SUMMARY STATISTICS
################################################################################

message("\n", strrep("=", 70))
message("EXTRACTION COMPLETE")
message(strrep("=", 70))

corpus_stats <- list(
  n_postings = nrow(corpus),
  n_skills = nrow(skill_vocab_filtered),
  n_pairs = nrow(posting_skills),
  mean_skills_per_posting = round(mean(corpus$n_skills), 1),
  median_skills_per_posting = median(corpus$n_skills),
  date_range = paste(start_date, "to", end_date),
  min_frequency = min_skill_frequency,
  n_stopwords_removed = n_stopword_skills
)

message("\nCorpus Statistics:")
message("  Postings: ", format(corpus_stats$n_postings, big.mark = ","))
message("  Unique skills: ", format(corpus_stats$n_skills, big.mark = ","))
message("  Skill-posting pairs: ", format(corpus_stats$n_pairs, big.mark = ","))
message("  Mean skills/posting: ", corpus_stats$mean_skills_per_posting)
message("  Median skills/posting: ", corpus_stats$median_skills_per_posting)

saveRDS(corpus_stats, file.path(output_dir, "corpus_stats.rds"))

message("\nOutput files:")
message("  ", file.path(output_dir, "skills_corpus.txt"))
message("  ", file.path(output_dir, "skill_metadata.csv"))
message("  ", file.path(output_dir, "posting_occupations.csv"))
message("  ", file.path(output_dir, "corpus_stats.rds"))


################################################################################
# CLEANUP
################################################################################

dbDisconnect(con)
rm(posting_skills, corpus)
gc()

message("\nReady for Word2Vec training (Part 2).\n")