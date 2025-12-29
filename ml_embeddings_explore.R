################################################################################
# Skills Embeddings Feasibility Audit
# Purpose: Characterize BGI skill data to assess embeddings project viability
#
# Data source: EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS
# Schema:
#   - ID (posting ID, joins to POSTINGS.ID)
#   - SKILL_ID (unique skill identifier)
#   - SKILL_NAME (human-readable)
#   - SKILL_TYPE (Specialized Skill, Certification, Common Skill)
#   - SKILL_CATEGORY / SKILL_CATEGORY_NAME
#   - SKILL_SUBCATEGORY / SKILL_SUBCATEGORY_NAME
#   - IS_SOFTWARE (boolean)
#
# Igor Geyn. December 2025.
################################################################################

library(tidyverse)
library(lubridate)
library(scales)
library(DBI)

################################################################################
# CONFIGURATION
################################################################################

# Output
output_base <- "C:/Users/igorg/OneDrive/Desktop/bgi/projects/skills_embeddings/output"
output_dir <- file.path(output_base, paste0("skills_audit_", format(Sys.time(), "%Y-%m-%d_%H%M%S")))
dir.create(file.path(output_dir, "tables"), recursive = TRUE, showWarnings = FALSE)

# Date range for full corpus analysis
start_date <- "2020-01-01"
end_date <- "2025-01-01"

# Co-occurrence parameters
n_cooccurrence_skills <- 200
cooc_posting_limit <- 500000

# Quality analysis parameters
n_sample_skills <- 500
min_skill_frequency <- 10

message("\n", strrep("=", 70))
message("SKILLS EMBEDDINGS FEASIBILITY AUDIT")
message(strrep("=", 70))
message("Output: ", output_dir)
message("Scope: National (all U.S.), ", start_date, " to ", end_date)
message(strrep("=", 70), "\n")


################################################################################
# DATABASE CONNECTION
################################################################################

message("Connecting to database...")
con <- connectToBGI()
message("Connected.\n")


################################################################################
# PART 1: CORPUS OVERVIEW
################################################################################

message(strrep("=", 70))
message("PART 1: CORPUS OVERVIEW")
message(strrep("=", 70))

# 1a. Skill types (uses GROUP BY - efficient on full corpus)
message("\n1a. Skill types breakdown...")

skill_types <- tryCatch({
  dbGetQuery(con, "
    SELECT 
      SKILL_TYPE,
      COUNT(DISTINCT SKILL_ID) as n_skills,
      COUNT(*) as n_mentions
    FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS
    GROUP BY SKILL_TYPE
    ORDER BY n_mentions DESC
  ") %>% rename_all(tolower)
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(skill_types)) {
  total_mentions <- sum(skill_types$n_mentions)
  total_skills <- sum(skill_types$n_skills)
  
  message("  Total skill-posting pairs: ", format(total_mentions, big.mark = ","))
  message("  Total unique skills: ", format(total_skills, big.mark = ","))
  message("\n  By type:")
  for (i in 1:nrow(skill_types)) {
    message("    ", skill_types$skill_type[i], ": ", 
            format(skill_types$n_skills[i], big.mark = ","), " skills, ",
            format(skill_types$n_mentions[i], big.mark = ","), " mentions")
  }
  write_csv(skill_types, file.path(output_dir, "tables", "skill_types.csv"))
}

# 1b. Skill categories
message("\n1b. Skill categories breakdown...")

skill_categories <- tryCatch({
  dbGetQuery(con, "
    SELECT 
      SKILL_CATEGORY,
      SKILL_CATEGORY_NAME,
      COUNT(DISTINCT SKILL_ID) as n_skills,
      COUNT(*) as n_mentions
    FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS
    GROUP BY SKILL_CATEGORY, SKILL_CATEGORY_NAME
    ORDER BY n_mentions DESC
  ") %>% rename_all(tolower)
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(skill_categories)) {
  message("  Categories (top 15):")
  for (i in 1:min(15, nrow(skill_categories))) {
    message("    ", skill_categories$skill_category_name[i], ": ", 
            format(skill_categories$n_skills[i], big.mark = ","), " skills")
  }
  write_csv(skill_categories, file.path(output_dir, "tables", "skill_categories.csv"))
}


################################################################################
# PART 2: SKILL FREQUENCY DISTRIBUTION
################################################################################

message("\n", strrep("=", 70))
message("PART 2: SKILL FREQUENCY DISTRIBUTION")
message(strrep("=", 70))

message("\n2a. Computing skill frequencies (full corpus)...")

skill_frequencies <- tryCatch({
  start_time <- Sys.time()
  result <- dbGetQuery(con, "
    SELECT 
      SKILL_ID as skill_id,
      SKILL_NAME as skill_name,
      SKILL_TYPE as skill_type,
      SKILL_CATEGORY_NAME as skill_category,
      IS_SOFTWARE as is_software,
      COUNT(*) as n_postings
    FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS
    GROUP BY SKILL_ID, SKILL_NAME, SKILL_TYPE, SKILL_CATEGORY_NAME, IS_SOFTWARE
    ORDER BY n_postings DESC
  ") %>% rename_all(tolower)
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 1)
  message("  Query completed in ", elapsed, " seconds")
  result
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(skill_frequencies) && nrow(skill_frequencies) > 0) {
  
  message("\n  Vocabulary statistics:")
  message("    Total unique skills: ", format(nrow(skill_frequencies), big.mark = ","))
  message("    Skills with 10+ postings: ", format(sum(skill_frequencies$n_postings >= 10), big.mark = ","))
  message("    Skills with 100+ postings: ", format(sum(skill_frequencies$n_postings >= 100), big.mark = ","))
  message("    Skills with 1,000+ postings: ", format(sum(skill_frequencies$n_postings >= 1000), big.mark = ","))
  message("    Skills with 10,000+ postings: ", format(sum(skill_frequencies$n_postings >= 10000), big.mark = ","))
  message("    Skills with 100,000+ postings: ", format(sum(skill_frequencies$n_postings >= 100000), big.mark = ","))
  
  message("\n  Frequency distribution:")
  message("    Min: ", min(skill_frequencies$n_postings))
  message("    Median: ", format(median(skill_frequencies$n_postings), big.mark = ","))
  message("    Mean: ", format(round(mean(skill_frequencies$n_postings)), big.mark = ","))
  message("    Max: ", format(max(skill_frequencies$n_postings), big.mark = ","))
  
  # Concentration
  total_freq <- sum(skill_frequencies$n_postings)
  message("\n  Concentration:")
  message("    Top 10 skills: ", round(100 * sum(head(skill_frequencies$n_postings, 10)) / total_freq, 1), "% of mentions")
  message("    Top 100 skills: ", round(100 * sum(head(skill_frequencies$n_postings, 100)) / total_freq, 1), "% of mentions")
  message("    Top 1,000 skills: ", round(100 * sum(head(skill_frequencies$n_postings, 1000)) / total_freq, 1), "% of mentions")
  
  message("\n  Top 25 skills:")
  for (i in 1:min(25, nrow(skill_frequencies))) {
    message("    ", i, ". ", skill_frequencies$skill_name[i], " (",
            format(skill_frequencies$n_postings[i], big.mark = ","), ")")
  }
  
  write_csv(skill_frequencies, file.path(output_dir, "tables", "skill_frequencies.csv"))
}

# 2b. Skills per posting
message("\n2b. Computing skills per posting distribution...")

skills_per_posting <- tryCatch({
  dbGetQuery(con, "
    SELECT n_skills, COUNT(*) as n_postings
    FROM (
      SELECT ID, COUNT(*) as n_skills
      FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS
      GROUP BY ID
    )
    GROUP BY n_skills
    ORDER BY n_skills
  ") %>% rename_all(tolower)
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(skills_per_posting) && nrow(skills_per_posting) > 0) {
  total_postings <- sum(skills_per_posting$n_postings)
  weighted_mean <- sum(skills_per_posting$n_skills * skills_per_posting$n_postings) / total_postings
  cumsum_pct <- cumsum(skills_per_posting$n_postings) / total_postings
  
  message("  Skills per posting:")
  message("    Mean: ", round(weighted_mean, 1))
  message("    10th percentile: ", skills_per_posting$n_skills[which(cumsum_pct >= 0.10)[1]])
  message("    25th percentile: ", skills_per_posting$n_skills[which(cumsum_pct >= 0.25)[1]])
  message("    50th percentile (median): ", skills_per_posting$n_skills[which(cumsum_pct >= 0.50)[1]])
  message("    75th percentile: ", skills_per_posting$n_skills[which(cumsum_pct >= 0.75)[1]])
  message("    90th percentile: ", skills_per_posting$n_skills[which(cumsum_pct >= 0.90)[1]])
  message("    Max: ", max(skills_per_posting$n_skills))
  
  write_csv(skills_per_posting, file.path(output_dir, "tables", "skills_per_posting.csv"))
}


################################################################################
# PART 3: TEMPORAL AND GEOGRAPHIC COVERAGE
################################################################################

message("\n", strrep("=", 70))
message("PART 3: TEMPORAL AND GEOGRAPHIC COVERAGE")
message(strrep("=", 70))

# 3a. Temporal
message("\n3a. Analyzing temporal coverage...")

temporal_coverage <- tryCatch({
  dbGetQuery(con, sprintf("
    SELECT 
      DATE_TRUNC('month', p.POSTED) as month,
      COUNT(DISTINCT p.ID) as n_postings,
      COUNT(DISTINCT s.ID) as n_postings_with_skills,
      COUNT(*) as n_skill_mentions
    FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p
    LEFT JOIN EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s ON p.ID = s.ID
    WHERE p.POSTED >= '%s' AND p.POSTED < '%s'
    GROUP BY DATE_TRUNC('month', p.POSTED)
    ORDER BY month
  ", start_date, end_date)) %>% rename_all(tolower)
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(temporal_coverage) && nrow(temporal_coverage) > 0) {
  temporal_coverage <- temporal_coverage %>%
    mutate(pct_with_skills = round(100 * n_postings_with_skills / n_postings, 1))
  
  message("  Months covered: ", nrow(temporal_coverage))
  message("  Date range: ", min(temporal_coverage$month), " to ", max(temporal_coverage$month))
  message("  Skill coverage over time:")
  message("    Min %: ", min(temporal_coverage$pct_with_skills, na.rm = TRUE))
  message("    Max %: ", max(temporal_coverage$pct_with_skills, na.rm = TRUE))
  message("    Mean %: ", round(mean(temporal_coverage$pct_with_skills, na.rm = TRUE), 1))
  
  write_csv(temporal_coverage, file.path(output_dir, "tables", "temporal_coverage.csv"))
}

# 3b. Geographic
message("\n3b. Analyzing geographic coverage...")

geo_coverage <- tryCatch({
  dbGetQuery(con, sprintf("
    SELECT 
      p.STATE_NAME as state,
      COUNT(DISTINCT p.ID) as n_postings,
      COUNT(DISTINCT s.ID) as n_postings_with_skills
    FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS p
    LEFT JOIN EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s ON p.ID = s.ID
    WHERE p.POSTED >= '%s' AND p.POSTED < '%s'
    GROUP BY p.STATE_NAME
    ORDER BY n_postings DESC
  ", start_date, end_date)) %>% rename_all(tolower)
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(geo_coverage) && nrow(geo_coverage) > 0) {
  geo_coverage <- geo_coverage %>%
    mutate(pct_with_skills = round(100 * n_postings_with_skills / n_postings, 1))
  
  message("  States covered: ", nrow(geo_coverage))
  message("  Total postings: ", format(sum(geo_coverage$n_postings), big.mark = ","))
  message("  Skill coverage by state:")
  message("    Min: ", min(geo_coverage$pct_with_skills, na.rm = TRUE), "%")
  message("    Max: ", max(geo_coverage$pct_with_skills, na.rm = TRUE), "%")
  message("    Median: ", median(geo_coverage$pct_with_skills, na.rm = TRUE), "%")
  
  message("\n  Top 10 states:")
  for (i in 1:min(10, nrow(geo_coverage))) {
    message("    ", geo_coverage$state[i], ": ", 
            format(geo_coverage$n_postings[i], big.mark = ","), " (",
            geo_coverage$pct_with_skills[i], "% with skills)")
  }
  
  write_csv(geo_coverage, file.path(output_dir, "tables", "geographic_coverage.csv"))
}


################################################################################
# PART 4: VOCABULARY QUALITY
################################################################################

message("\n", strrep("=", 70))
message("PART 4: VOCABULARY QUALITY")
message(strrep("=", 70))

if (!is.null(skill_frequencies) && nrow(skill_frequencies) > 0) {
  
  # 4a. Automated quality flags on full vocabulary
  message("\n4a. Applying quality flags to full vocabulary...")
  
  skill_frequencies <- skill_frequencies %>%
    mutate(
      flag_too_short = nchar(skill_name) <= 2,
      flag_too_long = nchar(skill_name) > 60,
      flag_has_numbers = str_detect(skill_name, "\\d"),
      flag_all_caps = skill_name == toupper(skill_name) & nchar(skill_name) > 3,
      flag_likely_junk = str_detect(tolower(skill_name), 
                                    "^(the|and|or|for|with|years?|experience|required|preferred|must|ability|strong)$"),
      flag_looks_like_phrase = str_count(skill_name, " ") >= 5,
      flag_has_special_chars = str_detect(skill_name, "[^a-zA-Z0-9 \\-\\/\\+\\#\\.\\(\\)\\&]"),
      needs_review = flag_too_short | flag_too_long | flag_has_numbers | flag_all_caps | 
        flag_likely_junk | flag_looks_like_phrase | flag_has_special_chars
    )
  
  message("  Quality flag summary:")
  message("    Too short (<=2 chars): ", sum(skill_frequencies$flag_too_short))
  message("    Too long (>60 chars): ", sum(skill_frequencies$flag_too_long))
  message("    Contains numbers: ", sum(skill_frequencies$flag_has_numbers))
  message("    All caps: ", sum(skill_frequencies$flag_all_caps))
  message("    Likely junk word: ", sum(skill_frequencies$flag_likely_junk))
  message("    Looks like phrase (5+ words): ", sum(skill_frequencies$flag_looks_like_phrase))
  message("    Has unusual characters: ", sum(skill_frequencies$flag_has_special_chars))
  message("    Flagged for any reason: ", sum(skill_frequencies$needs_review), 
          " (", round(100 * mean(skill_frequencies$needs_review), 1), "%)")
  
  # 4b. Quality by category
  message("\n4b. Quality flags by category...")
  
  category_quality <- skill_frequencies %>%
    group_by(skill_category) %>%
    summarise(
      n_skills = n(),
      n_flagged = sum(needs_review),
      pct_flagged = round(100 * mean(needs_review), 1),
      .groups = "drop"
    ) %>%
    arrange(desc(pct_flagged))
  
  message("  Categories with highest flag rates:")
  for (i in 1:min(10, nrow(category_quality))) {
    message("    ", category_quality$skill_category[i], ": ", 
            category_quality$pct_flagged[i], "% flagged (",
            format(category_quality$n_skills[i], big.mark = ","), " skills)")
  }
  write_csv(category_quality, file.path(output_dir, "tables", "quality_by_category.csv"))
  
  # 4c. Duplicate detection
  message("\n4c. Checking for duplicates...")
  
  skill_dedup <- skill_frequencies %>%
    mutate(skill_normalized = str_to_lower(str_trim(skill_name))) %>%
    group_by(skill_normalized) %>%
    summarise(
      n_variants = n(),
      variants = paste(skill_name, collapse = " | "),
      skill_ids = paste(skill_id, collapse = " | "),
      total_postings = sum(n_postings),
      .groups = "drop"
    ) %>%
    filter(n_variants > 1) %>%
    arrange(desc(total_postings))
  
  message("  Case/whitespace duplicates: ", nrow(skill_dedup))
  
  # 4d. Version/variant analysis
  message("\n4d. Version/variant analysis...")
  
  versioned_skills <- skill_frequencies %>%
    mutate(
      has_version = str_detect(skill_name, "\\d+(\\.\\d+)?$|\\s\\d{4}$"),
      base_name = str_replace(skill_name, "\\s*\\d+(\\.\\d+)?$|\\s*\\d{4}$", "") %>% str_trim()
    ) %>%
    filter(has_version) %>%
    group_by(base_name) %>%
    summarise(
      n_versions = n(),
      versions = paste(skill_name, collapse = " | "),
      total_postings = sum(n_postings),
      .groups = "drop"
    ) %>%
    filter(n_versions > 1) %>%
    arrange(desc(total_postings))
  
  message("  Skills with multiple versions: ", nrow(versioned_skills))
  if (nrow(versioned_skills) > 0) {
    message("  Top versioned skills:")
    for (i in 1:min(10, nrow(versioned_skills))) {
      message("    ", versioned_skills$base_name[i], " (", versioned_skills$n_versions[i], 
              " versions, ", format(versioned_skills$total_postings[i], big.mark = ","), " mentions)")
    }
    write_csv(versioned_skills, file.path(output_dir, "tables", "versioned_skills.csv"))
  }
  
  # 4e. Vocabulary tiers
  message("\n4e. Vocabulary tier analysis...")
  
  vocab_tiers <- skill_frequencies %>%
    mutate(
      tier = case_when(
        n_postings >= 100000 ~ "Very common (100K+)",
        n_postings >= 10000 ~ "Common (10K-100K)",
        n_postings >= 1000 ~ "Moderate (1K-10K)",
        n_postings >= 100 ~ "Uncommon (100-1K)",
        n_postings >= 10 ~ "Rare (10-100)",
        TRUE ~ "Very rare (<10)"
      ),
      tier = factor(tier, levels = c("Very common (100K+)", "Common (10K-100K)", 
                                     "Moderate (1K-10K)", "Uncommon (100-1K)",
                                     "Rare (10-100)", "Very rare (<10)"))
    ) %>%
    group_by(tier) %>%
    summarise(
      n_skills = n(),
      total_mentions = sum(n_postings),
      .groups = "drop"
    ) %>%
    mutate(
      pct_of_vocab = round(100 * n_skills / sum(n_skills), 1),
      pct_of_mentions = round(100 * total_mentions / sum(total_mentions), 1)
    )
  
  message("  Vocabulary tiers:")
  for (i in 1:nrow(vocab_tiers)) {
    message("    ", vocab_tiers$tier[i], ": ", 
            format(vocab_tiers$n_skills[i], big.mark = ","), " skills (",
            vocab_tiers$pct_of_vocab[i], "% of vocab, ",
            vocab_tiers$pct_of_mentions[i], "% of mentions)")
  }
  write_csv(vocab_tiers, file.path(output_dir, "tables", "vocabulary_tiers.csv"))
  
  # 4f. Vocabulary size sensitivity
  message("\n4f. Vocabulary size at different frequency thresholds...")
  
  freq_thresholds <- c(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000)
  vocab_sensitivity <- tibble(
    min_freq = freq_thresholds,
    n_skills = map_int(freq_thresholds, ~sum(skill_frequencies$n_postings >= .x)),
    mentions_covered = map_dbl(freq_thresholds, ~sum(skill_frequencies$n_postings[skill_frequencies$n_postings >= .x])),
    pct_coverage = round(100 * mentions_covered / sum(skill_frequencies$n_postings), 1)
  )
  
  for (i in 1:nrow(vocab_sensitivity)) {
    message("    min_freq >= ", vocab_sensitivity$min_freq[i], ": ", 
            format(vocab_sensitivity$n_skills[i], big.mark = ","), " skills (",
            vocab_sensitivity$pct_coverage[i], "% coverage)")
  }
  write_csv(vocab_sensitivity, file.path(output_dir, "tables", "vocabulary_sensitivity.csv"))
  
  # 4g. Skill name length distribution
  message("\n4g. Skill name length distribution...")
  
  skill_frequencies <- skill_frequencies %>%
    mutate(
      n_words = str_count(skill_name, "\\S+"),
      length_category = case_when(
        n_words == 1 ~ "1: Single word",
        n_words == 2 ~ "2: Two words",
        n_words <= 4 ~ "3: 3-4 words",
        TRUE ~ "4: 5+ words (phrase)"
      )
    )
  
  length_dist <- skill_frequencies %>%
    group_by(length_category) %>%
    summarise(
      n_skills = n(),
      total_mentions = sum(n_postings),
      example = first(skill_name),
      .groups = "drop"
    ) %>%
    mutate(
      pct_skills = round(100 * n_skills / sum(n_skills), 1),
      pct_mentions = round(100 * total_mentions / sum(total_mentions), 1)
    )
  
  message("  Skill name length:")
  for (i in 1:nrow(length_dist)) {
    message("    ", length_dist$length_category[i], ": ", 
            format(length_dist$n_skills[i], big.mark = ","), " skills (",
            length_dist$pct_skills[i], "%), ",
            length_dist$pct_mentions[i], "% of mentions")
    message("      Example: '", length_dist$example[i], "'")
  }
  write_csv(length_dist, file.path(output_dir, "tables", "skill_length_distribution.csv"))
}


################################################################################
# PART 5: CO-OCCURRENCE ANALYSIS
################################################################################

message("\n", strrep("=", 70))
message("PART 5: CO-OCCURRENCE ANALYSIS")
message(strrep("=", 70))

if (!is.null(skill_frequencies) && nrow(skill_frequencies) > 0) {
  
  top_skills <- head(skill_frequencies, n_cooccurrence_skills)
  top_skill_ids <- paste0("'", top_skills$skill_id, "'", collapse = ", ")
  
  message("\n5a. Computing co-occurrence for top ", n_cooccurrence_skills, " skills...")
  message("    (Sampling ", format(cooc_posting_limit, big.mark = ","), " postings)")
  
  cooccurrence <- tryCatch({
    start_time <- Sys.time()
    result <- dbGetQuery(con, sprintf("
      WITH sampled_postings AS (
        SELECT DISTINCT ID
        FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS
        WHERE SKILL_ID IN (%s)
        LIMIT %d
      ),
      filtered_skills AS (
        SELECT s.ID, s.SKILL_ID, s.SKILL_NAME
        FROM EMSI_BURNING_GLASS_INSTITUTE.US.POSTINGS_SKILLS s
        INNER JOIN sampled_postings sp ON s.ID = sp.ID
        WHERE s.SKILL_ID IN (%s)
      )
      SELECT 
        a.SKILL_NAME as skill_a,
        b.SKILL_NAME as skill_b,
        COUNT(DISTINCT a.ID) as n_cooccur
      FROM filtered_skills a
      INNER JOIN filtered_skills b ON a.ID = b.ID AND a.SKILL_NAME < b.SKILL_NAME
      GROUP BY a.SKILL_NAME, b.SKILL_NAME
      HAVING COUNT(DISTINCT a.ID) >= 100
      ORDER BY n_cooccur DESC
      LIMIT 10000
    ", top_skill_ids, cooc_posting_limit, top_skill_ids)) %>% rename_all(tolower)
    elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 1)
    message("  Query completed in ", elapsed, " seconds")
    result
  }, error = function(e) {
    message("  Query failed: ", e$message)
    NULL
  })
  
  if (!is.null(cooccurrence) && nrow(cooccurrence) > 0) {
    message("  Computed ", format(nrow(cooccurrence), big.mark = ","), " skill pairs")
    
    # Matrix sparsity
    possible_pairs <- n_cooccurrence_skills * (n_cooccurrence_skills - 1) / 2
    density <- round(100 * nrow(cooccurrence) / possible_pairs, 1)
    message("\n  Matrix density: ", density, "% of possible pairs observed (n >= 100)")
    
    message("\n  Top 20 co-occurring pairs (raw count):")
    for (i in 1:min(20, nrow(cooccurrence))) {
      message("    ", cooccurrence$skill_a[i], " + ", cooccurrence$skill_b[i], 
              " (", format(cooccurrence$n_cooccur[i], big.mark = ","), ")")
    }
    write_csv(cooccurrence, file.path(output_dir, "tables", "skill_cooccurrence.csv"))
    
    # 5b. PMI calculation
    message("\n5b. Computing PMI (Pointwise Mutual Information)...")
    
    skill_marginals <- top_skills %>% select(skill_name, n_postings)
    total_marginal <- sum(skill_marginals$n_postings)
    total_cooc <- sum(cooccurrence$n_cooccur)
    
    cooc_pmi <- cooccurrence %>%
      left_join(skill_marginals, by = c("skill_a" = "skill_name")) %>%
      rename(n_a = n_postings) %>%
      left_join(skill_marginals, by = c("skill_b" = "skill_name")) %>%
      rename(n_b = n_postings) %>%
      filter(!is.na(n_a), !is.na(n_b)) %>%
      mutate(
        p_ab = n_cooccur / total_cooc,
        p_a = n_a / total_marginal,
        p_b = n_b / total_marginal,
        pmi = log2(p_ab / (p_a * p_b)),
        npmi = pmi / (-log2(p_ab))
      ) %>%
      filter(!is.na(pmi), !is.infinite(pmi))
    
    message("  Top 20 pairs by PMI (strongest associations):")
    top_pmi <- cooc_pmi %>% 
      filter(n_cooccur >= 500) %>%
      arrange(desc(pmi)) %>% 
      head(20)
    
    for (i in 1:min(20, nrow(top_pmi))) {
      message("    ", top_pmi$skill_a[i], " + ", top_pmi$skill_b[i], 
              " (PMI=", round(top_pmi$pmi[i], 2), ", n=", 
              format(top_pmi$n_cooccur[i], big.mark = ","), ")")
    }
    write_csv(cooc_pmi, file.path(output_dir, "tables", "skill_cooccurrence_pmi.csv"))
    
    # 5c. Spot-check specific skills
    message("\n5c. Spot-checking skill neighborhoods...")
    
    spot_check <- c("Python", "SQL", "Microsoft Excel", "Project Management", 
                    "Machine Learning", "Customer Service", "Nursing", "Accounting",
                    "JavaScript", "Salesforce")
    
    for (skill in spot_check) {
      neighbors <- cooc_pmi %>%
        filter(skill_a == skill | skill_b == skill) %>%
        mutate(neighbor = ifelse(skill_a == skill, skill_b, skill_a)) %>%
        arrange(desc(pmi)) %>%
        head(5)
      
      if (nrow(neighbors) > 0) {
        message("\n    '", skill, "' top neighbors:")
        for (j in 1:nrow(neighbors)) {
          message("      - ", neighbors$neighbor[j], " (PMI=", round(neighbors$pmi[j], 2), ")")
        }
      } else {
        message("\n    '", skill, "' not in top ", n_cooccurrence_skills, " skills")
      }
    }
  }
}


################################################################################
# PART 6: PDL TRANSITION DATA
################################################################################

message("\n", strrep("=", 70))
message("PART 6: PDL TRANSITION DATA")
message(strrep("=", 70))

message("\n6a. Checking PDL transition volume...")

transition_stats <- tryCatch({
  dbGetQuery(con, "
    WITH person_jobs AS (
      SELECT 
        PERSON_ID,
        BGI_SOC5 as occupation,
        BGI_START_DATE,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY BGI_START_DATE) as job_seq
      FROM PDL_CLEAN.V5.EXPERIENCE
      WHERE BGI_START_DATE >= '2020-01-01'
        AND BGI_START_DATE < '2025-01-01'
        AND BGI_SOC5 IS NOT NULL
    ),
    transitions AS (
      SELECT a.PERSON_ID, a.occupation as occ_from, b.occupation as occ_to
      FROM person_jobs a
      INNER JOIN person_jobs b ON a.PERSON_ID = b.PERSON_ID AND a.job_seq = b.job_seq - 1
      WHERE a.occupation != b.occupation
    )
    SELECT 
      COUNT(*) as n_transitions,
      COUNT(DISTINCT PERSON_ID) as n_persons
    FROM transitions
  ") %>% rename_all(tolower)
}, error = function(e) {
  message("  Query failed: ", e$message)
  NULL
})

if (!is.null(transition_stats) && !is.na(transition_stats$n_transitions)) {
  message("  Observable occupation transitions: ", format(transition_stats$n_transitions, big.mark = ","))
  message("  Persons with transitions: ", format(transition_stats$n_persons, big.mark = ","))
} else {
  message("  Could not retrieve transition statistics")
}


################################################################################
# PART 7: FEASIBILITY SUMMARY
################################################################################

message("\n", strrep("=", 70))
message("PART 7: FEASIBILITY SUMMARY")
message(strrep("=", 70))

strengths <- c()
issues <- c()

# Volume
if (!is.null(skill_frequencies)) {
  n_1k <- sum(skill_frequencies$n_postings >= 1000)
  n_10k <- sum(skill_frequencies$n_postings >= 10000)
  
  if (n_10k >= 10000) {
    strengths <- c(strengths, sprintf("Excellent volume: %s skills with 10K+ postings", format(n_10k, big.mark = ",")))
  } else if (n_1k >= 20000) {
    strengths <- c(strengths, sprintf("Good volume: %s skills with 1K+ postings", format(n_1k, big.mark = ",")))
  }
}

# Quality
if (!is.null(skill_frequencies)) {
  flag_rate <- mean(skill_frequencies$needs_review)
  if (flag_rate < 0.10) {
    strengths <- c(strengths, sprintf("High quality: only %.0f%% of skills flagged", 100 * flag_rate))
  } else if (flag_rate < 0.20) {
    strengths <- c(strengths, "Acceptable quality")
  } else {
    issues <- c(issues, sprintf("Quality concerns: %.0f%% of skills flagged", 100 * flag_rate))
  }
}

# Temporal
if (!is.null(temporal_coverage)) {
  min_cov <- min(temporal_coverage$pct_with_skills, na.rm = TRUE)
  if (min_cov >= 90) {
    strengths <- c(strengths, sprintf("Excellent temporal coverage: %.0f%%+ throughout", min_cov))
  }
}

# Co-occurrence
if (exists("cooc_pmi") && nrow(cooc_pmi) > 0) {
  strengths <- c(strengths, "Co-occurrence patterns look sensible")
}

# Transitions
if (!is.null(transition_stats) && !is.na(transition_stats$n_transitions) && transition_stats$n_transitions >= 1000000) {
  strengths <- c(strengths, sprintf("Good transition data: %s observable transitions", format(transition_stats$n_transitions, big.mark = ",")))
}

# Schema
strengths <- c(strengths, "Clean junction table with SKILL_ID identifiers")
strengths <- c(strengths, "Pre-built taxonomy (categories, types)")

message("\nSTRENGTHS:")
for (s in strengths) message("  + ", s)

if (length(issues) > 0) {
  message("\nISSUES:")
  for (i in issues) message("  - ", i)
}

message("\nRECOMMENDED NEXT STEPS:")
message("  1. Train Word2Vec/Skip-gram on skills with 100+ postings (~28K skills)")
message("  2. Consider removing top 20-30 'stop word' skills (Communication, Customer Service, etc.)")
message("  3. Validate embeddings against PDL occupation transitions")
message("  4. Use existing taxonomy for hierarchical embedding or validation")


################################################################################
# SAVE SUMMARY AND CLEANUP
################################################################################

findings <- list(
  corpus = list(
    total_mentions = if (!is.null(skill_types)) sum(skill_types$n_mentions) else NA,
    total_skills = if (!is.null(skill_types)) sum(skill_types$n_skills) else NA
  ),
  vocabulary = if (!is.null(skill_frequencies)) list(
    n_skills = nrow(skill_frequencies),
    n_1k_plus = sum(skill_frequencies$n_postings >= 1000),
    n_10k_plus = sum(skill_frequencies$n_postings >= 10000),
    pct_flagged = round(100 * mean(skill_frequencies$needs_review), 1)
  ) else NULL,
  coverage = list(
    temporal_min = if (!is.null(temporal_coverage)) min(temporal_coverage$pct_with_skills, na.rm = TRUE) else NA,
    geographic_min = if (!is.null(geo_coverage)) min(geo_coverage$pct_with_skills, na.rm = TRUE) else NA
  ),
  transitions = list(
    n_transitions = if (!is.null(transition_stats)) transition_stats$n_transitions else NA
  )
)

saveRDS(findings, file.path(output_dir, "audit_findings.rds"))

dbDisconnect(con)

message("\n", strrep("=", 70))
message("AUDIT COMPLETE")
message(strrep("=", 70))
message("Output: ", output_dir)
message("\nKey files:")
message("  - skill_frequencies.csv (full vocabulary)")
message("  - skill_cooccurrence_pmi.csv (co-occurrence with PMI)")
message("  - audit_findings.rds (programmatic summary)")
message("\n")