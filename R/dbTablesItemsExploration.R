if(!('pacman' %in% installed.packages()[,"Package"])) install.packages("pacman")
library(pacman)
p_load(tidyverse, bigrquery)
load(file = 'data/sepsis_detection.rda')
load('queries.R')

project_id <- "datathon-tarragona-2018"
options(httr_oauth_cache = FALSE)

# Wrapper for running BigQuery queries.
run_query <- function(query){
  data <- query_exec(query, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)
  return(data)
}

sepsis_patients <- sepsis_detection %>% 
  filter(angus == 1)

# Take data from subject_id and hadm_id
take_data_filtered <- function(sepsis_patients, table_name, int = FALSE) {
  if (int) {
    subject_ids <- sepsis_patients$subject_id %>% 
      unique() %>% paste0(collapse = ", ")
    query = paste0("SELECT * FROM `physionet-data.mimiciii_clinical.",
                   table_name,
                   "` WHERE subject_id IN (",
                   subject_ids, ")")
  } else {
    subject_ids <- sepsis_patients$subject_id %>% 
      unique() %>% paste0(collapse = "', '")
    query = paste0("SELECT * FROM `physionet-data.mimiciii_clinical.",
                   table_name,
                   "` WHERE subject_id IN ('",
                   subject_ids, "')")
  }
  return(query)
}

take_n_items <- function(sepsis_patients, table_name, int = FALSE) {
  if (int) {
    subject_ids <- sepsis_patients$subject_id %>% 
      unique() %>% paste0(collapse = ", ")
    query = paste0("SELECT count(subject_id) FROM `physionet-data.mimiciii_clinical.",
                   table_name,
                   "` WHERE subject_id IN (",
                   subject_ids, ")")
  } else {
    subject_ids <- sepsis_patients$subject_id %>% 
      unique() %>% paste0(collapse = "', '")
    query = paste0("SELECT count(subject_id) FROM `physionet-data.mimiciii_clinical.",
                   table_name,
                   "` WHERE subject_id IN ('",
                   subject_ids, "')")
  }
  return(query)
}

dictionaries <- c('d_cpt', 'd_icd_diagnoses', 'd_icd_procedures',
                  'd_items', 'd_labitems')

for (dictionary in dictionaries) {
  assign(dictionary, run_query(paste0("SELECT * FROM `physionet-data.mimiciii_clinical.", dictionary, "`")))
  save(list = dictionary,
       file = paste0('data/', dictionary, '.rda'))
}

tables <- c('admissions', 'chartevents', 'cptevents', 
            'datetimeevents', 'diagnoses_icd', 'icustays', 
            'inputevents_cv', 'inputevents_mv', 'labevents',
            'microbiologyevents', 'outputevents', 'patients', 
            'prescriptions', 'procedureevents_mv', 'procedures_icd', 
            'transfers')

n_items <- NULL
for (table_name in tables) {
  n_items <- c(n_items, run_query(take_n_items(sepsis_patients, table_name, TRUE))[1])
}

trial <- take_data_filtered(sepsis_patients, table_name, TRUE)
trial2 <- run_query(trial)
