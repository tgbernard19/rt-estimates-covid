library(tidyverse)
library(EpiNow2)
library(lubridate)
library(here) # Good practice for paths

# --- 1. SETUP & BIOLOGY ---

# Define the biological parameters (Must be done before function!)
gen_time <- Gamma(
  mean = 5.0,
  sd = 1.9,
  max = 20
)

death_delay <- LogNormal(mean = 22, sd = 12, max = 60)

# --- 2. LOAD & FILTER DATA ---

# A. Load the main dataset
# using here() to ensure path works from project root
all_deaths <- read_csv(here("data/processed_deaths.csv"))

# B. Load your Top 20 list
# Assuming this CSV has a column named "Country"
top_20_list <- read_csv(here("data/top_20_by_deaths.csv"))

# C. Filter the main dataset
# We only keep rows where the Country is in your Top 20 list
# inner_join is a safe way to do this (it drops anything not in the list)
target_data <- all_deaths %>%
  inner_join(top_20_list %>% select(Country), by = "Country") 

# --- 3. THE RUNNER FUNCTION ---

process_deaths_by_country <- function(df, cutoff) {
  
  # Get the list of unique countries from the ALREADY FILTERED dataframe
  country_list <- unique(df$Country)
  
  # Safety: Don't exceed the number of countries we actually have
  loop_limit <- min(length(country_list), cutoff)
  
  dir.create("rt_results", showWarnings = FALSE)
  
  for(i in 1:loop_limit) {
    
    current_country <- country_list[i]
    safe_name <- gsub(" ", "_", current_country)
    
    message(paste("Processing", i, "of", loop_limit, ":", current_country))
    
    file_path <- file.path("rt_results", paste0("rt_", safe_name, ".csv"))
    
    # 1. Skip if already done
    if(file.exists(file_path)) {
      message("   -> Skipping (File exists)")
      next
    }
    
    # 2. Filter for single country
    df_by_country <- df %>%
      filter(Country == current_country) %>% 
      select(date, confirm)
    
    # 3. Skip if insufficient data
    if(sum(df_by_country$confirm) < 50) {
      message("   -> Skipping (Insufficient data)")
      next 
    }
    
    # 4. Run Model
    tryCatch({
      estimates <- epinow(
        
        data = df_by_country,
        
        generation_time = generation_time_opts(gen_time),
        
        delays = delay_opts(death_delay),
        
        rt = rt_opts(prior = LogNormal(mean = 2, sd = 0.2)),
        
        stan = stan_opts(cores = 4, chains = 4)
        
      )
      
      summary_df <- estimates$estimates$summarised %>%
        mutate(Country = current_country)
      
      write_csv(summary_df, file_path)
      
    }, error = function(e) {
      message(paste("   -> FAILED on:", current_country))
      message(e)
    })
    
    # Clean RAM immediately
    gc()
  }
  
  message("Batch processing complete.")
}

# --- 4. EXECUTE ---

# Run on the top 20 list (controlled by cutoff)
# Set cutoff = 20 to run all of them, or cutoff = 1 to test.
process_deaths_by_country(target_data, cutoff = 20)