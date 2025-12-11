library(tidyverse)
library(tidyr)
library(dplyr)
library(magrittr)
library(WDI)
library(lubridate)
library(EpiNow2)
library(zoo)
library(ggplot2)

prepped_deaths <- read_csv('/rt-estimates-covid/processed_deaths.csv')

death_delay <- LogNormal(mean = 22, sd = 12, max = 60) #this needs better empirical justification than what I'm providing now

gen_time <- Gamma( ##double check against Ferretti et al paper
  mean = 5.0,
  sd = 1.9,
  max = 20
)

process_deaths_by_country <- function(df, cutoff) {
  
  country_list <- unique(df$Country)
  dir.create("rt_results", showWarnings = FALSE)
  
  
  for(i in 1:cutoff) {
    current_country <- country_list[i]
    safe_name <- gsub(" ", "_", current_country)
    message(paste("Processing", i, "of", cutoff, ":", current_country))
    file_path <- file.path("rt_results", paste0("rt_", safe_name, ".csv"))
    if(file.exists(file_path)) {
      message("  -> Skipping (File exists)")
      next
    }
    
    df_by_country <- df %>%
      filter(Country == current_country) %>% 
      select(date, confirm)
    
    if(sum(df_by_country$confirm) < 50) {
      message("  -> Skipping (Insufficient data)")
      next 
    }
    
    tryCatch({
      estimates <- epinow(
        data = df_by_country,
        generation_time = generation_time_opts(gen_time),
        delays = delay_opts(death_delay),
        rt = rt_opts(prior = LogNormal(mean = 2, sd = 0.2)), ##not confident in prior SD - EpiNow vignette used .2 but seems tight
        stan = stan_opts(cores = 4),
        verbose = FALSE
      )
      
      summary_df <- estimates$estimates$summarised %>%
        mutate(Country = current_country)
      write_csv(summary_df, file_path)
      
    }, error = function(e) {
      message(paste("  -> FAILED on:", current_country))
      message(e)
    })
    
    # Clean RAM
    gc()
  }
  
  message("Batch processing complete.")
}


process_deaths_by_country(prepped_deaths, cutoff = 1) ##however many you want to run it for 