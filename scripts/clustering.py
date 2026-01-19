import pandas as pd
import glob
import os
from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters

# 1. SETUP PATHS
# Adjust this path if your script is in 'scripts/' but data is in 'rt_results/'
input_path = "../rt_results/rt_*.csv" 
output_path = "../rt_results/country_features.csv"

print("Looking for files...")
all_files = glob.glob(input_path)
print(f"Found {len(all_files)} country files.")

# 2. LOAD AND COMBINE DATA
df_list = []

for filename in all_files:
    # Read the CSV
    df = pd.read_csv(filename)
    
    # Extract country name from filename (e.g., 'rt_Bangladesh.csv' -> 'Bangladesh')
    # This is a bit of string hacking: split by slash, take last part, remove 'rt_' and '.csv'
    base_name = os.path.basename(filename)
    country_name = base_name.replace("rt_", "").replace(".csv", "").replace("_", " ")
    
    # Add country column (this is our ID)
    df['Country'] = country_name
    
    # Keep only what we need
    if 'mean' in df.columns and 'date' in df.columns:
        df = df[['date', 'Country', 'mean']]
        df_list.append(df)

# Combine into one big dataframe
if not df_list:
    raise ValueError("No valid data found! Check your paths.")

full_df = pd.concat(df_list, ignore_index=True)

# Ensure date is interpreted as time
full_df['date'] = pd.to_datetime(full_df['date'])

print("Data loaded. Starting feature extraction (this takes a moment)...")

# 3. RUN TSFRESH
# We use 'EfficientFCParameters' to avoid calculating expensive/useless features
extracted_features = extract_features(
    full_df, 
    column_id="Country", 
    column_sort="date",
    default_fc_parameters=EfficientFCParameters()
)

# 4. CLEAN UP
# tsfresh creates NaN values if a feature can't be calculated. Fill them with 0.
extracted_features = extracted_features.dropna(axis=1)

print(f"Extracted {len(extracted_features.columns)} features for {len(extracted_features)} countries.")

# 5. SAVE
extracted_features.to_csv(output_path)
print(f"Saved to {output_path}")