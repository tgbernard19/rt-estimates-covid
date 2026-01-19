import pandas as pd
import glob
import os
from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters

def main():
    # --- 1. SETUP PATHS ---
    input_path = "/Users/tgbernard19/Desktop/rt-estimates-covid/rt_results/rt_*.csv"
    output_path = "/Users/tgbernard19/Desktop/rt-estimates-covid/rt_results/country_features.csv"

    print(f"Looking for files in: {input_path}")
    all_files = glob.glob(input_path)
    print(f"Found {len(all_files)} country files.")

    # --- 2. LOAD AND COMBINE DATA ---
    df_list = []

    for filename in all_files:
        try:
            df = pd.read_csv(filename)
            
            # Extract country name
            base_name = os.path.basename(filename)
            country_name = base_name.replace("rt_", "").replace(".csv", "").replace("_", " ")
            df['Country'] = country_name
            
            if 'mean' in df.columns and 'date' in df.columns:
                df = df[['date', 'Country', 'mean']]
                df_list.append(df)
                
        except Exception as e:
            print(f"Skipping {filename}: {e}")

    if not df_list:
        raise ValueError("No valid data found! Double check the folder path.")

    full_df = pd.concat(df_list, ignore_index=True)

    # --- CLEANING ---
    full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce')
    full_df = full_df.dropna(subset=['date', 'Country', 'mean'])

    print("Data loaded and cleaned. Starting feature extraction...")

    # --- 3. RUN TSFRESH (Safe Mode) ---
    try:
        extracted_features = extract_features(
            full_df, 
            column_id="Country", 
            column_sort="date",
            default_fc_parameters=EfficientFCParameters(),
            n_jobs=1  # <--- CRITICAL FIX: Runs on 1 core to avoid macOS crashes
        )

        # --- 4. CLEAN & SAVE ---
        extracted_features = extracted_features.dropna(axis=1, how='all')
        extracted_features = extracted_features.fillna(0)
        
        extracted_features.to_csv(output_path)
        print(f"Success! Saved features to: {output_path}")

    except Exception as e:
        print(f"Feature extraction failed: {e}")

# This is the magic line that fixes the "Bootstrap" error
if __name__ == '__main__':
    main()