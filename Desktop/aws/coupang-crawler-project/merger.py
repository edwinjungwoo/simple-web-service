import pandas as pd
import os
import glob

def merge_validated_files(base_dir):
    """
    Merges 'source_urls.xlsx' with appended data from validated files in the RAW folder.

    Args:
        base_dir (str): The base directory containing the files and RAW folder.
    """
    url_info_path = os.path.join(base_dir, 'source_urls.xlsx')
    raw_folder_path = os.path.join(base_dir, 'RAW')
    output_path = os.path.join(base_dir, 'coupang_merged.xlsx')

    # --- 1. Read source_urls.xlsx --- 
    try:
        print(f"Reading {url_info_path}...")
        url_info_df = pd.read_excel(url_info_path)
        url_info_df = url_info_df[['PROD_ID', 'CPH_LEVEL_1_NAME', 'CPH_LEVEL_2_NAME', 'CPH_LEVEL_3_NAME', 'CPH_LEVEL_4_NAME']]

        print(f"Successfully read {url_info_path}. Shape: {url_info_df.shape}")
        if 'PROD_ID' not in url_info_df.columns:
             print(f"Error: 'PROD_ID' column not found in {url_info_path}")
             return
    except FileNotFoundError:
        print(f"Error: File not found at {url_info_path}")
        return
    except Exception as e:
        print(f"Error reading {url_info_path}: {e}")
        return

    # --- 2. Find and Read Validated Files in RAW folder --- 
    validated_files = glob.glob(os.path.join(raw_folder_path, '*_validated.xlsx'))

    if not validated_files:
        print(f"No '*_validated.xlsx' files found in {raw_folder_path}.")
        return

    print(f"Found validated files: {validated_files}")
    all_validated_data = []
    for file_path in validated_files:
        try:
            print(f"Reading {file_path}...")
            df = pd.read_excel(file_path)
            print(f"Successfully read {file_path}. Shape: {df.shape}")
            if 'PROD_ID' not in df.columns:
                print(f"Warning: 'PROD_ID' column not found in {file_path}. Skipping this file for merge.")
                continue 
            all_validated_data.append(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if not all_validated_data:
        print("No valid data could be read from the validated files.")
        return

    # --- 3. Append Validated Data --- 
    print("Appending data from validated files...")
    validated_df = pd.concat(all_validated_data, ignore_index=True)
    print(f"Appended data shape: {validated_df.shape}")

    # Ensure PROD_ID is suitable for merging (e.g., same data type)
    try:
      url_info_df['PROD_ID'] = url_info_df['PROD_ID'].astype(str)
      validated_df['PROD_ID'] = validated_df['PROD_ID'].astype(str)
    except KeyError:
        print("Error: 'PROD_ID' column missing in one of the dataframes after processing.")
        return
    except Exception as e:
        print(f"Error converting PROD_ID to string: {e}")
        # Decide how to handle this - maybe try merging anyway or stop

    # --- 4. Merge DataFrames --- 
    print(f"Merging url_info_df (shape: {url_info_df.shape}) and validated_df (shape: {validated_df.shape}) on 'PROD_ID'...")
    try:
        # Perform the merge, handling potential duplicate columns from validated_df
        # Keep columns from url_info_df, and add non-overlapping columns from validated_df
        merged_df = pd.merge(url_info_df, validated_df, on='PROD_ID', how='inner', suffixes=('', '_validated')).drop(columns = ['URL', 'ORIGINAL_INDEX'])
        merged_df['EXTRACTION_TIME'] = pd.to_datetime(merged_df['EXTRACTION_TIME'], errors='coerce')
        merged_df['DATE'] = merged_df['DATE'].dt.date
        merged_df['DISCOUNT_RATE'] = 1 - (merged_df['PRICE'] / merged_df['ORIGIN_PRICE'])
        merged_df = merged_df[['DATE', 'PROD_ID', 'CPH_LEVEL_1_NAME', 'CPH_LEVEL_2_NAME', 'CPH_LEVEL_3_NAME', 'CPH_LEVEL_4_NAME', 'COUPON', 'COUPON_PRICE', 'AC_PRICE', 'PRICE', 'ORIGIN_PRICE', 'DISCOUNT_RATE']].sort_values(by='DATE', ignore_index=True, ascending=False)
        
        # Optional: Select or rename columns if needed after merge
        # Example: drop duplicate columns introduced by merge if suffixes were not used or handled properly
        # merged_df = merged_df.loc[:,~merged_df.columns.duplicated()] 

        print(f"Merge successful. Merged data shape: {merged_df.shape}")
    except KeyError:
        print("Error: 'PROD_ID' column issue during merge. Check column names and presence.")
        return
    except Exception as e:
        print(f"Error during merge: {e}")
        return
        
    # --- 5. Save Merged Data --- 
    try:
        print(f"Saving merged data to {output_path}...")
        merged_df.to_excel(output_path, index=False)
        print(f"Successfully saved merged data to {output_path}")
    except Exception as e:
        print(f"Error saving merged file: {e}")

if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__)) # Gets the directory where the script is located
    # Or, if you always run it from the coupang_crawler directory:
    # current_directory = r'c:\Users\admin\Desktop\aws\coupang_crawler'
    merge_validated_files(current_directory)
