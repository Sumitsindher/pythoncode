# pythoncode
import pandas as pd

def append_files_and_get_latest_records(file_paths):
    combined_df = pd.DataFrame()

    for file in file_paths:
        try:
            # Read each file and append to the combined DataFrame
            df = pd.read_csv(file)
            # Standardize column names
            df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error loading {file}: {e}")

    if combined_df.empty:
        raise ValueError("No valid files to process.")
    
    # Ensure required columns exist
    required_columns = {'claim_number', 'file_date', 'status'}
    if not required_columns.issubset(combined_df.columns):
        raise ValueError(f"The required columns {required_columns} are missing in the input files.")

    # Strip any leading/trailing spaces in file_date column
    combined_df['file_date'] = combined_df['file_date'].str.strip()

    # Parse file_date assuming format DD-MM-YYYY
    combined_df['file_date'] = pd.to_datetime(combined_df['file_date'], format='%d-%m-%Y', errors='coerce')

    # Remove rows with invalid file_date
    combined_df = combined_df[combined_df['file_date'].notna()]

    # Sort by claim_number and file_date to ensure proper grouping
    combined_df.sort_values(by=['claim_number', 'file_date'], inplace=True)

    result = []
    for claim_number, group in combined_df.groupby('claim_number'):
        # Get the last record for this claim_number
        last_record = group.iloc[-1]
        
        if last_record['status'] == 'Closed':
            # If the last record is Closed, keep only that record
            result.append(last_record)
        else:
            # If the last record is Open, keep the last Open and Closed records
            latest_open = group[group['status'] == 'Open'].iloc[-1]
            closed_records = group[group['status'] == 'Closed']
            
            if not closed_records.empty:
                latest_closed = closed_records.iloc[-1]
                result.extend([latest_open, latest_closed])
            else:
                # If no Closed record exists, keep only the latest Open record
                result.append(latest_open)

    # Convert result list to a DataFrame
    final_df = pd.DataFrame(result)

    # Sort the final DataFrame by claim_number for consistency
    final_df.sort_values(by=['claim_number'], inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    return final_df

# File paths
file_paths = [
    r"C:/Users/Smit/Documents/COR/Insurers/NIA/NIA Claims Input Jan_25.csv"
]

# Process files
latest_claim_records = append_files_and_get_latest_records(file_paths)

# Save output
output_path = r'C:\Users\Smit\Documents\COR\Smit\Model Claims Data\Output_Final\NIA_Smit_Jan25.csv'

# Attempt to save the file
try:
    latest_claim_records.to_csv(output_path, index=False)
    print(f"Latest records saved to: {output_path}")
except PermissionError:
    print(f"Permission denied for {output_path}. Trying a temporary filename...")
    temp_path = output_path.replace('.csv', '_temp.csv')
    latest_claim_records.to_csv(temp_path, index=False)
    print(f"File saved to temporary path: {temp_path}")
