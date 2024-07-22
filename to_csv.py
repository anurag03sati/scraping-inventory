import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import re
import os
from glob import glob

latest_file = max(glob('data_*.json'), key=os.path.getctime)
print(latest_file)
# Step 1: Load and transform JSON data
with open(latest_file, 'r') as json_file:
    json_data = json.load(json_file)

# Step 2: Define column mappings for different categories
column_mappings = {
    'generators': {
        'Title': ['title'],
        'Manufacturer': ['manufacturer'],
        'kVA': ['specifications', 'kVA'],
        'KW': [],
        'Voltage': ['specifications', 'Voltage'],
        'Fuel Type': ['specifications', 'Fuel Type'],
        'Rating': [],
        'Enclosure': [],
        'Description': ['other_description'],
        'Link': ['url']
    },
    'motor': {
        'Title': ['title'],
        'Manufacturer': ['manufacturer'],
        'Amps': ['specifications', 'Amps'],
        'Voltage': ['specifications', 'Voltage'],
        'Max. Voltage': ['specifications', 'Max. Voltage'],
        'Bus Ratings': ['other_description', 'Bus Ratings'],
        'Electrical': ['other_description', 'Electrical'],
        'Description': ['other_description'],
        'Link': ['url']
    },
    'switchgears': {
        'Title': ['title'],
        'Manufacturer': ['manufacturer'],
        'Amps': ['specifications', 'Amps'],
        'Voltage': ['specifications', 'Voltage'],
        'Max. Voltage': ['specifications', 'Max. Voltage'],
        'Enclosure': [],
        'Description': ['other_description'],
        'Link': ['url']
    },
    'switches': {
        'Title': ['title'],
        'Manufacturer': ['manufacturer'],
        'Amps': ['specifications', 'Amps'],
        'Voltage': ['specifications', 'Voltage'],
        'Max. Voltage': ['specifications', 'Max. Voltage'],
        'Enclosure': [],
        'Description': ['other_description'],
        'Link': ['url']
    },
    'transformer': {
        'Title': ['title'],
        'Manufacturer': ['manufacturer'],
        'kVA': ['specifications', 'kVA'],
        'Primary Voltage': ['specifications', 'Primary Voltage'],
        'Secondary Voltage': ['specifications', 'Secondary Voltage'],
        'Phase': ['specifications', 'Phase'],
        'Oil vs Dry': ['specifications', 'Oil vs Dry'],
        'Transformer Type': ['specifications', 'Transformer Type'],
        'Enclosure': ['specifications', 'Enclosure Type'],
        'Description': ['other_description'],
        'Link': ['url']
    },
}


# Step 3: Function to merge columns based on mappings
def merge_columns(df, new_col, old_cols):
    def get_nested_value(entry, keys):
        try:
            for key in keys:
                entry = entry[key]
            return entry
        except (KeyError, TypeError):
            return None

    def extract_from_description(description, field):
        if pd.isnull(description):
            return None
        if field == 'Bus Ratings':
            match = re.search(r'Bus Rating(?:s)?:\s*(.*?)(?:\s*Top|$)', description)
        elif field == 'Electrical':
            match = re.search(r'Electrical:\s*(.*?)(?:\s*Short Circuit|$)', description)
        else:
            return None
        if match:
            return match.group(1).strip()
        return None

    if old_cols:
        if new_col in ['Bus Ratings', 'Electrical']:
            merged_col = df['other_description'].apply(lambda desc: extract_from_description(desc, new_col))
        else:
            merged_col = df.apply(lambda row: get_nested_value(row, old_cols), axis=1)
    else:
        merged_col = pd.Series([None] * len(df), index=df.index)
    return merged_col


# Step 4: Process each category and write to separate CSV files
output_folder = 'Lenmark_data2'
os.makedirs(output_folder, exist_ok=True)

for category, mappings in column_mappings.items():
    category_data = [entry for entry in json_data if entry['category'].lower() == category]
    if not category_data:
        continue

    df = pd.DataFrame(category_data)
    merged_df = pd.DataFrame()

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(merge_columns, df, new_col, old_cols): new_col for new_col, old_cols in
                   mappings.items()}
        for future in futures:
            new_col = futures[future]
            merged_df[new_col] = future.result()

    # Ensure all columns specified in mappings are present in the DataFrame
    for col in mappings.keys():
        if col not in merged_df:
            merged_df[col] = None

    output_file_path = os.path.join(output_folder, f'{category.capitalize()}.csv')
    merged_df.to_csv(output_file_path, index=False)

    print(f"Transformed and filtered data for {category.capitalize()} saved to {output_file_path}")
