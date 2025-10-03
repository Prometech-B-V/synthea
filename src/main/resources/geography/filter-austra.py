import pandas as pd

# Load ADRESSE.csv (adjust path if needed; large file, ~500 MB uncompressed)
df = pd.read_csv('austria-addresses.csv', sep=';', encoding='utf-8', dtype=str)  # All as string to avoid type issues

# Filter for Vienna: GKZ = '90001'
vienna_df = df[df['GKZ'] == '90001']

# Select key location columns from PDF (address and coordinates)
vienna_locations = vienna_df[[
    'ADRCD',  # Unique address ID
    'PLZ',  # Postal code
    'HNR_ADR_ZUSAMMEN',  # Combined house number
    'HOFNAME',  # Optional: Hofname
    'RW',  # Easting (projected)
    'HW',  # Northing (projected)
    'EPSG',  # Projection code (e.g., '31255')
    'QUELLADRESSE',  # Quality of coordinate
    'BESTIMMUNGSART'  # Determination method
]]

# Convert RW/HW to float for numeric use
vienna_locations['RW'] = pd.to_numeric(vienna_locations['RW'], errors='coerce')
vienna_locations['HW'] = pd.to_numeric(vienna_locations['HW'], errors='coerce')

# Drop rows with missing coordinates
vienna_locations = vienna_locations.dropna(subset=['RW', 'HW'])

# Add city/state columns for consistency (hardcoded for Vienna)
vienna_locations['CITY'] = 'Wien'
vienna_locations['STATE'] = 'Wien'

# Save to new CSV
vienna_locations.to_csv('vienna_locations.csv', index=False, sep=';', encoding='utf-8')

print(f"Extracted {len(vienna_locations)} Vienna locations.")
print(vienna_locations.head())  # Preview first 5 rows
