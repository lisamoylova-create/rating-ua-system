import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)

# Test file processing with exact logic
df = pd.read_excel('attached_assets/test baza do vidbory-2_1756248855291.xlsx')

print("=== Testing data extraction ===")
for index in range(min(3, len(df))):
    row = df.iloc[index]
    
    # Test exact column access
    edrpou_raw = row.get('Код ЄДРПОУ')
    name_raw = row.get('Название компании')
    
    print(f"Row {index}:")
    print(f"  Raw EDRPOU: {repr(edrpou_raw)} (type: {type(edrpou_raw)})")
    print(f"  Raw Name: {repr(name_raw)} (type: {type(name_raw)})")
    
    # Test cleaned values
    if pd.isna(edrpou_raw):
        print(f"  EDRPOU is NaN!")
    else:
        edrpou_clean = str(edrpou_raw).strip()
        print(f"  Clean EDRPOU: {repr(edrpou_clean)}")
    
    if pd.isna(name_raw):
        print(f"  Name is NaN!")
    else:
        name_clean = str(name_raw).strip()
        print(f"  Clean Name: {repr(name_clean)}")
    
    print()