# analyze_host_location.py
import pandas as pd
import os
import glob
from typing import Dict, List

def analyze_host_location():
    """Standalone script to analyze host_location column format"""
    data_folder = r"D:\Projects\SQL-data-warehouse\data\raw_data_airbnb_insights"
    
    print("🔍 Analyzing host_location column format...")
    print("=" * 50)
    
    # Find listing files
    pattern = os.path.join(data_folder, "*listings*.csv.gz")
    files = glob.glob(pattern)
    
    if not files:
        print("❌ No listing files found")
        return
    
    # Use first file for analysis
    sample_file = files[0]
    print(f"📊 Analyzing: {os.path.basename(sample_file)}")
    
    try:
        # Read sample data
        df = pd.read_csv(sample_file, compression='gzip', nrows=1000)
        
        if 'host_location' not in df.columns:
            print("❌ host_location column not found")
            return
        
        print(f"\n📈 Dataset size: {len(df)} rows")
        print(f"📍 host_location missing values: {df['host_location'].isna().sum()} ({(df['host_location'].isna().sum()/len(df)*100):.1f}%)")
        
        # Get non-null host locations
        host_locations = df['host_location'].dropna()
        print(f"📍 Valid host locations: {len(host_locations)}")
        
        print(f"\n🎯 FIRST 15 HOST_LOCATION VALUES:")
        print("-" * 40)
        for i, location in enumerate(host_locations.head(15)):
            print(f"{i+1:2d}. '{location}'")
        
        print(f"\n🔍 FORMAT ANALYSIS:")
        print("-" * 40)
        
        # Analyze parts and patterns
        format_stats = {
            '1_part': 0,    # Just country: "Argentina"
            '2_parts': 0,   # City, Country: "Buenos Aires, Argentina" 
            '3_parts': 0,   # Full address: "Buenos Aires, Capital Federal, Argentina"
            'other': 0
        }
        
        country_only = 0
        city_country = 0
        unknown_format = 0
        
        for location in host_locations:
            location_str = str(location).strip()
            parts = [part.strip() for part in location_str.split(',') if part.strip()]
            
            # Count parts
            if len(parts) == 1:
                format_stats['1_part'] += 1
                country_only += 1
            elif len(parts) == 2:
                format_stats['2_parts'] += 1
                city_country += 1
            elif len(parts) >= 3:
                format_stats['3_parts'] += 1
                unknown_format += 1
            else:
                format_stats['other'] += 1
        
        # Print format statistics
        total_valid = len(host_locations)
        print(f"📊 Format Distribution:")
        for format_type, count in format_stats.items():
            if count > 0:
                percentage = (count / total_valid) * 100
                print(f"   • {format_type}: {count} ({percentage:.1f}%)")
        
        print(f"\n🎨 COMMON PATTERNS:")
        print("-" * 40)
        
        # Show most common values
        common_locations = host_locations.value_counts().head(10)
        for location, count in common_locations.items():
            percentage = (count / total_valid) * 100
            parts = [part.strip() for part in str(location).split(',') if part.strip()]
            print(f"   • '{location}'")
            print(f"     → {len(parts)} parts: {parts}")
            print(f"     → {count} occurrences ({percentage:.1f}%)")
            print()
        
        print(f"\n💡 INFERENCE FOR PARSING:")
        print("-" * 40)
        
        if format_stats['2_parts'] > format_stats['1_part']:
            print("✅ Most host_locations are in 'City, Country' format")
            print("   → Can reliably parse into host_city and host_country")
        elif format_stats['1_part'] > format_stats['2_parts']:
            print("✅ Most host_locations are just country names") 
            print("   → Can parse into host_country (host_city will be 'Unknown')")
        else:
            print("⚠️  Mixed formats - will need fallback parsing")
        
        # Test parsing on sample values
        print(f"\n🧪 PARSING TEST ON SAMPLE VALUES:")
        print("-" * 40)
        
        test_locations = host_locations.head(5)
        for location in test_locations:
            host_city, host_country = parse_host_location(location)
            print(f"   • '{location}' → City: '{host_city}', Country: '{host_country}'")
            
    except Exception as e:
        print(f"❌ Error analyzing host_location: {e}")

def parse_host_location(location_string: str):
    """Parse host_location string into city and country"""
    if pd.isna(location_string) or location_string == '':
        return 'Unknown', 'Unknown'
    
    parts = str(location_string).split(',')
    parts = [part.strip() for part in parts if part.strip()]
    
    if len(parts) >= 2:
        # Assume "City, Country" format
        country = parts[-1]
        city = ', '.join(parts[:-1])
        return city, country
    elif len(parts) == 1:
        # Assume it's just a country
        return 'Unknown', parts[0]
    else:
        return 'Unknown', 'Unknown'

if __name__ == "__main__":
    analyze_host_location()