# modules/data_cleaner.py (CORRECTED VERSION)
"""
Data cleaning with proper geographic inference from filenames
"""

import pandas as pd
import os
import glob
from typing import Dict, List, Set, Tuple
import json

from config.settings import Config
from utils.utility import validate_directory, print_progress
from utils.logger import get_logger

logger = get_logger(__name__)

class AirbnbDataCleaner:
    
    def __init__(self, config: Config):
        self.config = config
        self.relevant_columns = self._define_minimal_columns()
    
    def infer_geography_from_filename(self, filename: str) -> Tuple[str, str]:
        """
        Extract country and city from filename
        Filename patterns:
        - "United_States_Hawaii_listings_13-June-2025.csv.gz" → ("Hawaii", "United States")
        - "Argentina_Buenos Aires_listings_29-January-2025.csv.gz" → ("Buenos Aires", "Argentina")
        """
        try:
            basename = os.path.basename(filename)
            # Remove file extension and split by underscores
            name_without_ext = basename.replace('.csv.gz', '')
            parts = name_without_ext.split('_')
            
            # The pattern is: Country_City_listings_date.csv.gz
            if len(parts) >= 4:
                country = parts[0].replace('_', ' ')  # "United States"
                city = parts[1].replace('_', ' ')     # "Hawaii"
                return city, country
            
        except Exception as e:
            logger.warning(f"   ⚠️  Error parsing filename {filename}: {e}")
        
        return 'Unknown', 'Unknown'
    
    def parse_host_location(self, location_string: str) -> Tuple[str, str]:
        """Parse host_location string into city and country"""
        if pd.isna(location_string) or location_string == '':
            return 'Unknown', 'Unknown'
        
        parts = str(location_string).split(',')
        parts = [part.strip() for part in parts if part.strip()]
        
        if len(parts) >= 2:
            # "City, Country" format (97.4% of cases)
            country = parts[-1]
            city = ', '.join(parts[:-1])
            return city, country
        elif len(parts) == 1:
            # Just country name (2.6% of cases)
            return 'Unknown', parts[0]
        else:
            return 'Unknown', 'Unknown'
    
    def _clean_file_type(self, file_type: str, output_folder: str):
        """Clean a specific file type with proper geographic hierarchy"""
        logger.info(f"\n📁 Processing {file_type} files...")
        pattern = os.path.join(self.config.RAW_DATA_FOLDER, f"*{file_type}*.csv.gz")
        files = glob.glob(pattern)
        
        if not files:
            logger.error(f"❌ No {file_type} files found")
            return
        
        relevant_cols = self.relevant_columns.get(file_type, [])
        processed_count = 0
        
        for file_path in files:
            try:
                # Read the file
                df = pd.read_csv(file_path, compression='gzip')
                
                # Keep only relevant columns that exist in the file
                existing_cols = [col for col in relevant_cols if col in df.columns]
                missing_cols = [col for col in relevant_cols if col not in df.columns]
                
                if missing_cols:
                    logger.warning(f"   ⚠️  Missing columns: {missing_cols}")
                
                # Create cleaned dataframe
                df_clean = df[existing_cols].copy()
                
                # Enhanced location processing for listings
                if file_type == 'listings':
                    # 1. Parse host_location into host_city and host_country
                    if 'host_location' in df_clean.columns:
                        host_location_data = df_clean['host_location'].apply(self.parse_host_location)
                        df_clean['host_city'] = host_location_data.apply(lambda x: x[0])
                        df_clean['host_country'] = host_location_data.apply(lambda x: x[1])
                        
                        logger.info(f"   👤 Parsed host_location into:")
                        logger.info(f"      • {df_clean['host_country'].nunique()} unique host countries")
                        logger.info(f"      • {df_clean['host_city'].nunique()} unique host cities")
                    
                    # 2. Create property geographic hierarchy FROM FILENAME (CORRECTED)
                    property_city, property_country = self.infer_geography_from_filename(file_path)
                    df_clean['property_country'] = property_country
                    df_clean['property_city'] = property_city
                    df_clean['property_neighbourhood'] = df_clean.get('neighbourhood_cleansed', 'Unknown')
                    
                    logger.info(f"   🏠 Created property hierarchy from filename:")
                    logger.info(f"      • Property: {property_city}, {property_country}")
                    logger.info(f"      • {df_clean['property_neighbourhood'].nunique()} neighborhoods")
                    
                    # 4. DROP redundant columns
                    columns_to_drop = []
                    if 'host_location' in df_clean.columns:
                        columns_to_drop.append('host_location')
                    if 'neighbourhood_cleansed' in df_clean.columns:
                        columns_to_drop.append('neighbourhood_cleansed')
                    
                    if columns_to_drop:
                        df_clean = df_clean.drop(columns_to_drop, axis=1)
                        logger.info(f"   🗑️  Dropped redundant columns: {columns_to_drop}")
                    
                    # Show final geographic distribution
                    if 'property_country' in df_clean.columns:
                        country_counts = df_clean['property_country'].value_counts()
                        logger.info(f"   📊 Final property distribution:")
                        for country, count in country_counts.items():
                            logger.info(f"      • {country}: {count:,} listings")
                
                # Save cleaned file
                if df_clean.empty:
                    logger.warning(f"   ⚠️  Skipping empty file: {os.path.basename(file_path)}")
                    continue

                output_filename = f"minimal_{os.path.basename(file_path)}"
                output_path = os.path.join(output_folder, output_filename)
                df_clean.to_csv(output_path, compression='gzip', index=False, sep='|')
                
                final_col_count = df_clean.shape[1]
                original_col_count = df.shape[1]
                reduction_percent = ((original_col_count - final_col_count) / original_col_count) * 100
                
                logger.info(f"   ✅ Cleaned: {os.path.basename(file_path)}")
                logger.info(f"      📊 {original_col_count} → {final_col_count} columns (-{reduction_percent:.1f}%)")
                logger.info(f"      📈 {df_clean.shape[0]:,} rows preserved")
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"   ❌ Error processing {os.path.basename(file_path)}: {e}")
        
        logger.info(f"🎯 Successfully processed {processed_count}/{len(files)} {file_type} files")

    # ... rest of the class remains the same ...
    def _define_minimal_columns(self) -> Dict[str, List[str]]:
        """Define focused columns - using only reliable data"""
        return {
            'listings': [
                # Core identifiers
                'id', 'host_id', 'host_name', 'host_location',
                
                # Property location - ONLY reliable columns
                'neighbourhood_cleansed',
                
                # Core metrics
                'price', 'number_of_reviews', 'review_scores_rating',
                'calculated_host_listings_count'
            ],
            
            'reviews': [
                'listing_id', 'id', 'date', 'reviewer_id', 'reviewer_name', 'comments'
            ],
            
            'calendar': [
                'listing_id', 'date', 'available', 'price'
            ]
        }
    
    def analyze_column_relevance(self):
        """Analyze which columns exist and their relevance"""
        logger.info("🔍 Analyzing column relevance across all files...")
        
        # First, analyze property location columns specifically
        self.analyze_property_location_columns()
        
        logger.info(f"\nCONTINUING WITH GENERAL COLUMN ANALYSIS")
        logger.info("-" * 40)
        
        for file_type in ['listings', 'reviews', 'calendar']:
            logger.info(f"\n{file_type.upper()} FILES:")
            
            pattern = os.path.join(self.config.RAW_DATA_FOLDER, f"*{file_type}*.csv.gz")
            files = glob.glob(pattern)
            
            if not files:
                logger.warning("   ❌ No files found")
                continue
                
            logger.info(f"   Found {len(files)} files")
            
            # Analyze first file
            sample_file = files[0]
            try:
                df_sample = pd.read_csv(sample_file, compression='gzip', nrows=0)
                all_columns = set(df_sample.columns)
                relevant_cols = set(self.relevant_columns[file_type])
                
                logger.info(f"   📊 Columns: {len(all_columns)} total → {len(relevant_cols)} kept")
                logger.info(f"   🗑️  Reduction: {((len(all_columns) - len(relevant_cols)) / len(all_columns) * 100):.1f}%")
                
                logger.info(f"\n   ✅ KEEPING ({len(relevant_cols)}):")
                for col in sorted(relevant_cols):
                    exists = "✓" if col in all_columns else "✗"
                    logger.info(f"      {exists} {col}")
                    
            except Exception as e:
                logger.error(f"   ⚠️  Error reading {os.path.basename(sample_file)}: {e}")
    
    def analyze_property_location_columns(self):
        """Specifically analyze property location columns"""
        logger.info("🔍 Analyzing PROPERTY location columns...")
        
        pattern = os.path.join(self.config.RAW_DATA_FOLDER, "*listings*.csv.gz")
        files = glob.glob(pattern)
        
        if not files:
            logger.error("❌ No listing files found")
            return
        
        sample_file = files[0]
        logger.info(f"📊 Analyzing: {os.path.basename(sample_file)}")
        
        # Show how filename parsing will work
        logger.info(f"\n🏠 PROPERTY LOCATION INFERENCE FROM FILENAMES:")
        for file_path in files[:5]:  # Show first 5 files as examples
            city, country = self.infer_geography_from_filename(file_path)
            logger.info(f"   • {os.path.basename(file_path)}")
            logger.info(f"     → Property: {city}, {country}")
    
    def create_cleaned_dataset(self, output_folder: str = None):
        """Create cleaned datasets with proper geographic hierarchy"""
        if not output_folder:
            output_folder = self.config.CLEANED_DATA_FOLDER
        
        # The directory is already ensured to exist by Config.__init__
        # validate_directory(output_folder, create_if_missing=True)
        logger.info(f"\n🧹 Creating cleaned datasets in: {output_folder}")
        
        # Process each file type
        for file_type in ['listings', 'reviews', 'calendar']:
            self._clean_file_type(file_type, output_folder)