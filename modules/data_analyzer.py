# modules/data_analyzer.py
"""
Exploratory Data Analysis module for Airbnb data
Performs comprehensive analysis on raw data files
"""

import pandas as pd
import os
import glob
from typing import Dict, List, Tuple
import numpy as np
import json
from datetime import datetime

from modules.spark_manager import SparkSessionManager
from config.settings import Config

class AirbnbDataAnalyzer:
    """
    Performs EDA on Airbnb dataset to understand:
    - Data structure and schema
    - Data quality issues
    - Relationships between tables
    - Recommendations for data warehouse design
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.analysis_results = {}
        self.spark_manager = SparkSessionManager(config)
        self.spark = self.spark_manager.start_session()
    
    def analyze_all_files(self):
        """Main EDA analysis method"""
        print("🔍 Starting Exploratory Data Analysis...")
        print(f"📁 Data folder: {self.config.RAW_DATA_FOLDER}")
        
        try:
            # Find and analyze files
            file_groups = self._discover_files()
            
            # Analyze each file type
            for file_type, file_list in file_groups.items():
                self.analyze_file_type(file_type, file_list)
            
            # Generate comprehensive reports
            self.generate_summary_report()
            self.get_recommendations()
            self.identify_join_keys()
            
        finally:
            self.spark_manager.stop_session()
    
    def _discover_files(self) -> Dict[str, List[str]]:
        """Discover and group files by type"""
        file_patterns = {
            "listings": os.path.join(self.config.RAW_DATA_FOLDER, self.config.LISTINGS_PATTERN),
            "calendar": os.path.join(self.config.RAW_DATA_FOLDER, self.config.CALENDAR_PATTERN),
            "reviews": os.path.join(self.config.RAW_DATA_FOLDER, self.config.REVIEWS_PATTERN),
        }
        file_groups = {}
        
        for file_type, pattern in file_patterns.items():
            files = glob.glob(pattern)
            if files:
                file_groups[file_type] = files
                print(f"✅ Found {len(files)} {file_type} files")
            else:
                print(f"⚠️  No {file_type} files found")
        
        return file_groups
    
    def analyze_file_type(self, file_type: str, files: List[str]):
        """Analyze a specific file type"""
        print(f"\n{'='*60}")
        print(f"📊 Analyzing {file_type.upper()} files")
        print(f"{'='*60}")
        
        print(f"📂 Found {len(files)} {file_type} files")
        
        # Use first file for detailed analysis
        sample_file = files[0]
        print(f"🔍 Detailed analysis of: {os.path.basename(sample_file)}")
        
        # Run both Pandas and Spark analysis
        pandas_analysis = self._pandas_analysis(sample_file, file_type)
        spark_analysis = self._spark_analysis_safe(sample_file, file_type)
        
        # Store results
        self.analysis_results[file_type] = {
            'pandas': pandas_analysis,
            'spark': spark_analysis,
            'total_files': len(files),
            'sample_files': [os.path.basename(f) for f in files[:3]]
        }
        
        self._print_analysis_summary(file_type, pandas_analysis, spark_analysis)
    
    def _pandas_analysis(self, file_path: str, file_type: str) -> Dict:
        """Quick analysis using Pandas (for smaller datasets)"""
        try:
            print("   📊 Running Pandas analysis...")
            
            df = pd.read_csv(file_path, compression='gzip', nrows=self.config.SAMPLE_SIZE)
            
            analysis = {
                'file_name': os.path.basename(file_path),
                'shape': df.shape,
                'columns': list(df.columns),
                'dtypes': df.dtypes.astype(str).to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict(),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
                'duplicate_rows': df.duplicated().sum(),
            }
            
            # Add basic statistics
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if not numeric_cols.empty:
                analysis['numeric_stats'] = df[numeric_cols].describe().to_dict()
            
            return analysis
            
        except Exception as e:
            return {'error': f"Pandas analysis failed: {str(e)}"}
    
    def _spark_analysis_safe(self, file_path: str, file_type: str) -> Dict:
        """Safe PySpark analysis for large datasets"""
        try:
            print("   ⚡ Running PySpark analysis...")
            
            df = self.spark.read \
                .option("compression", "gzip") \
                .option("inferSchema", "false") \
                .csv(file_path, header=True)
            
            # Basic statistics
            row_count = df.count()
            column_count = len(df.columns)
            
            return {
                'file_name': os.path.basename(file_path),
                'shape': (row_count, column_count),
                'schema': [{'column': field.name, 'type': str(field.dataType)} 
                          for field in df.schema.fields]
            }
            
        except Exception as e:
            return {'error': f"Spark analysis failed: {str(e)}"}
    
    def _print_analysis_summary(self, file_type: str, pandas_analysis: Dict, spark_analysis: Dict):
        """Print analysis summary"""
        print(f"\n   📋 {file_type.upper()} ANALYSIS SUMMARY:")
        print(f"   {'─' * 40}")
        
        if 'error' in pandas_analysis:
            print(f"   ❌ Pandas Error: {pandas_analysis['error']}")
        else:
            print(f"   📊 Shape: {pandas_analysis['shape']}")
            print(f"   🗂️  Columns: {len(pandas_analysis['columns'])}")
            print(f"   💾 Memory: {pandas_analysis['memory_usage_mb']:.2f} MB")
    
    def generate_summary_report(self):
        """Generate comprehensive EDA summary"""
        print(f"\n{'='*80}")
        print("📈 COMPREHENSIVE EDA SUMMARY REPORT")
        print(f"{'='*80}")
        
        for file_type, analysis in self.analysis_results.items():
            print(f"\n🎯 {file_type.upper()} FILES:")
            pandas_data = analysis['pandas']
            
            if 'error' not in pandas_data:
                print(f"   📊 Dimensions: {pandas_data['shape'][0]:,} rows × {pandas_data['shape'][1]} columns")
                print(f"   💾 Memory usage: {pandas_data['memory_usage_mb']:.2f} MB")
    
    def get_recommendations(self):
        """Generate data cleaning and modeling recommendations"""
        print(f"\n{'='*80}")
        print("💡 DATA WAREHOUSE DESIGN RECOMMENDATIONS")
        print(f"{'='*80}")
        
        for file_type, analysis in self.analysis_results.items():
            pandas_data = analysis['pandas']
            
            if 'error' in pandas_data:
                continue
                
            print(f"\n📋 {file_type.upper()} RECOMMENDATIONS:")
            
            # Data quality recommendations
            high_missing_cols = {k: v for k, v in pandas_data['missing_percentage'].items() if v > 50}
            if high_missing_cols:
                print(f"   🗑️  Drop columns with >50% missing: {list(high_missing_cols.keys())}")
    
    def identify_join_keys(self):
        """Identify relationships between tables"""
        print(f"\n{'='*80}")
        print("🔗 DATA MODEL RELATIONSHIPS")
        print(f"{'='*80}")
        
        print("   📊 Fact Tables: calendar (daily metrics), reviews (review metrics)")
        print("   🏠 Dimension Tables: listings (property info), hosts, neighborhoods")
        print("   🔑 Primary Keys: listing_id, host_id, date")
        print("   🤝 Foreign Keys: listing_id connects all tables")