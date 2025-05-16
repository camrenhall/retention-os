# src/retention_os/data_ingestion/parsers/csv_parser.py
import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Any

class CSVParser:
    """
    Utility class for parsing CSV files with various options.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def detect_encoding(self, file_path: str) -> str:
        """
        Detect the encoding of a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Detected encoding
        """
        try:
            import chardet
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read(10000))
                return result['encoding'] or 'utf-8'
        except ImportError:
            self.logger.warning("chardet library not installed, defaulting to utf-8")
            return 'utf-8'
        except Exception as e:
            self.logger.error(f"Error detecting encoding: {str(e)}")
            return 'utf-8'
    
    def detect_delimiter(self, file_path: str, encoding: str = 'utf-8') -> str:
        """
        Detect the delimiter used in a CSV file.
        
        Args:
            file_path: Path to the CSV file
            encoding: File encoding
            
        Returns:
            Detected delimiter (comma, tab, semicolon, etc.)
        """
        try:
            import csv
            sniffer = csv.Sniffer()
            with open(file_path, 'r', encoding=encoding) as f:
                sample = f.read(4096)
                dialect = sniffer.sniff(sample)
                return dialect.delimiter
        except Exception as e:
            self.logger.error(f"Error detecting delimiter: {str(e)}")
            return ','  # Default to comma
    
    def parse_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Parse a CSV file into a pandas DataFrame with intelligent defaults.
        
        Args:
            file_path: Path to the CSV file
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            Pandas DataFrame with the CSV data
        """
        try:
            # Auto-detect encoding if not provided
            encoding = kwargs.pop('encoding', None)
            if encoding is None:
                encoding = self.detect_encoding(file_path)
                
            # Auto-detect delimiter if not provided
            delimiter = kwargs.pop('delimiter', None)
            if delimiter is None:
                delimiter = self.detect_delimiter(file_path, encoding)
            
            # Read with sensible defaults
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                delimiter=delimiter,
                low_memory=False,  # Avoid mixed type inference warnings
                keep_default_na=True,  # Interpret various NA values
                na_values=['NULL', 'null', 'N/A', 'n/a', '', ' '],  # Common NA strings
                **kwargs
            )
            
            self.logger.info(f"Successfully parsed {file_path}, shape: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing {file_path}: {str(e)}")
            raise