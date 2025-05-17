import os
import pandas as pd
import argparse
import sys
from collections import defaultdict
import json
from typing import Dict, List, Any, Optional, Tuple
import csv
import datetime

def analyze_csv_files(directory_path: str, output_file: str, output_format: str = "text", 
                     sample_rows: int = 5, encoding: str = "utf-8") -> None:
    """
    Analyze all CSV files in the specified directory and output column information to a file.
    
    Args:
        directory_path: Path to directory containing CSV files
        output_file: Path to write the output report
        output_format: Output format ('text', 'json', or 'markdown')
        sample_rows: Number of sample rows to display for each file
        encoding: File encoding to use when reading CSVs
    """
    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Find all CSV files in the directory
    csv_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        sys.exit(0)
    
    results = {}
    
    # Analyze each CSV file
    print(f"Analyzing {len(csv_files)} CSV files...")
    for i, file_name in enumerate(csv_files):
        print(f"Processing {i+1}/{len(csv_files)}: {file_name}")
        file_path = os.path.join(directory_path, file_name)
        try:
            # Try to detect the delimiter
            with open(file_path, 'r', encoding=encoding) as f:
                sample = f.read(4096)
                dialect = csv.Sniffer().sniff(sample)
                delimiter = dialect.delimiter
            
            # Read the file with the detected delimiter
            df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, 
                            low_memory=False, nrows=1000)  # Read first 1000 rows for analysis
            
            # Get basic file info
            row_count = len(df)
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
            
            # Analyze columns
            columns_info = []
            for column in df.columns:
                non_null_count = df[column].count()
                null_percentage = (1 - non_null_count / row_count) * 100 if row_count > 0 else 0
                
                # Determine data type
                inferred_type = str(df[column].dtype)
                if inferred_type == 'object':
                    # Check if it might be a date
                    if pd.to_datetime(df[column], errors='coerce').notna().any():
                        inferred_type = 'datetime'
                
                # Get unique values and their count
                unique_count = df[column].nunique()
                unique_percentage = (unique_count / row_count) * 100 if row_count > 0 else 0
                
                # Get sample values (avoid very long values)
                sample_values = df[column].dropna().head(3).tolist()
                sample_values = [str(val)[:100] + '...' if len(str(val)) > 100 else str(val) 
                               for val in sample_values]
                
                columns_info.append({
                    'name': column,
                    'inferred_type': inferred_type,
                    'unique_count': unique_count,
                    'unique_percentage': round(unique_percentage, 2),
                    'null_percentage': round(null_percentage, 2),
                    'sample_values': sample_values
                })
            
            # Store results
            results[file_name] = {
                'file_size_mb': round(file_size, 2),
                'row_count': row_count,
                'column_count': len(df.columns),
                'delimiter': repr(delimiter),
                'columns': columns_info,
                'sample_rows': df.head(sample_rows).to_dict('records')
            }
            
        except Exception as e:
            results[file_name] = {
                'error': str(e)
            }
    
    # Write results to file based on format
    with open(output_file, 'w', encoding='utf-8') as f:
        if output_format == 'json':
            # Custom JSON encoder to handle datetime and other non-serializable types
            class CustomEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, (datetime.datetime, datetime.date)):
                        return obj.isoformat()
                    return str(obj)
                    
            json.dump(results, f, indent=2, cls=CustomEncoder)
        
        elif output_format == 'markdown':
            f.write(f"# CSV Analysis Report\n\n")
            f.write(f"Analysis of {len(csv_files)} CSV files from `{directory_path}`\n\n")
            f.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"## Summary\n\n")
            
            # Write summary table
            f.write("| File Name | Columns | Rows | Size (MB) | Status |\n")
            f.write("|-----------|---------|------|-----------|--------|\n")
            for file_name, info in results.items():
                if 'error' in info:
                    status = "❌ Error"
                    columns = "-"
                    rows = "-"
                    size = "-"
                else:
                    status = "✅ OK"
                    columns = str(info['column_count'])
                    rows = str(info['row_count'])
                    size = str(info['file_size_mb'])
                f.write(f"| {file_name} | {columns} | {rows} | {size} | {status} |\n")
            
            f.write("\n---\n\n")
            
            # Write detailed file information
            for file_name, info in results.items():
                f.write(f"## {file_name}\n\n")
                
                if 'error' in info:
                    f.write(f"**ERROR**: {info['error']}\n\n")
                    continue
                    
                f.write(f"- File Size: {info['file_size_mb']} MB\n")
                f.write(f"- Row Count: {info['row_count']}\n")
                f.write(f"- Column Count: {info['column_count']}\n")
                f.write(f"- Delimiter: {info['delimiter']}\n")
                f.write("\n### Columns\n\n")
                
                f.write("| Column Name | Type | Unique Values | Null % | Sample Values |\n")
                f.write("|-------------|------|--------------|--------|---------------|\n")
                
                for col in info['columns']:
                    samples = ', '.join([str(s) for s in col['sample_values']])
                    f.write(f"| {col['name']} | {col['inferred_type']} | {col['unique_count']} ({col['unique_percentage']}%) | {col['null_percentage']}% | {samples} |\n")
                
                f.write("\n### Sample Data\n\n")
                if info['sample_rows']:
                    headers = list(info['sample_rows'][0].keys())
                    f.write("| " + " | ".join(headers) + " |\n")
                    f.write("| " + " | ".join(["---" for _ in headers]) + " |\n")
                    
                    for row in info['sample_rows']:
                        values = [str(row[h])[:20].replace("\n", " ") for h in headers]
                        f.write("| " + " | ".join(values) + " |\n")
                
                f.write("\n---\n\n")
        
        else:  # text format
            f.write(f"CSV Analysis Report\n\n")
            f.write(f"Analysis of {len(csv_files)} CSV files from: {directory_path}\n")
            f.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"{'=' * 80}\n")
            f.write(f"SUMMARY\n")
            f.write(f"{'=' * 80}\n\n")
            
            for file_name in results:
                if 'error' in results[file_name]:
                    status = "ERROR: " + results[file_name]['error']
                else:
                    status = f"{results[file_name]['column_count']} columns, {results[file_name]['row_count']} rows, {results[file_name]['file_size_mb']} MB"
                f.write(f"- {file_name}: {status}\n")
            
            for file_name, info in results.items():
                f.write(f"\n{'=' * 80}\n")
                f.write(f"FILE: {file_name}\n")
                f.write(f"{'=' * 80}\n")
                
                if 'error' in info:
                    f.write(f"ERROR: {info['error']}\n")
                    continue
                    
                f.write(f"File Size: {info['file_size_mb']} MB\n")
                f.write(f"Row Count: {info['row_count']}\n")
                f.write(f"Column Count: {info['column_count']}\n")
                f.write(f"Delimiter: {info['delimiter']}\n\n")
                
                f.write("COLUMNS:\n")
                f.write(f"{'Column Name':<40} {'Type':<15} {'Unique':<20} {'Null %':<10} {'Sample Values'}\n")
                f.write('-' * 120 + '\n')
                
                for col in info['columns']:
                    samples = ', '.join([str(s) for s in col['sample_values']])
                    unique_info = f"{col['unique_count']} ({col['unique_percentage']}%)"
                    f.write(f"{col['name']:<40} {col['inferred_type']:<15} {unique_info:<20} {col['null_percentage']:<10}% {samples}\n")
                
                f.write("\nSAMPLE DATA:\n")
                f.write('-' * 120 + '\n')
                if info['sample_rows']:
                    for i, row in enumerate(info['sample_rows']):
                        f.write(f"Row {i+1}:\n")
                        for key, value in row.items():
                            # Truncate very long values
                            display_value = str(value)
                            if len(display_value) > 70:
                                display_value = display_value[:70] + "..."
                            f.write(f"  {key}: {display_value}\n")
                    f.write('\n')
    
    print(f"Analysis complete! Report written to: {output_file}")
    print(f"Analyzed {len(csv_files)} CSV files from {directory_path}")
    
    # Print a quick summary to console
    for file_name in results:
        if 'error' in results[file_name]:
            status = "ERROR"
        else:
            status = f"{results[file_name]['column_count']} columns, {results[file_name]['row_count']} rows"
        print(f"- {file_name}: {status}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze CSV files in a directory')
    parser.add_argument('directory', help='Directory containing CSV files')
    parser.add_argument('--output', '-o', default='csv_analysis_report.txt',
                       help='Output file path (default: csv_analysis_report.txt)')
    parser.add_argument('--format', '-f', choices=['text', 'json', 'markdown'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--samples', '-s', type=int, default=5, 
                       help='Number of sample rows to display (default: 5)')
    parser.add_argument('--encoding', '-e', default='utf-8',
                       help='File encoding (default: utf-8)')
    
    args = parser.parse_args()
    
    analyze_csv_files(args.directory, args.output, args.format, args.samples, args.encoding)