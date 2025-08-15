"""
Main entry point for PySpark applications.
"""

import sys
import argparse
from src.jobs.data_ingestion import main as ingestion_main
from src.jobs.data_transformation import main as transformation_main  
from src.jobs.data_quality import main as quality_main

def main():
    """Main entry point that routes to specific job types."""
    parser = argparse.ArgumentParser(description='PySpark Job Runner')
    parser.add_argument('job_type', choices=['ingestion', 'transformation', 'quality'],
                       help='Type of job to run')
    parser.add_argument('--input-path', help='Input data path')
    parser.add_argument('--output-path', help='Output data path')
    parser.add_argument('--date', help='Processing date')
    
    args, remaining_args = parser.parse_known_args()
    
    # Set sys.argv for the job modules
    sys.argv = [sys.argv[0]] + remaining_args
    
    try:
        if args.job_type == 'ingestion':
            ingestion_main()
        elif args.job_type == 'transformation':
            transformation_main()
        elif args.job_type == 'quality':
            quality_main()
        else:
            raise ValueError(f"Unknown job type: {args.job_type}")
            
    except Exception as e:
        print(f"Job failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
