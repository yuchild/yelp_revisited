#!usr/bin/python3

import tarfile
import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor # leverage mutli-core processes for parallel compute

def json_2_parquet() -> None:
    """

    Function pases raw json files into pandas and saves as parquet for later use in spark
    
    """
    # Define the path to the tar file and an extraction directory
    tar_path = '/media/oem/onetbsamdot/datasets/yelp_10Mar2025/Yelp-JSON/Yelp JSON/yelp_dataset.tar'
    extract_path = '/media/oem/onetbsamdot/datasets/yelp_10Mar2025/Yelp-JSON/Yelp JSON/'  # or another directory of your choice
    
    # List the extracted files to verify
    print(os.listdir(extract_path))
    
    # Create the extraction directory if it doesn't exist
    os.makedirs(extract_path, exist_ok=True)
    
    # Create a directory to store the converted Parquet files
    parquet_dir = "../data"
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Function to convert a JSON file to Parquet using Pandas
    def convert_json_to_parquet(json_path, parquet_path):
        print(f"Converting {json_path} to {parquet_path}...")
        # Read the JSON file (JSON Lines mode)
        df = pd.read_json(json_path, lines=True)
        # Write the DataFrame to Parquet (without the index)
        df.to_parquet(parquet_path, index=False)
        print(f"Saved {parquet_path}")
    
    # List the conversion tasks as (source, destination) tuples
    tasks = [
        (business_file, os.path.join(parquet_dir, 'business.parquet')),
        (review_file, os.path.join(parquet_dir, 'review.parquet')),
        (checkin_file, os.path.join(parquet_dir, 'checkin.parquet')),
        (tip_file, os.path.join(parquet_dir, 'tip.parquet')),
        (user_file, os.path.join(parquet_dir, 'user.parquet')),
    ]
    
    # Use a ProcessPoolExecutor with 5 workers to convert files in parallel
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(convert_json_to_parquet, src, dest) for src, dest in tasks]
        # Optionally wait for all tasks to complete
        for future in futures:
            future.result()
    
    print("All conversions completed!")















if __name__ == '__main__':
    ...