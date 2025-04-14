#!usr/bin/python3

import tarfile
import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor # leverage mutli-core processes for parallel compute


def convert_json_to_parquet(json_path, parquet_path):
    try:
        print(f"Converting {json_path} to {parquet_path}...")
        df = pd.read_json(json_path, lines=True)
        df.to_parquet(parquet_path, index=False)
        print(f"Saved {parquet_path}")
    except Exception as e:
        print(f"❌ Failed to convert {json_path} → {parquet_path}: {e}")


def json_2_parquet() -> None:
    """
    Convert Yelp JSON datasets to Parquet format using multiprocessing.
    """
    extract_path = '/media/oem/onetbsamdot/datasets/yelp_10Mar2025/Yelp-JSON/Yelp JSON/'
    parquet_dir = "./data"

    os.makedirs(extract_path, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)

    print("Files in extraction directory:", os.listdir(extract_path))

    # Define JSON input paths
    business_file = os.path.join(extract_path, 'yelp_academic_dataset_business.json')
    review_file   = os.path.join(extract_path, 'yelp_academic_dataset_review.json')
    checkin_file  = os.path.join(extract_path, 'yelp_academic_dataset_checkin.json')
    tip_file      = os.path.join(extract_path, 'yelp_academic_dataset_tip.json')
    user_file     = os.path.join(extract_path, 'yelp_academic_dataset_user.json')
    photo_file    = os.path.join(extract_path, 'yelp_academic_dataset_photos.json')

    tasks = [
        (business_file, os.path.join(parquet_dir, 'business.parquet')),
        (review_file, os.path.join(parquet_dir, 'review.parquet')),
        (checkin_file, os.path.join(parquet_dir, 'checkin.parquet')),
        (tip_file, os.path.join(parquet_dir, 'tip.parquet')),
        (user_file, os.path.join(parquet_dir, 'user.parquet')),
        (photo_file, os.path.join(parquet_dir, 'photo.parquet')),
    ]

    # Parallel processing
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(convert_json_to_parquet, src, dest) for src, dest in tasks]
        for future in futures:
            future.result()

    print("✅ All conversions completed!")

















if __name__ == '__main__':
    ...