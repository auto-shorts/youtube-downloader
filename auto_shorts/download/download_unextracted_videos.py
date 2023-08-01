from auto_shorts.download.s3 import get_extracted_video_ids, download_files
from auto_shorts.download.db._postgres import get_video_ids_and_buckets_not_in_list

if __name__ == '__main__':
    extracted_vids = get_extracted_video_ids()
    unextracted_vids = get_video_ids_and_buckets_not_in_list(extracted_vids)
    download_files([s3_path for row in unextracted_vids for s3_path in row])