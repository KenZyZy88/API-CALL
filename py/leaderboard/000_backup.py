import os
import shutil
from datetime import datetime, timezone

def backup_files(source_folder, destination_folder):
    # Create a backup folder with the current date (in UTC)
    today_utc = datetime.now(timezone.utc)
    backup_day = today_utc.strftime("%Y-%m-%d")
    backup_time = today_utc.strftime("%H-%M")
    backup_folder_path = os.path.join(destination_folder, backup_day, backup_time)
    os.makedirs(backup_folder_path, exist_ok=True)

    # List of files to back up
    files_to_backup = ["leaderboard_account.txt", "leaderboard_breakdown.txt", "leaderboard_subscriberData.txt", "leaderboard_summary.txt", "leaderboard.txt"]

    for filename in files_to_backup:
        source_file_path = os.path.join(source_folder, filename)
        if os.path.exists(source_file_path):
            # Create the backup file name
            timestamp_utc = today_utc.strftime("%y-%m-%d-%H-%M")
            backup_file_name = f"{filename}_{timestamp_utc}.txt"
            backup_file_path = os.path.join(backup_folder_path, backup_file_name)

            # Copy the file to the backup folder
            shutil.copy2(source_file_path, backup_file_path)
            print(f"Backed up {filename} to {backup_file_path}")
        else:
            print(f"File {filename} does not exist in the source folder.")

# Example usage
source_folder_path = "/home/kenny/txt"
destination_folder_path = "/home/kenny/txt/backup/leaderboard"
backup_files(source_folder_path, destination_folder_path)
