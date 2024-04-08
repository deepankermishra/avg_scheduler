import os


def get_csv_files(directory):
    return [os.path.join(directory,file) for file in os.listdir(directory) if file.endswith('.csv')]