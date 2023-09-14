import os
import time
import multiprocessing
import sys
from datetime import datetime
# import shutil

# Suppress warning
import warnings
warnings.filterwarnings("ignore")

import pandas as pd

# Limit the number of threads
max_threads = 100

# Date format
date_format = '%Y-%m-%d %H:%M:%S'

def debug_print(return_data):
    print(f'{round(return_data["size"] / 1024 / 1024 / 1024, 3)} GB -> {return_data["filecount"]} files')

def single_thread(dirname, return_data, lock, parent_dirname):
    data = {
        'size': 0,
        'filecount': 0
    }

    temp = {
        'size': 0,
        'filecount': 0
    }

    # Temp
    if dirname.split(parent_dirname)[1].replace('/', '').replace('\\', '') == 'httpcache':
        print(f'Skipping {dirname}')
        return

    # Get date modified of this directory
    date_modified = os.path.getmtime(dirname)

    counter = 0
    for root, dirs, files in os.walk(dirname):
        # for d in dirs:
        #     print(d)
        for f in files:
            try:
                temp_size = os.path.getsize(os.path.join(root, f))
                
                data['size'] += temp_size
                data['filecount'] += 1

                temp['size'] += temp_size
                temp['filecount'] += 1
            except:
                print(f'Error with file: {f}')
            
            counter += 1
            
            if counter == 10000:
                with lock:
                    return_data['size'] += temp['size']
                    return_data['filecount'] += temp['filecount']
                counter = 0
                temp['size'] = 0
                temp['filecount'] = 0
                
                debug_print(return_data)
    
    # Create output row
    output_row = {
        'parent_directory': parent_dirname,
        'directory': dirname.split(parent_dirname)[1],
        'size': round(data["size"] / 1024 / 1024, 3), # MB
        'filecount': data['filecount'],
        'date_modified': datetime.fromtimestamp(date_modified).strftime(date_format) # Convert to date
    }

    with lock:
        return_data['size'] += temp['size']
        return_data['filecount'] += temp['filecount']

        # Add row to dataframe using concat
        return_data['df'] = pd.concat([return_data['df'], pd.DataFrame([output_row])], ignore_index=True)

        # Write dataframe
        return_data['df'].to_csv('temp_no_cache.csv', index=False)

    # print(return_data['filecount'])


def multi_threads(dirname, return_data, lock):
    data = {
        'size': 0,
        'filecount': 0
    }
    
    # Get all the directories in the current directory
    dirs = [os.path.join(dirname, d) for d in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, d))]

    # Get all the files in the current directory
    files = [os.path.join(dirname, f) for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]

    # Get the size of all the files in the current directory
    for f in files:
        try:
            data['size'] += os.path.getsize(f)
            data['filecount'] += 1
        except:
            print(f'Error with file: {f}')

    # Get date modified of this directory
    date_modified = os.path.getmtime(dirname)

    # Create output row
    output_row = {
        'parent_directory': dirname,
        'directory': '.',
        'size': round(data["size"] / 1024 / 1024, 3), # MB
        'filecount': data['filecount'],
        'date_modified': datetime.fromtimestamp(date_modified).strftime(date_format) # Convert to date
    }

    with lock:
        # Add row to dataframe using concat
        return_data['df'] = pd.concat([return_data['df'], pd.DataFrame([output_row])], ignore_index=True)

        # Write dataframe
        return_data['df'].to_csv('temp_no_cache.csv', index=False)
    
    # Create a thread for each directory and wait for them to finish
    threads = []

    threads_started = 0

    print(f'Total Dirs: {len(dirs)}')
    finished = 0

    for d in dirs:
        p = multiprocessing.Process(target=single_thread, args=(d, return_data, lock, dirname,))
        p.start()
        threads_started += 1
        threads.append(p)

        # Check count of threads
        while len(threads) >= max_threads:
            for p in threads:
                if not p.is_alive():
                    p.join()
                    finished += 1
                    print(f'{finished}/{len(dirs)} threads finished...')
                    threads.remove(p)
            
            time.sleep(0.1)
        
    for p in threads:
        p.join()
        finished += 1
        print(f'{finished}/{len(dirs)} threads finished...')

    print(f'Threads Started: {threads_started}')

    with lock:
        return_data['size'] += data['size']
        return_data['filecount'] += data['filecount']

if __name__ == '__main__':
    # Get directory from args
    if len(sys.argv) > 1:
        dirname = sys.argv[1]
    else:
        # Get current directory
        dirname = os.getcwd()

    manager = multiprocessing.Manager()
    return_data = manager.dict()

    return_data['size'] = 0
    return_data['filecount'] = 0

    output_columns = ['parent_directory', 'directory', 'size', 'filecount', 'date_modified']

    # Creating a dataframe
    df = pd.DataFrame(columns = output_columns)
    return_data['df'] = df

    # Create a lock
    lock = multiprocessing.Lock()
    
    # Get all the directories in the current directory
    dirs = [os.path.join(dirname, d) for d in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, d))]

    # Get all the files in the current directory
    files = [os.path.join(dirname, f) for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]

    start = time.time()
    # Get the size of all the files in the current directory
    for f in files:
        try:
            return_data['size'] += os.path.getsize(f)
            return_data['filecount'] += 1
        except:
            print(f'Error with file: {f}')

    for d in dirs:
        multi_threads(d, return_data, lock)

    end = time.time()

    # Print size in GB
    print(f'Size: {round(return_data["size"] / 1024 / 1024 / 1024, 3)} GB')
    # Print size in MB
    print(f'Size: {round(return_data["size"] / 1024 / 1024, 3)} MB')
    print('Filecount: ' + str(return_data['filecount']))
    print('Time: ' + str(round(end - start, 2)) + 's')

    # Write dataframe to csv
    return_data['df'].to_csv('output_no_cache.csv', index=False)
