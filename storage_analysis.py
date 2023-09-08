import os
import time
import multiprocessing
import sys
# import shutil

# Suppress warning
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd

# Limit the number of threads
max_threads = 100

def debug_print(return_data):
    print(f'{round(return_data["size"] / 1024 / 1024 / 1024, 3)} GB -> {return_data["filecount"]} files')

def single_thread(dirname, return_data, lock):
    data = {
        'size': 0,
        'filecount': 0
    }

    temp = {
        'size': 0,
        'filecount': 0
    }

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
        'directory': dirname,
        'size': data['size'],
        'filecount': data['filecount'],
        'date_modified': date_modified
    }

    with lock:
        return_data['size'] += temp['size']
        return_data['filecount'] += temp['filecount']

        # Append row to dataframe
        return_data['df'] = return_data['df'].append(output_row, ignore_index=True)

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
    
    # Create a thread for each directory and wait for them to finish
    threads = []

    threads_started = 0

    print(f'Total Dirs: {len(dirs)}')
    finished = 0

    for d in dirs:
        p = multiprocessing.Process(target=single_thread, args=(d, return_data, lock,))
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

    output_columns = ['directory', 'size', 'filecount', 'date_modified']

    # Creating a dataframe
    df = pd.DataFrame(columns = output_columns)
    return_data['df'] = df

    # Create a lock
    lock = multiprocessing.Lock()

    start = time.time()
    multi_threads(dirname, return_data, lock)
    end = time.time()

    # Print size in GB
    print(f'Size: {round(return_data["size"] / 1024 / 1024 / 1024, 3)} GB')
    # Print size in MB
    print(f'Size: {round(return_data["size"] / 1024 / 1024, 3)} MB')
    print('Filecount: ' + str(return_data['filecount']))
    print('Time: ' + str(round(end - start, 2)) + 's')

    # Write dataframe to csv
    return_data['df'].to_csv('output.csv', index=False)
