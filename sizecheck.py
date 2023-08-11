import os
import time
import multiprocessing
import sys
# import shutil

# Limit the number of threads
max_threads = 100

def debug_print(return_data):
    print(f'{round(return_data["size"] / 1024 / 1024 / 1024, 3)} GB -> {return_data["filecount"]} files')

def get_sizes_single(dirname, return_data, lock):
    data = {
        'size': 0,
        'filecount': 0
    }

    counter = 0
    for root, dirs, files in os.walk(dirname):
        # for d in dirs:
        #     print(d)
        for f in files:
            try:
                data['size'] += os.path.getsize(os.path.join(root, f))
                data['filecount'] += 1
            except:
                print(f'Error with file: {f}')
            
            counter += 1
            
            if counter == 10000:
                with lock:
                    return_data['size'] += data['size']
                    return_data['filecount'] += data['filecount']
                counter = 0
                data['size'] = 0
                data['filecount'] = 0
                
                debug_print(return_data)
    
    with lock:
        return_data['size'] += data['size']
        return_data['filecount'] += data['filecount']

    # print(return_data['filecount'])


def get_sizes_filecount(dirname, return_data, lock):
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

    # Check if number of directories is greater than max_threads
    if len(dirs) > max_threads:
        # print(f'Total Chunks: {len(dirs)/max_threads}')
        threads_started = 0
        print(f'Total Dirs: {len(dirs)}')
        finished = 0
        for d in dirs:
            p = multiprocessing.Process(target=get_sizes_single, args=(d, return_data, lock,))
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
    else:
        for d in dirs:
            p = multiprocessing.Process(target=get_sizes_single, args=(d, return_data, lock,))
            p.start()
            threads.append(p)
        
        print(f'Waiting for {len(threads)} threads to finish...')

        finished = 0

        for p in threads:
            p.join()
            finished += 1
            print(f'{finished}/{len(threads)} threads finished...')

    with lock:
        return_data['size'] += data['size']
        return_data['filecount'] += data['filecount']

if __name__ == '__main__':
    # Get directory from args
    if len(sys.argv) > 1:
        dirname = sys.argv[1]
    else:
        dirname = '/'

    manager = multiprocessing.Manager()
    return_data = manager.dict()

    return_data['size'] = 0
    return_data['filecount'] = 0

    # Create a lock
    lock = multiprocessing.Lock()

    start = time.time()
    get_sizes_filecount(dirname, return_data, lock)
    end = time.time()

    # Print size in GB
    print(f'Size: {round(return_data["size"] / 1024 / 1024 / 1024, 3)} GB')
    # Print size in MB
    print(f'Size: {round(return_data["size"] / 1024 / 1024, 3)} MB')
    print('Filecount: ' + str(return_data['filecount']))
    print('Time: ' + str(round(end - start, 2)) + 's')
