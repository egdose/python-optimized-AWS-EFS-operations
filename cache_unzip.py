import os
import time
import multiprocessing
import sys
import shutil

# Limit the number of threads
max_threads = 100

FILE_LOGGING = 0

def log(str):
    if FILE_LOGGING:
        with open('logs.txt', 'a') as f:
            f.write(str + '\n')
    print(str)

def debug_print(return_data, dirname):
    log(f'Dirs Extracted: {return_data["dircount"]} ({dirname})')

def dearchive_directory(dirname, return_data, lock):
    # Dearchive dirname
    shutil.unpack_archive(dirname + '.zip', dirname, 'zip')

    with lock:
        return_data['dircount'] += 1

    debug_print(return_data, dirname)

def dearchive_directory_mp(dirname, return_data, lock):    
    # Get all zip files in current directory
    dirs = [os.path.join(dirname, f.replace('.zip', '')) for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f)) and f.endswith('.zip')]
    
    # Create a thread for each directory and wait for them to finish
    threads = []

    # Check if number of directories is greater than max_threads
    if len(dirs) > max_threads:
        # log(f'Total Chunks: {len(dirs)/max_threads}')
        threads_started = 0
        log(f'Total Dirs: {len(dirs)}')
        finished = 0
        for d in dirs:
            p = multiprocessing.Process(target=dearchive_directory, args=(d, return_data, lock,))
            p.start()
            threads_started += 1
            threads.append(p)

            # Check count of threads
            while len(threads) >= max_threads:
                for p in threads:
                    if not p.is_alive():
                        p.join()
                        finished += 1
                        log(f'{finished}/{len(dirs)} threads finished...')
                        threads.remove(p)
                
                time.sleep(0.1)
            
        for p in threads:
            p.join()
            finished += 1
            log(f'{finished}/{len(dirs)} threads finished...')
    
        log(f'Threads Started: {threads_started}')
    else:
        for d in dirs:
            p = multiprocessing.Process(target=dearchive_directory, args=(d, return_data, lock,))
            p.start()
            threads.append(p)
        
        log(f'Waiting for {len(threads)} threads to finish...')

        finished = 0

        for p in threads:
            p.join()
            finished += 1
            log(f'{finished}/{len(threads)} threads finished...')

    # Delete all the zips in the directory
    for d in dirs:
        os.remove(d + '.zip')

if __name__ == '__main__':
    # Get zip name from args
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]) and sys.argv[1] != "/" and sys.argv[1].endswith(".zip"):
        zipname = sys.argv[1]
    else:
        log(f'Usage: {sys.argv[0]} <zipname>')

        sys.exit(1)

    manager = multiprocessing.Manager()
    return_data = manager.dict()

    return_data['dircount'] = 0

    # Create a lock
    lock = multiprocessing.Lock()

    start = time.time()
    # Extract the main zip
    shutil.unpack_archive(zipname, zipname.replace('.zip', ''), 'zip')
    dearchive_directory_mp(zipname.replace('.zip', ''), return_data, lock)
    end = time.time()

    # Print number of directories deleted
    log(f'Directories archived: {return_data["dircount"]}')
    log('Time: ' + str(round(end - start, 2)) + 's')
