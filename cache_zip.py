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
    log(f'Dirs Archived: {return_data["dircount"]} ({dirname})')

def archive_directory(dirname, return_data, lock):
    # Archive dirname
    shutil.make_archive(dirname, 'zip', dirname)

    with lock:
        return_data['dircount'] += 1

    debug_print(return_data, dirname)

def archive_directory_mp(dirname, return_data, lock):    
    # Get all the directories in the current directory
    dirs = [os.path.join(dirname, d) for d in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, d))]
    
    # Create a thread for each directory and wait for them to finish
    threads = []

    # Check if number of directories is greater than max_threads
    if len(dirs) > max_threads:
        # log(f'Total Chunks: {len(dirs)/max_threads}')
        threads_started = 0
        log(f'Total Dirs: {len(dirs)}')
        finished = 0
        for d in dirs:
            p = multiprocessing.Process(target=archive_directory, args=(d, return_data, lock,))
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
            p = multiprocessing.Process(target=archive_directory, args=(d, return_data, lock,))
            p.start()
            threads.append(p)
        
        log(f'Waiting for {len(threads)} threads to finish...')

        finished = 0

        for p in threads:
            p.join()
            finished += 1
            log(f'{finished}/{len(threads)} threads finished...')

    # Get all the zip files in current directory
    zips = [os.path.join(dirname, f) for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f)) and f.endswith('.zip')]

    # Create temp directory if it doesn't exist
    if not os.path.isdir(os.path.join(dirname, 'temp')):
        os.mkdir(os.path.join(dirname, 'temp'))

    # Move all zip files to temp directory
    for z in zips:
        shutil.move(z, os.path.join(dirname, 'temp'))
    
    log('Zips moved to temp directory...')

    # Zip temp directory
    shutil.make_archive(dirname, 'zip', os.path.join(dirname, 'temp'))

    log('Temp directory zipped...')

    # Delete temp directory
    shutil.rmtree(os.path.join(dirname, 'temp'))

    log('Temp directory deleted...')

if __name__ == '__main__':
    # Get directory from args
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]) and sys.argv[1] != "/":
        dirname = sys.argv[1]
    else:
        log(f'Usage: {sys.argv[0]} <dirname>')

        sys.exit(1)

    manager = multiprocessing.Manager()
    return_data = manager.dict()

    return_data['dircount'] = 0

    # Create a lock
    lock = multiprocessing.Lock()

    start = time.time()
    archive_directory_mp(dirname, return_data, lock)
    end = time.time()

    # Print number of directories deleted
    log(f'Directories archived: {return_data["dircount"]}')
    log('Time: ' + str(round(end - start, 2)) + 's')
