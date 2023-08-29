import os
import time
import multiprocessing
import sys
import shutil

# Limit the number of threads
max_threads = 100

def debug_print(return_data):
    print(f'Dirs Deleted: {return_data["dircount"]}')

def get_sizes_single(dirname, return_data, lock):

    # shutil.rmtree(dirname)

    counter = 0
    
    # Get dirs in current directory
    dirs = [os.path.join(dirname, d) for d in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, d))]

    print(f'Array list created for {dirname}')

    for d in dirs:
        # shutil.rmtree(d)
        # os.rmdir(d)
        # Use rm -rf
        os.system(f'rm -rf {d}')

        # print(f'Deleting {d}...')

        counter += 1

        if counter >= 800:
            with lock:
                return_data['dircount'] += counter
            
            debug_print(return_data)

            counter = 0

    with lock:
        return_data['dircount'] += counter

def get_sizes_filecount(dirname, return_data, lock):    
    # Get all the directories in the current directory
    dirs = [os.path.join(dirname, d) for d in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, d))]

    # Get all the files in the current directory
    files = [os.path.join(dirname, f) for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]

    # Delete all the files in current directory
    for f in files:
        os.remove(f)
    
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

if __name__ == '__main__':
    # Get directory from args
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]) and sys.argv[1] != "/":
        dirname = sys.argv[1]
    else:
        print(f'Usage: {sys.argv[0]} <dirname>')

        sys.exit(1)

    manager = multiprocessing.Manager()
    return_data = manager.dict()

    return_data['dircount'] = 0

    # Create a lock
    lock = multiprocessing.Lock()

    start = time.time()
    get_sizes_filecount(dirname, return_data, lock)
    end = time.time()

    # Delete the directory
    shutil.rmtree(dirname)

    # Print number of directories deleted
    print(f'Dirs Deleted: {return_data["dircount"]}')
    print('Time: ' + str(round(end - start, 2)) + 's')
