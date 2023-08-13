import os
import time
import multiprocessing
import sys
import shutil

# Limit the number of threads
max_threads = 100

def debug_print(return_data, dirname):
    print(f'Dirs Extracted: {return_data["dircount"]} ({dirname})')

def dearchive_directory(dirname, return_data, lock):
    # Dearchive dirname
    os.system(f'unzip -q -o {dirname}.zip -d {dirname}')

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
        # print(f'Total Chunks: {len(dirs)/max_threads}')
        threads_started = 0
        print(f'Total Dirs: {len(dirs)}')
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
            p = multiprocessing.Process(target=dearchive_directory, args=(d, return_data, lock,))
            p.start()
            threads.append(p)
        
        print(f'Waiting for {len(threads)} threads to finish...')

        finished = 0

        for p in threads:
            p.join()
            finished += 1
            print(f'{finished}/{len(threads)} threads finished...')

    # Delete all the zips in the directory
    for d in dirs:
        os.remove(d + '.zip')

if __name__ == '__main__':
    # Get zip name from args
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]) and sys.argv[1] != "/" and sys.argv[1].endswith(".zip"):
        zipname = sys.argv[1]
    else:
        print(f'Usage: {sys.argv[0]} <zipname>')

        sys.exit(1)

    manager = multiprocessing.Manager()
    return_data = manager.dict()

    return_data['dircount'] = 0

    # Create a lock
    lock = multiprocessing.Lock()

    start = time.time()
    # Extract the main zip
    os.system(f'unzip -o {zipname} -d {zipname.replace(".zip", "")}')
    dearchive_directory_mp(zipname.replace('.zip', ''), return_data, lock)
    end = time.time()

    # Print number of directories deleted
    print(f'Directories archived: {return_data["dircount"]}')
    print('Time: ' + str(round(end - start, 2)) + 's')
