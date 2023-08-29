# python-optimized-AWS-EFS-operations
 Utility functions for performing optimized/faster storage operations using python multiprocessing for specific use cases.

# Use Cases

## Cache Operations

Cache operations are very slow in EFS if using built-in posix functions. This is because EFS is a network file system and the latency is high. This can be improved by using multiprocessing to perform such system calls in parallel.

### 1. Check Size

### 2. Delete

### 3. Zip

### 4. Unzip
