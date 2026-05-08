# Producer Consumer Synchronization Simulator

Operating Systems project implementing the classic Producer-Consumer problem using multithreading in C.

## Features

- Multiple Producers (5 threads)
- Multiple Consumers (5 threads)
- Dynamic buffer size input
- Circular buffer implementation
- Synchronization using:
  - POSIX Threads (pthreads)
  - Semaphores
  - Mutex locks
  - Condition Variables
- Fair turn-based scheduling for producers and consumers
- Real-time buffer state logging
- Thread production/consumption statistics
- Graceful shutdown after execution

## Concepts Used

- Process Synchronization
- Critical Section Problem
- Mutual Exclusion
- Producer Consumer Problem
- Bounded Buffer
- Deadlock Prevention
- Thread Coordination

## Technologies

- C Language
- GCC Compiler
- POSIX Threads
- Linux / Unix Environment

## How to Compile

```bash
gcc Project.c -o project -lpthread
