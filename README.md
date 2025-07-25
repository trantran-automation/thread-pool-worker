# thread-pool-worker
An example about thread pool to increase process speed

# How it work
This processor use input queue and output queue to in and out job
```python
MAX_WORKER:int = 10
input_queue = queue.Queue()
output_queue = queue.Queue()
```

This function use to commit job to thread pool and add watch dog to check for timeout, exception, return result
```python
def dispatcher():
    """
        Process task with thread pool
    """
    with _cf.ThreadPoolExecutor(max_workers=MAX_WORKER) as thread_pool:
        processing_worker = {} # store running worker handler
        # main loop of pool handle that keep take new job and push 
        # to thread pool
        while True:
            try:
                job = input_queue.get(timeout=1)
                future = thread_pool.submit(worker,job)

                # new watch dog run to monitor timeout exception for each thread
                def watchdog():
                    try:
                        result = future.result(timeout=5.0)
                        output_queue.put( (result,'OK' ) )
                    except _cf.TimeoutError:
                        future.cancel()
                        output_queue.put( ( None,"TIMEOUT") )
                    except Exception as e:
                        output_queue.put( (None,str(e) ))

                threading.Thread(target=watchdog,daemon=True).start()

            except queue.Empty:
                # just pass if queue input timeout
                pass
```

Worker processor where real process happen for data here is only a fake process, change it with your real process, real data
```python
def worker(job):
    # fake fail exception in code
    if random.uniform(0,1) < 0.2:
        raise ValueError("Fake crash")
    time.sleep(random.uniform(0.2,10))
    return job
```

In main run all thread as daemon, use input value with your real data. 1 thread for data input, 1 thead for data out put, 1 thread to run dispatcher.
```python
def main():
    def job_send():
        while True:
            # simulator task task assignment
            input_queue.put("simulate data")
            time.sleep(1)

    def result_take():
        while True:
            try:
                output =  output_queue.get(timeout=1)
                print(output)
            except queue.Empty:
                pass

    
    job_send_thread = threading.Thread(target=job_send,daemon=True).start()
    result_take_thread = threading.Thread(target=result_take,daemon=True).start()
    dispatcher_thread = threading.Thread(target=dispatcher,daemon=True).start()

    # main loop keep awake
    while True:
        time.sleep(1)
    
    print("END")

```