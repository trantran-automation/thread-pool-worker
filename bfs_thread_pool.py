from collections import defaultdict, deque
from typing import List, Dict, Any
import concurrent.futures as _cf
import queue
import threading
import time
import random
import copy

MAX_WORKER:int = 10
input_queue = queue.Queue()
output_queue = queue.Queue()

graph_data = {
    "A": ["B", "C", "F"],
    "B": ["A", "C", "D", "G"],
    "C": ["A", "B", "D", "E", "H"],
    "D": ["B", "C", "E", "I"],
    "E": ["C", "D", "F", "J"],
    "F": ["A", "E", "G", "K"],
    "G": ["B", "F", "H", "L"],
    "H": ["C", "G", "I", "M"],
    "I": ["D", "H", "J", "N"],
    "J": ["E", "I", "K", "O"],
    "K": ["F", "J", "L", "P"],
    "L": ["G", "K", "M", "Q"],
    "M": ["H", "L", "N", "R"],
    "N": ["I", "M", "O", "S"],
    "O": ["J", "N", "P", "T"],
    "P": ["K", "O", "Q", "U"],
    "Q": ["L", "P", "R", "V"],
    "R": ["M", "Q", "S", "W"],
    "S": ["N", "R", "T", "X"],
    "T": ["O", "S", "U", "Y"],
    "U": ["P", "T", "V", "Z"],
    "V": ["Q", "U", "W", "A"],
    "W": ["R", "V", "X", "B"],
    "X": ["S", "W", "Y", "C"],
    "Y": ["T", "X", "Z", "D"],
    "Z": ["U", "Y", "A", "E"]
}

# DFS that returns all paths from start -> goal
def dfs_paths(input:tuple) -> List[List[str]]:
    start, goal = input
    
    print("Start Stop",start, goal)
    graph = copy.deepcopy(graph_data)
    q = deque([(start, [start])])   # (current, path_so_far)
    seen = {start}

    while q:
        node, path = q.popleft()
        if node == goal:
            return path
        for nbr in graph.get(node, []):
            if nbr not in seen:
                seen.add(nbr)
                q.append((nbr, path + [nbr]))
    return None

def dispatcher():
    """
        Process task with thread pool
    """
    
    # main loop of pool handle that keep take new job and push 
    # to thread pool
    with _cf.ThreadPoolExecutor(max_workers=MAX_WORKER) as thread_pool:
        while True:
            try:
                input = input_queue.get(timeout=1)
                future = thread_pool.submit(dfs_paths,input)

                # new watch dog run to monitor timeout exception for each thread
                def watchdog():
                    try:
                        result = future.result(timeout=10.0)
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

def main():
    def job_send():

        search_list = [
            ('A','Z'),
            ('A','E'),
            ('A','G'),
            ('A','J'),
            ('A','K'),
            ('B','Z'),
            ('B','E'),
            ('B','G'),
            ('B','J'),
            ('B','K'),
            ('C','Z'),
            ('C','E'),
            ('C','G'),
            ('C','J'),
            ('C','K'),
  
        ]
        for search in search_list:
            # simulator task task assignment
            input_queue.put(search)
            time.sleep(0.001)

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

if __name__ == "__main__":
    main()