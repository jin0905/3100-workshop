import pickle
import random
import time
import multiprocessing

CHECKPOINT_FILE = "checkpoint.pkl"
lock = multiprocessing.Lock()  

def save_checkpoint(state):
    """Save the current state to a checkpoint file with a lock to avoid race conditions."""
    with lock:
        try:
            existing_state = load_checkpoint()
            existing_state.update(state)  
        except FileNotFoundError:
            existing_state = state
        
        with open(CHECKPOINT_FILE, "wb") as f:
            pickle.dump(existing_state, f)
        print(f"Checkpoint saved: {existing_state}")

def load_checkpoint():
    """Load the last saved checkpoint."""
    try:
        with open(CHECKPOINT_FILE, "rb") as f:
            state = pickle.load(f)
        print(f"Restored from checkpoint: {state}")
        return state
    except FileNotFoundError:
        print("No checkpoint found. Starting fresh.")
        return {0: {"task_count": 0}, 1: {"task_count": 0}, 2: {"task_count": 0}}

def worker(process_id):
    """Simulate a process performing a task with checkpointing and failure recovery."""
    while True:
        state = load_checkpoint()  # Always load the latest checkpoint

        if state[process_id]["task_count"] >= 10:
            print(f"Process {process_id} completed all tasks successfully!")
            break

        if random.random() < 0.2:  # 20% chance of failure
            print(f"Process {process_id} crashed! Reloading from last checkpoint...")
            continue  # Restart loop with reloaded state

        # Perform the task
        state[process_id]["task_count"] += 1
        print(f"Process {process_id} processing task {state[process_id]['task_count']}")

        save_checkpoint(state)
        time.sleep(1) 
if __name__ == "__main__":
    processes = []
    for i in range(3):  
        p = multiprocessing.Process(target=worker, args=(i,))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
