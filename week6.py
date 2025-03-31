import pickle
import random
import time
import multiprocessing
import os

# Constants for checkpoint files
CHECKPOINT_DIR = "checkpoints"
if not os.path.exists(CHECKPOINT_DIR):
    os.makedirs(CHECKPOINT_DIR)

# A lock to ensure checkpointing is thread-safe
lock = multiprocessing.Lock()

def save_checkpoint(state, process_id):
    """Save the current state to a checkpoint file with a lock to avoid race conditions."""
    with lock:
        try:
            # Read the existing state
            existing_state = load_checkpoint(process_id)
            existing_state.update(state)  # Update the process state with the new state
        except FileNotFoundError:
            existing_state = state
        
        checkpoint_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_{process_id}.pkl")
        with open(checkpoint_file, "wb") as f:
            pickle.dump(existing_state, f)
        print(f"Process {process_id}: Checkpoint saved.")

def load_checkpoint(process_id):
    """Load the last saved checkpoint for a specific process."""
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_{process_id}.pkl")
    try:
        with open(checkpoint_file, "rb") as f:
            state = pickle.load(f)
        print(f"Process {process_id}: Restored from checkpoint.")
        return state
    except FileNotFoundError:
        print(f"Process {process_id}: No checkpoint found. Starting fresh.")
        return {"task_count": 0}

def worker(process_id):
    """Simulate a process performing a task with checkpointing and failure recovery."""
    while True:
        state = load_checkpoint(process_id)  # Always load the latest checkpoint

        if state["task_count"] >= 10:
            print(f"Process {process_id} completed all tasks successfully!")
            break

        if random.random() < 0.2:  # 20% chance of failure
            print(f"Process {process_id} crashed! Reloading from last checkpoint...")
            continue  # Restart loop with reloaded state

        # Perform the task
        state["task_count"] += 1
        print(f"Process {process_id} processing task {state['task_count']}")

        save_checkpoint(state, process_id)
        time.sleep(1)  # Simulate time taken for task

if __name__ == "__main__":
    processes = []
    
    # Start 3 worker processes
    for i in range(3):  
        p = multiprocessing.Process(target=worker, args=(i,))
        processes.append(p)
        p.start()
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
