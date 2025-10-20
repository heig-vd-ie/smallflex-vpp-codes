

# %%

from tqdm.auto import tqdm
import time
# --- Configuration ---
NUM_OUTER_TASKS = 3
NUM_INNER_STEPS = 5

# --- Nested Progress Bar Logic ---

# Create the outer progress bar
with tqdm(total=NUM_OUTER_TASKS, desc='Outer Process', position=0, leave=True) as pbar_outer:
    for i in range(NUM_OUTER_TASKS):
        # Create the inner progress bar (will show below the outer one)
        # 'position=1' ensures it appears on the second line.
        # 'leave=False' is the key part: it makes the bar disappear when it's finished.
        with tqdm(total=NUM_INNER_STEPS, desc=f'Subtask {i+1}', position=1, leave=False) as pbar_inner:
            for j in range(NUM_INNER_STEPS):
                # Simulate a subtask step
                time.sleep(0.1) # Simulate some work being done
                pbar_inner.update(1) # Update the inner progress bar

        # Update the outer progress bar after all inner steps are done
        pbar_outer.update(1)

print("\nAll processes complete! 🎉")