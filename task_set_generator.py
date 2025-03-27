import random
import json

def generate_harmonic_task_set(count=5, start_task_id=0):
    """Generate a set of harmonic tasks"""
    task_set = {
        "scheduling": "RM",
        "tasks": []
    }
    periods = [2, 4, 8, 16]
    
    for i in range(count):
        period = random.choice(periods)
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": random.uniform(0.1, period / 2),
            "period": period,
            "deadline": period
        }
        task_set["tasks"].append(task)
    
    return task_set

def generate_tight_deadline_task_set(count=5, start_task_id=0):
    """Generate tasks with tight deadlines"""
    task_set = {
        "scheduling": "EDF",
        "tasks": []
    }
    
    for i in range(count):
        period = random.uniform(5, 10)
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": random.uniform(0.5, 2),
            "period": period,
            "deadline": random.uniform(1, 3)  # Tight deadlines
        }
        task_set["tasks"].append(task)
    
    return task_set

def generate_equal_period_task_set(count=5, start_task_id=0):
    """Generate tasks with equal periods"""
    task_set = {
        "scheduling": "RR",
        "tasks": []
    }
    period = 5
    execution_time = 1
    
    for i in range(count):
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": execution_time,
            "period": period,
            "deadline": period
        }
        task_set["tasks"].append(task)
    
    return task_set

def save_task_sets_to_json(filename="task_sets.json", num_sets=3, tasks_per_set=5):
    """Generate multiple task sets and save them to a JSON file"""
    task_sets = {
        "harmonic_sets": [],
        "tight_deadline_sets": [],
        "equal_period_sets": []
    }
    
    task_id_counter = 0
    
    # Generate sets for each type
    for set_num in range(num_sets):
        # Harmonic sets
        harmonic_set = generate_harmonic_task_set(
            count=tasks_per_set,
            start_task_id=task_id_counter
        )
        task_sets["harmonic_sets"].append(harmonic_set)
        task_id_counter += tasks_per_set
        
        # Tight deadline sets
        tight_deadline_set = generate_tight_deadline_task_set(
            count=tasks_per_set,
            start_task_id=task_id_counter
        )
        task_sets["tight_deadline_sets"].append(tight_deadline_set)
        task_id_counter += tasks_per_set
        
        # Equal period sets
        equal_period_set = generate_equal_period_task_set(
            count=tasks_per_set,
            start_task_id=task_id_counter
        )
        task_sets["equal_period_sets"].append(equal_period_set)
        task_id_counter += tasks_per_set
    
    with open(filename, 'w') as f:
        json.dump(task_sets, f, indent=4)
    
    print(f"Generated {num_sets} sets of each type and saved to {filename}")

if __name__ == "__main__":
    save_task_sets_to_json() 