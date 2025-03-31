import random
import json
import time

def generate_mixed_utilization_set(count=18, start_task_id=0):
    """
    Generate tasks with mixed utilization levels.
    A balanced mix of light, medium, and heavy tasks.
    """
    task_properties = []
    
    # Define execution patterns for different load types
    execution_patterns = [
        (0.8, "light"),    # Light load: 0.8s execution time
        (3.0, "medium"),   # Medium load: 3.0s execution time
        (6.4, "heavy")     # Heavy load: 6.4s execution time
    ]
    
    tasks_per_pattern = count // len(execution_patterns)
    
    # Generate task properties without assigning IDs yet
    for pattern_idx, (execution_time, load_type) in enumerate(execution_patterns):
        for i in range(tasks_per_pattern):
            # Calculate a generous deadline (3x execution time)
            deadline = execution_time * 5.0
            
            # Store properties without task_id
            task_properties.append({
                "execution_time": execution_time,
                "deadline": deadline,
                "load_type": load_type
            })
    
    # Shuffle the properties
    random.shuffle(task_properties)
    
    # Create task set with ordered IDs but shuffled properties
    task_set = {
        "type": "mixed_utilization",
        "tasks": []
    }
    
    for i, properties in enumerate(task_properties):
        task = properties.copy()
        task["task_id"] = f"T{start_task_id + i}"
        task_set["tasks"].append(task)
    
    return task_set

def generate_deadline_intensity_set(count=18, start_task_id=0):
    """
    Generate tasks with varying deadline intensity.
    Tasks have similar execution times but different deadline constraints.
    """
    task_properties = []
    
    # Different deadline intensity levels with increased deadline factors
    deadline_patterns = [
        (2.0, 5.0, "tight"),     # Increased from 1.5× to 2.5×
        (2.0, 10.0, "moderate"),  # Kept at 5.0×
        (2.0, 15.0, "relaxed")   # Increased from 5.0× to 10.0×
    ]
    
    tasks_per_pattern = count // len(deadline_patterns)
    
    # Generate task properties without assigning IDs yet
    for pattern_idx, (execution_time, deadline_factor, intensity) in enumerate(deadline_patterns):
        for i in range(tasks_per_pattern):
            # Add a small random variation to make it more realistic
            actual_exec_time = execution_time * random.uniform(0.9, 1.1)
            deadline = actual_exec_time * deadline_factor
            
            # Map deadline intensity to load type for consistent analysis
            if intensity == "tight":
                load_type = "heavy"
            elif intensity == "moderate":
                load_type = "medium"
            else:  # relaxed
                load_type = "light"
            
            # Store properties without task_id
            task_properties.append({
                "execution_time": actual_exec_time,
                "deadline": deadline,
                "load_type": load_type,
                "deadline_intensity": intensity
            })
    
    # Shuffle the properties
    random.shuffle(task_properties)
    
    # Create task set with ordered IDs but shuffled properties
    task_set = {
        "type": "deadline_intensity",
        "tasks": []
    }
    
    for i, properties in enumerate(task_properties):
        task = properties.copy()
        task["task_id"] = f"T{start_task_id + i}"
        task_set["tasks"].append(task)
    
    return task_set

def save_task_sets_to_json(filename="task_sets.json", tasks_per_set=18):
    """Generate task sets for testing different scheduling algorithms"""
    # Generate the task sets (already shuffled internally)
    mixed_utilization = generate_mixed_utilization_set(count=tasks_per_set, start_task_id=0)
    deadline_intensity = generate_deadline_intensity_set(count=tasks_per_set, start_task_id=tasks_per_set)
    
    # Combine into final structure
    task_sets = {
        "task_sets": [
            mixed_utilization,
            deadline_intensity
        ]
    }
    
    # Save to JSON file
    with open(filename, 'w') as f:
        json.dump(task_sets, f, indent=4)
    
    print(f"Generated {len(task_sets['task_sets'])} task sets with {tasks_per_set} tasks per set:")
    print("1. Mixed Utilization: Balanced mix of light/medium/heavy tasks (shuffled order)")
    print("2. Deadline Intensity: Tasks with similar execution times but varied deadline constraints (shuffled order)")
    print("Task IDs remain in sequential order, but task properties are randomly mixed")

if __name__ == "__main__":
    save_task_sets_to_json(tasks_per_set=21) 