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

def generate_mixed_utilization_set(count=9, start_task_id=0):
    """Generate tasks with mixed utilization levels"""
    task_set = {
        "type": "mixed_utilization",
        "tasks": []
    }
    
    utilization_patterns = [
        (0.2, 4, "light"),    # Light load: 20% utilization, 4s period
        (0.5, 6, "medium"),   # Medium load: 50% utilization, 6s period
        (0.8, 8, "heavy")     # Heavy load: 80% utilization, 8s period
    ]
    
    tasks_per_pattern = count // len(utilization_patterns)
    
    for pattern_idx, (utilization, period, load_type) in enumerate(utilization_patterns):
        for i in range(tasks_per_pattern):
            execution_time = period * utilization
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "period": period,
                "deadline": period * 0.9,
                "load_type": load_type
            }
            task_set["tasks"].append(task)
    
    return task_set

def generate_varying_priority_set(count=9, start_task_id=0):
    """Generate tasks with varying priorities"""
    task_set = {
        "type": "varying_priority",
        "tasks": []
    }
    
    priority_patterns = [
        (2, 0.3, "high"),     # High priority: 2s period, 30% utilization
        (4, 0.4, "medium"),   # Medium priority: 4s period, 40% utilization
        (8, 0.5, "low")       # Low priority: 8s period, 50% utilization
    ]
    
    tasks_per_pattern = count // len(priority_patterns)
    
    for pattern_idx, (period, utilization, priority) in enumerate(priority_patterns):
        for i in range(tasks_per_pattern):
            execution_time = period * utilization
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "period": period,
                "deadline": period * 0.9,
                "priority": priority
            }
            task_set["tasks"].append(task)
    
    return task_set

def generate_burst_task_set(count=9, start_task_id=0):
    """Generate burst tasks"""
    task_set = {
        "type": "burst",
        "tasks": []
    }
    
    burst_patterns = [
        (0.5, 3, "rapid"),     # Rapid bursts: 0.5s execution, 3s period
        (2.0, 6, "medium"),    # Medium bursts: 2s execution, 6s period
        (4.0, 12, "heavy")     # Heavy bursts: 4s execution, 12s period
    ]
    
    tasks_per_pattern = count // len(burst_patterns)
    
    for pattern_idx, (execution_time, period, burst_type) in enumerate(burst_patterns):
        for i in range(tasks_per_pattern):
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "period": period,
                "deadline": period * 0.8,
                "burst_type": burst_type
            }
            task_set["tasks"].append(task)
    
    return task_set

def save_task_sets_to_json(filename="task_sets.json", tasks_per_set=9):
    """Generate one set of each type for testing with different scheduling algorithms"""
    task_sets = {
        "task_sets": [
            generate_mixed_utilization_set(count=tasks_per_set, start_task_id=0),
            generate_varying_priority_set(count=tasks_per_set, start_task_id=tasks_per_set),
            generate_burst_task_set(count=tasks_per_set, start_task_id=tasks_per_set*2)
        ]
    }
    
    with open(filename, 'w') as f:
        json.dump(task_sets, f, indent=4)
    
    print(f"Generated task sets for testing with different scheduling algorithms")

if __name__ == "__main__":
    save_task_sets_to_json() 