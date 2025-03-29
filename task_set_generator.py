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

def generate_mixed_utilization_set(count=18, start_task_id=0):
    """
    Generate tasks with mixed utilization levels for one-time execution.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "mixed_utilization",
        "tasks": []
    }
    
    # Define execution patterns for different load types
    execution_patterns = [
        (0.8, "light"),    # Light load: 0.8s execution time
        (3.0, "medium"),   # Medium load: 3.0s execution time
        (6.4, "heavy")     # Heavy load: 6.4s execution time
    ]
    
    tasks_per_pattern = count // len(execution_patterns)
    
    for pattern_idx, (execution_time, load_type) in enumerate(execution_patterns):
        for i in range(tasks_per_pattern):
            # Calculate a very generous deadline (3x execution time)
            deadline = execution_time * 3.0  # 200% margin
            
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "deadline": deadline,
                "load_type": load_type
            }
            task_set["tasks"].append(task)
    
    return task_set

def generate_varying_priority_set(count=18, start_task_id=0):
    """
    Generate tasks with varying priorities for one-time execution.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "varying_priority",
        "tasks": []
    }
    
    # Priority levels based on execution time and deadline
    priority_patterns = [
        (0.6, 2.0, "high"),    # High priority: Short execution, reasonable deadline
        (2.0, 6.0, "medium"),  # Medium priority: Medium execution, generous deadline
        (4.0, 14.0, "low")     # Low priority: Long execution, very generous deadline
    ]
    
    tasks_per_pattern = count // len(priority_patterns)
    
    for pattern_idx, (execution_time, deadline, priority) in enumerate(priority_patterns):
        for i in range(tasks_per_pattern):
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "deadline": deadline,
                "priority": priority
            }
            task_set["tasks"].append(task)
    
    return task_set

def generate_burst_task_set(count=18, start_task_id=0):
    """
    Generate burst tasks for one-time execution.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "burst",
        "tasks": []
    }
    
    # Burst patterns to test different burst scenarios
    burst_patterns = [
        (0.5, 2.0, "rapid"),     # Rapid bursts: Short execution time
        (2.0, 7.0, "medium"),    # Medium bursts: Medium execution time
        (4.0, 14.0, "heavy")     # Heavy bursts: Long execution time
    ]
    
    tasks_per_pattern = count // len(burst_patterns)
    
    for pattern_idx, (execution_time, deadline, burst_type) in enumerate(burst_patterns):
        for i in range(tasks_per_pattern):
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "deadline": deadline,
                "burst_type": burst_type
            }
            task_set["tasks"].append(task)
    
    return task_set

def generate_rm_optimized_set(count=9, start_task_id=0):
    """
    Generate tasks that challenge RM scheduling for one-time execution.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "rm_optimized",
        "tasks": []
    }
    
    # Mix of short and long execution times
    execution_times = [0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0]
    
    for i in range(count):
        execution_time = random.choice(execution_times)
        # More generous deadline (3x execution time)
        deadline = execution_time * 3.0
        
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": execution_time,
            "deadline": deadline,
            "test_case": "short" if execution_time < 2.0 else "long"
        }
        task_set["tasks"].append(task)
    
    return task_set

def generate_edf_optimized_set(count=9, start_task_id=0):
    """
    Generate tasks that challenge EDF scheduling for one-time execution.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "edf_optimized",
        "tasks": []
    }
    
    # Mix of tasks with varied execution times
    for i in range(count):
        execution_time = random.uniform(1, 4)
        
        # More generous deadline factors
        deadline_factor = random.choice([2.0, 2.5, 3.0])
        deadline = execution_time * deadline_factor
        
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": execution_time,
            "deadline": deadline,
            "deadline_type": "tight" if deadline_factor < 2.5 else "generous" 
        }
        task_set["tasks"].append(task)
    
    return task_set

def generate_rr_optimized_set(count=9, start_task_id=0):
    """
    Generate tasks that challenge RR scheduling for one-time execution.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "rr_optimized",
        "tasks": []
    }
    
    # Mix of short and long execution times to test fairness
    execution_patterns = [
        (0.5, 2.0, "short"),     # Short execution
        (2.0, 6.0, "medium"),    # Medium execution
        (5.0, 15.0, "long")      # Long execution
    ]
    
    tasks_per_pattern = count // len(execution_patterns)
    
    for pattern_idx, (execution_time, deadline, execution_type) in enumerate(execution_patterns):
        for i in range(tasks_per_pattern):
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": execution_time,
                "deadline": deadline,
                "execution_type": execution_type
            }
            task_set["tasks"].append(task)
    
    return task_set

def generate_high_utilization_set(count=9, start_task_id=0):
    """
    Generate tasks with high overall load to test scheduler limits.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "high_utilization",
        "tasks": []
    }
    
    # Total execution time distributed across tasks
    total_execution_time = 15.0  # 15 seconds total execution time
    
    # Distribute execution time across tasks
    exec_times = []
    remaining = total_execution_time
    for i in range(count-1):
        # Random execution time between 0.5 and remaining/2
        exec_time = random.uniform(0.5, remaining/2)
        exec_times.append(exec_time)
        remaining -= exec_time
    exec_times.append(remaining)  # Add remaining execution time
    
    for i, execution_time in enumerate(exec_times):
        # More generous deadline (3x execution time)
        deadline = execution_time * 3.0
        
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": execution_time,
            "deadline": deadline,
            "load_percentage": (execution_time / total_execution_time) * 100
        }
        task_set["tasks"].append(task)
    
    return task_set

def generate_deadline_inversion_set(count=9, start_task_id=0):
    """
    Generate tasks where shorter deadlines have longer execution times.
    Increased overall deadlines to reduce missed deadlines while still maintaining
    the inversion relationship.
    """
    task_set = {
        "type": "deadline_inversion",
        "tasks": []
    }
    
    # Create inversions where shorter deadlines have longer execution times
    base_deadline = 10.0  # Increased base deadline
    for i in range(count):
        # Shorter deadlines for higher indices, but still generous
        deadline = base_deadline - (i * 0.5)
        # Longer execution times for higher indices
        execution_time = 0.5 + (i * 0.3)
        
        task = {
            "task_id": f"T{start_task_id + i}",
            "execution_time": execution_time,
            "deadline": deadline,
            "inversion_level": i
        }
        task_set["tasks"].append(task)
    
    return task_set

def generate_worker_balancing_set(count=21, start_task_id=0):
    """
    Generate tasks to test load balancing across workers.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "worker_balancing",
        "tasks": []
    }
    
    # Three distinct workload profiles - one per worker ideally
    profiles = [
        # Profile 1: Few heavy tasks
        [(8.0, 24.0, "heavy") for _ in range(3)],
        
        # Profile 2: Medium number of medium tasks
        [(3.0, 9.0, "medium") for _ in range(6)],
        
        # Profile 3: Many light tasks
        [(0.5, 1.5, "light") for _ in range(12)]
    ]
    
    task_id_counter = start_task_id
    for profile in profiles:
        for execution_time, deadline, profile_type in profile:
            task = {
                "task_id": f"T{task_id_counter}",
                "execution_time": execution_time,
                "deadline": deadline,
                "profile_type": profile_type
            }
            task_set["tasks"].append(task)
            task_id_counter += 1
    
    return task_set

def generate_diverse_execution_set(count=15, start_task_id=0):
    """
    Generate tasks with diverse execution times to test scheduling fairness.
    Increased deadlines to reduce missed deadlines.
    """
    task_set = {
        "type": "diverse_execution",
        "tasks": []
    }
    
    # Very different execution times with generous deadlines
    execution_patterns = [
        (0.2, 0.8, "very_short"),   # Very short tasks
        (1.0, 3.6, "short"),        # Short tasks
        (3.0, 9.0, "medium"),       # Medium tasks
        (5.0, 15.0, "long"),        # Long tasks
        (8.0, 24.0, "very_long")    # Very long tasks
    ]
    
    tasks_per_pattern = count // len(execution_patterns)
    
    for pattern_idx, (execution_time, deadline, exec_type) in enumerate(execution_patterns):
        for i in range(tasks_per_pattern):
            # Add a small random variation to make it more realistic
            actual_exec_time = execution_time * random.uniform(0.9, 1.1)
            actual_deadline = deadline * random.uniform(0.95, 1.05)
            
            task = {
                "task_id": f"T{start_task_id + pattern_idx * tasks_per_pattern + i}",
                "execution_time": actual_exec_time,
                "deadline": actual_deadline,
                "exec_type": exec_type
            }
            task_set["tasks"].append(task)
    
    return task_set

def save_task_sets_to_json(filename="task_sets.json", tasks_per_set=18):
    """Generate one set of each type for testing with different scheduling algorithms"""
    task_sets = {
        "task_sets": [
            generate_mixed_utilization_set(count=tasks_per_set, start_task_id=0),
            generate_varying_priority_set(count=tasks_per_set, start_task_id=tasks_per_set),
            generate_burst_task_set(count=tasks_per_set, start_task_id=tasks_per_set*2),
            generate_diverse_execution_set(count=tasks_per_set, start_task_id=tasks_per_set*3)
        ]
    }
    
    with open(filename, 'w') as f:
        json.dump(task_sets, f, indent=4)
    
    print(f"Generated task sets ({tasks_per_set} tasks per set) with generous deadlines for one-time execution testing")

if __name__ == "__main__":
    save_task_sets_to_json() 