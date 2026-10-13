import time
import ray

ray.init(address="auto")


@ray.remote(num_cpus=1)
def cpu_task(task_id: int):
    start = time.time()
    total = 0
    for i in range(20_000_000):
        total += i * i
    duration = time.time() - start
    return {"task_id": task_id, "result": total, "seconds": round(duration, 2)}


if __name__ == "__main__":
    num_tasks = 100
    print(f"Submitting {num_tasks} tasks...")
    futures = [cpu_task.remote(i) for i in range(num_tasks)]

    completed = 0
    while futures:
        done, futures = ray.wait(futures, num_returns=min(10, len(futures)), timeout=5)
        for ref in done:
            result = ray.get(ref)
            completed += 1
            print(f"[{completed}/{num_tasks}] task_id={result['task_id']} took {result['seconds']}s")

    print("All tasks completed.")
