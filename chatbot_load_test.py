import concurrent.futures
import time
import requests

URL = "http://localhost:8000/chat"
NUM_REQUESTS = 250
CONCURRENCY = 25

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=CONCURRENCY, pool_maxsize=CONCURRENCY)
session.mount("http://", adapter)

questions = [
    "What is Ray framework?",
    "Tell me a fun fact about space.",
    "Explain Kubernetes in one sentence.",
    "What's the capital of France?",
    "Give me a motivational quote.",
]


def send_request(i):
    start = time.time()
    try:
        r = session.post(URL, json={"message": questions[i % len(questions)]}, timeout=60)
        elapsed = time.time() - start
        return i, r.status_code, elapsed
    except Exception as e:
        return i, f"ERROR: {e}", time.time() - start


if __name__ == "__main__":
    print(f"Sending {NUM_REQUESTS} requests with concurrency={CONCURRENCY}...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_request, i) for i in range(NUM_REQUESTS)]
        for future in concurrent.futures.as_completed(futures):
            i, status, elapsed = future.result()
            print(f"[{i}] status={status} took={elapsed:.2f}s")

    print("Load test complete.")
