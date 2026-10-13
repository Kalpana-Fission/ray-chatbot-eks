import asyncio
import os
import time
import requests
import ray
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from kubernetes import client, config
from pydantic import BaseModel
from ray import serve

NVIDIA_MODEL = "nvidia/nemotron-3-super-120b-a12b"
NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

fastapi_app = FastAPI()
fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
  <title>Ray Chatbot</title>
  <style>
    body { font-family: sans-serif; max-width: 700px; margin: 40px auto; }
    #log { border: 1px solid #ccc; border-radius: 8px; padding: 12px; height: 420px; overflow-y: auto; margin-bottom: 12px; }
    .msg { margin: 8px 0; }
    .user { color: #0b5; font-weight: bold; }
    .bot { color: #06c; }
    #row { display: flex; gap: 8px; }
    #input { flex: 1; padding: 8px; }
    button { padding: 8px 16px; }
  </style>
</head>
<body>
  <h2>Ray Serve Chatbot (NVIDIA Nemotron)</h2>
  <div id="log"></div>
  <div id="row">
    <input id="input" placeholder="Type a message..." autofocus />
    <button onclick="send()">Send</button>
  </div>
  <script>
    const history = [];
    const log = document.getElementById('log');
    const input = document.getElementById('input');

    function append(role, text) {
      const div = document.createElement('div');
      div.className = 'msg ' + role;
      div.textContent = (role === 'user' ? 'You: ' : 'Bot: ') + text;
      log.appendChild(div);
      log.scrollTop = log.scrollHeight;
    }

    async function send() {
      const message = input.value.trim();
      if (!message) return;
      input.value = '';
      append('user', message);

      const resp = await fetch('/chat', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({message, history}),
      });
      const data = await resp.json();
      if (data.error) {
        append('bot', '[error: ' + data.error + ']');
        return;
      }
      append('bot', data.reply);
      history.push({role: 'user', content: message});
      history.push({role: 'assistant', content: data.reply});
    }

    input.addEventListener('keydown', (e) => { if (e.key === 'Enter') send(); });
  </script>
</body>
</html>
"""


class ChatRequest(BaseModel):
    message: str
    history: list = []


@ray.remote
class MetricsCounter:
    def __init__(self):
        self.ongoing = 0
        self.completed = 0
        self.waiting = 0
        self.replica_last_seen = {}

    def start_waiting(self):
        self.waiting += 1

    def start_request(self, replica_tag):
        self.waiting = max(0, self.waiting - 1)
        self.ongoing += 1
        self.replica_last_seen[replica_tag] = time.time()

    def finish_request(self, replica_tag):
        self.ongoing = max(0, self.ongoing - 1)
        self.completed += 1
        self.replica_last_seen[replica_tag] = time.time()

    def get_stats(self):
        now = time.time()
        active = [t for t, ts in self.replica_last_seen.items() if now - ts < 20]
        return {
            "ongoing": self.ongoing,
            "completed": self.completed,
            "waiting": self.waiting,
            "active_replicas": len(active),
            "replica_ids": active,
        }


def get_metrics_actor():
    try:
        return ray.get_actor("chatbot_metrics_counter")
    except ValueError:
        return MetricsCounter.options(
            name="chatbot_metrics_counter", lifetime="detached", get_if_exists=True
        ).remote()


@serve.deployment(
    ray_actor_options={"num_cpus": 0.5},
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 10,
        "target_ongoing_requests": 2,
        "upscale_delay_s": 5,
        "downscale_delay_s": 30,
    },
)
@serve.ingress(fastapi_app)
class ChatBot:
    def __init__(self):
        self.api_key = os.environ["NVIDIA_API_KEY"]
        self.metrics = get_metrics_actor()
        self.replica_tag = serve.get_replica_context().replica_tag
        try:
            config.load_incluster_config()
            self.k8s = client.CoreV1Api()
        except Exception:
            self.k8s = None

    @fastapi_app.get("/", response_class=HTMLResponse)
    async def home(self):
        return HTML_PAGE

    @fastapi_app.get("/metrics")
    async def metrics_endpoint(self):
        stats = await asyncio.to_thread(ray.get, self.metrics.get_stats.remote())
        return stats

    @fastapi_app.get("/nodes")
    async def nodes_endpoint(self):
        if self.k8s is None:
            return {"error": "k8s client not available"}

        def list_cluster_state():
            nodes = self.k8s.list_node()
            pods = self.k8s.list_namespaced_pod(
                namespace="default", label_selector="ray.io/cluster=ray-sentiment-cluster"
            )
            node_list = []
            for n in nodes.items:
                allocatable_cpu = n.status.allocatable.get("cpu", "?")
                allocatable_mem = n.status.allocatable.get("memory", "?")
                ready = any(
                    c.type == "Ready" and c.status == "True" for c in (n.status.conditions or [])
                )
                node_list.append(
                    {
                        "name": n.metadata.name,
                        "ready": ready,
                        "allocatable_cpu": allocatable_cpu,
                        "allocatable_memory": allocatable_mem,
                        "instance_type": n.metadata.labels.get(
                            "node.kubernetes.io/instance-type", "?"
                        ),
                    }
                )

            pod_list = []
            for p in pods.items:
                pod_list.append(
                    {
                        "name": p.metadata.name,
                        "node": p.spec.node_name,
                        "phase": p.status.phase,
                        "role": p.metadata.labels.get("ray.io/node-type", "?"),
                    }
                )

            return {"nodes": node_list, "pods": pod_list}

        return await asyncio.to_thread(list_cluster_state)

    @fastapi_app.post("/chat")
    async def chat(self, req: ChatRequest):
        self.metrics.start_waiting.remote()
        self.metrics.start_request.remote(self.replica_tag)

        messages = (
            [{"role": "system", "content": "You are a helpful assistant."}]
            + req.history
            + [{"role": "user", "content": req.message}]
        )

        def call_nvidia():
            return requests.post(
                NVIDIA_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": NVIDIA_MODEL,
                    "messages": messages,
                    "temperature": 0.5,
                    "top_p": 1,
                    "max_tokens": 512,
                    "stream": False,
                },
                timeout=90,
            )

        try:
            resp = await asyncio.to_thread(call_nvidia)
        except requests.exceptions.Timeout:
            self.metrics.finish_request.remote(self.replica_tag)
            return {"error": "timeout", "detail": "NVIDIA API did not respond in time"}

        self.metrics.finish_request.remote(self.replica_tag)

        if resp.status_code != 200:
            return {"error": resp.status_code, "detail": resp.text}

        result = resp.json()
        reply = result["choices"][0]["message"]["content"]
        return {"reply": reply}


app = ChatBot.bind()
