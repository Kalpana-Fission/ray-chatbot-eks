import asyncio
import os
import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from ray import serve

NVIDIA_MODEL = "nvidia/nemotron-3-super-120b-a12b"
NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

fastapi_app = FastAPI()

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


@serve.deployment(
    ray_actor_options={"num_cpus": 0.5},
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 6,
        "target_ongoing_requests": 2,
        "upscale_delay_s": 5,
        "downscale_delay_s": 30,
    },
)
@serve.ingress(fastapi_app)
class ChatBot:
    def __init__(self):
        self.api_key = os.environ["NVIDIA_API_KEY"]

    @fastapi_app.get("/", response_class=HTMLResponse)
    async def home(self):
        return HTML_PAGE

    @fastapi_app.post("/chat")
    async def chat(self, req: ChatRequest):
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
            return {"error": "timeout", "detail": "NVIDIA API did not respond in time"}

        if resp.status_code != 200:
            return {"error": resp.status_code, "detail": resp.text}

        result = resp.json()
        reply = result["choices"][0]["message"]["content"]
        return {"reply": reply}


app = ChatBot.bind()
