import os
import requests
from starlette.requests import Request
from ray import serve

NVIDIA_MODEL = "meta/llama3-8b-instruct"
NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"


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
class ChatBot:
    def __init__(self):
        self.api_key = os.environ["NVIDIA_API_KEY"]

    async def __call__(self, request: Request):
        data = await request.json()
        message = data["message"]
        history = data.get("history", [])

        messages = history + [{"role": "user", "content": message}]

        resp = requests.post(
            NVIDIA_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": NVIDIA_MODEL,
                "messages": messages,
                "max_tokens": 256,
                "temperature": 0.7,
            },
            timeout=30,
        )

        if resp.status_code != 200:
            return {"error": resp.status_code, "detail": resp.text}

        result = resp.json()
        reply = result["choices"][0]["message"]["content"]
        return {"reply": reply}


app = ChatBot.bind()
