import os
import requests
from starlette.requests import Request
from ray import serve

NVIDIA_MODEL = "nvidia/nemotron-3-super-120b-a12b"
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

        messages = (
            [{"role": "system", "content": "You are a helpful assistant."}]
            + history
            + [{"role": "user", "content": message}]
        )

        try:
            resp = requests.post(
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
        except requests.exceptions.Timeout:
            return {"error": "timeout", "detail": "NVIDIA API did not respond in time"}

        if resp.status_code != 200:
            return {"error": resp.status_code, "detail": resp.text}

        result = resp.json()
        reply = result["choices"][0]["message"]["content"]
        return {"reply": reply}


app = ChatBot.bind()
