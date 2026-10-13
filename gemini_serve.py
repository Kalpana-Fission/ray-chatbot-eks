import os
import requests
from starlette.requests import Request
from ray import serve

GEMINI_MODEL = "gemini-3.8-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"


@serve.deployment(ray_actor_options={"num_cpus": 0.5})
class GeminiApp:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"]

    async def __call__(self, request: Request):
        data = await request.json()
        prompt = data["prompt"]

        resp = requests.post(
            GEMINI_URL,
            headers={
                "Content-Type": "application/json",
                "X-goog-api-key": self.api_key,
            },
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=30,
        )

        if resp.status_code != 200:
            return {"error": resp.status_code, "detail": resp.text}

        result = resp.json()
        text = result["candidates"][0]["content"]["parts"][0]["text"]
        return {"response": text}


app = GeminiApp.bind()
