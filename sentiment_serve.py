from ray import serve
from starlette.requests import Request


@serve.deployment(ray_actor_options={"num_cpus": 1})
class SentimentModel:
    def __init__(self):
        from transformers import pipeline
        self.pipe = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
        )

    async def __call__(self, request: Request):
        data = await request.json()
        text = data["text"]
        result = self.pipe(text)[0]
        return {"label": result["label"], "score": round(result["score"], 4)}


app = SentimentModel.bind()
