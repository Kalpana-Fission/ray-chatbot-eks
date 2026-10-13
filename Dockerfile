FROM rayproject/ray:2.52.0

WORKDIR /home/ray

COPY chatbot_serve.py .

RUN pip install --no-cache-dir requests
