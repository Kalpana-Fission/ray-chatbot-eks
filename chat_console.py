import requests

URL = "http://localhost:8000/"


def main():
    history = []
    print("Chatbot console. Type 'exit' to quit.\n")
    while True:
        message = input("You: ").strip()
        if message.lower() in ("exit", "quit"):
            break
        if not message:
            continue

        try:
            r = requests.post(URL, json={"message": message, "history": history}, timeout=90)
        except requests.exceptions.RequestException as e:
            print(f"[error contacting chatbot: {e}]\n")
            continue

        data = r.json()
        if "error" in data:
            print(f"[error: {data['error']} - {data.get('detail', '')}]\n")
            continue

        reply = data["reply"]
        print(f"Bot: {reply}\n")

        history.append({"role": "user", "content": message})
        history.append({"role": "assistant", "content": reply})


if __name__ == "__main__":
    main()
