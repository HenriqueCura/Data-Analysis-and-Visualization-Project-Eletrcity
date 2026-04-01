import os
from openai import OpenAI
from pydantic import BaseModel
import json
from translate import main
from dotenv import load_dotenv

load_dotenv()

def run_agent(user_text: str):
    user_text = main(user_text)

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    system_prompt = """
    You are a professional summarization assistant.

    Rules:
    - The summary must be written in English.
    - Minimum 100 words.
    - Maximum 300 words.
    - Do not add explanations.
    - Only return the summary.
    """


    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text}
    ]

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
    )

    message = response.choices[0].message
    return message.content
