import json
import openai
from os import path
class Prompt:
    def __init__(self, context, task, name):
        self.context = context
        self.task = task
        self.name = name
    def __init__(self, filename):
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            self.context = data['context']
            self.task = data['task']
            self.name = path.basename(filename)
    def prompt(self, openai_client : openai.Client):
        chat_completion = openai_client.chat.completions.create(
        messages=[
            {
                "role" : 'system',
                "content": self.context
            },
            {
                "role" : 'system',
                "content": self.task
            }
        ],
        model="gpt-4o",
        )
        self.answer = chat_completion.choices[0].message.content
    def save(self):
        with open(f'answers/{self.name}.answer', 'w+') as file:
            file.write(self.answer)

