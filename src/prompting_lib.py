import json
import openai
from os import path
class Prompt:
    def __init__(self, context, task, name):
        self.context = context
        self.task = task
        self.name = name
    def __init__(self, context, task, sinp, sout, code, name):
        self.context = context
        self.code = code
        self.task = task
        self.name = name
        self.sinp = sinp
        self.sout = sout
        
    # def __init__(self, filename):
    #     with open(filename,'r') as json_file:
    #         data = json.load(json_file)
    #         self.context = data['context']
    #         self.task = data['task']
    #         self.name = path.basename(filename)
    def prompt_with_code(self, openai_client : openai.Client):
        chat_completion = openai_client.chat.completions.create(
        messages=[
            {
                "role" : 'system',
                "content": self.context
            },
            {
                "role" : 'system',
                "content": self.task
            },
            
                        {
                "role" : 'system',
                "content": self.sinp
            },
            
                        {
                "role" : 'system',
                "content": self.sout
            }
            ,
                        {
                "role" : 'system',
                "content": self.code
            }
        ],
        model="gpt-4o",
        n=5
        )
        self.answers = {}
        self.answers[0] = chat_completion.choices[0].message.content
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
        n=1
        )
        self.answers = {}
        self.answers[0] = chat_completion.choices[0].message.content
    def save_transformed(self):
        for i in range(1,2):
            with open(f'transformations/{self.name}.{i}.answer', 'w+', encoding='utf-8') as file:
                file.write(self.answers[i-1])
    def save(self):
        for i in range(1,6):
            with open(f'answers/{self.name}.{i}.answer', 'w+', encoding='utf-8') as file:
                file.write(self.answers[i-1])

