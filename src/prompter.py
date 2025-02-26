import os
from openai import OpenAI
from prompting_lib import Prompt
import tqdm
with open('secret', 'r') as f:
    key = f.read()


client = OpenAI(api_key=key)

# initial = Prompt('prompts/initial.json')
# initial.prompt(client)
# initial.save()

for section_no in tqdm.tqdm(range(1,11)):
    prompt = Prompt(f'prompts/create{section_no}.json')
    prompt.prompt(client)
    prompt.save()
    