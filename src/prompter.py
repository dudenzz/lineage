import os
from openai import OpenAI
from prompting_lib import Prompt
import tqdm
import json
from os import path
with open('secret', 'r') as f:
    key = f.read()


client = OpenAI(api_key=key)

# initial = Prompt('prompts/initial.json')
# initial.prompt(client)
# initial.save()

# for section_no in tqdm.tqdm(range(1,11)):
#     prompt = Prompt(f'prompts/create{section_no}.json')
#     prompt.prompt(client)
#     prompt.save()
with open('prompts/transform.json','r') as json_file:
            data = json.load(json_file)
            context = data['context']
            task = data['task']
            code = open('src/SQL_scripts/transformation1.2.sql').read()
            sample_input = open('examples/example.1.1.transformation').read()
            sample_output = open('examples/example.1.1.lineage_adaptation').read()
            name = path.basename('src/SQL_scripts/transformation1.2.sql')
            transform_prompt = Prompt(context, task, sample_input, sample_output, code, name)
            transform_prompt.prompt_with_code(client)
            transform_prompt.save_transformed()