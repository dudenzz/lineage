from flask import Flask, request, jsonify, render_template
from unsloth import FastLanguageModel
import flask
import torch

app = Flask(__name__)

# --- Model Configuration ---
max_seq_length = 2048
dtype = None 
load_in_4bit = False

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name = "llama3.2_1b_instruct_chinook_facts",
    max_seq_length = max_seq_length,
    dtype = dtype,
    load_in_4bit = load_in_4bit,
)
FastLanguageModel.for_inference(model)

# --- Prompt Template ---
prompt_template = (
    "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n"
    "You are a SQL Expert for the Chinook Database. Answer the user's request using valid SQL.<|eot_id|>"
    "<|start_header_id|>user<|end_header_id|>\n\n"
    "{user_prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate_sql', methods=['POST'])
def generate_sql():
    data = request.json
    user_query = data.get("prompt", "")
    
    full_prompt = prompt_template.format(user_prompt=user_query)
    inputs = tokenizer([full_prompt], return_tensors="pt").to("cuda")
    
    outputs = model.generate(
        **inputs, 
        max_new_tokens=256,
        temperature=0.1,
        use_cache=True
    )
    
    decoded = tokenizer.batch_decode(outputs, skip_special_tokens=False)[0]
    # Extract only the assistant's part
    sql_result = decoded.split("<|start_header_id|>assistant<|end_header_id|>\n\n")[-1]
    sql_result = sql_result.replace("<|eot_id|>", "").replace("<|end_of_text|>", "").strip()
    
    return jsonify({"sql": sql_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)