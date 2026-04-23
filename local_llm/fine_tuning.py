from unsloth import FastLanguageModel
from trl import SFTTrainer
from transformers import TrainingArguments
from datasets import load_dataset
import torch

# Config

max_seq_length = 2048
dtype = None 
load_in_4bit = False 
token = open('../secret2').read().strip() 
database = 'chinook'
seed = 67

def load_model():
    print(f'Loading model with provided token...')
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name = "unsloth/Llama-3.2-1B-Instruct",
        max_seq_length = max_seq_length,
        dtype = dtype,
        load_in_4bit = load_in_4bit,
        token = token
    )

    model = FastLanguageModel.get_peft_model(
        model,
        r = 64,
        target_modules = ["q_proj", "k_proj", "v_proj", "o_proj",
                        "gate_proj", "up_proj", "down_proj",],
        lora_alpha = 64,
        lora_dropout = 0,
        bias = "none",
        use_gradient_checkpointing = "unsloth",
        random_state = seed,
    )
    return model, tokenizer
def test_transformations(model, tokenizer):
    FastLanguageModel.for_inference(model)
    prompt_template = (
        "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n"
        "You are a SQL Expert for the Chinook Database. Answer the user's request using valid SQL.<|eot_id|>"
        "<|start_header_id|>user<|end_header_id|>\n\n"
        "{user_prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
    )

    prompts = [
        "Propose a 50 different transformations with projection in a database.",
        "Propose a 50 different transformations with projection and filter in a database.",
    ]

    print("\n--- Testing Transformation & Logic Injection ---")
    
    for user_prompt in prompts:
        full_prompt = prompt_template.format(user_prompt=user_prompt)
        
        inputs = tokenizer([full_prompt], return_tensors="pt").to("cuda")
        
        outputs = model.generate(
            **inputs, 
            use_cache=True,
            temperature=0.1,    # Keep it low for structural accuracy
            top_p=0.9
        )
        
        decoded_output = tokenizer.batch_decode(outputs, skip_special_tokens=False)[0]
        

        assistant_response = decoded_output.split("<|start_header_id|>assistant<|end_header_id|>\n\n")[-1]
        assistant_response = assistant_response.replace("<|eot_id|>", "").replace("<|end_of_text|>", "").strip()

        print(f"User Request: {user_prompt}")
        print(f"SQL Expert:   {assistant_response}")
        print("-" * 50)
def main():
    model, tokenizer = load_model()

    # 1. Load the facts
    dataset = load_dataset("json", data_files=
                           [
                               f"fine_tuning/intro/{database}/1_100messages.json",
                               f"fine_tuning/intro/{database}/101_200messages.json",
                               f"fine_tuning/intro/{database}/201_300messages.json",
                               f"fine_tuning/intro/{database}/301_400messages.json",
                               f"fine_tuning/intro/{database}/401_500messages.json",
                               f"fine_tuning/sql/{database}/schema_messages.json",
                               f"fine_tuning/sql/{database}/1_20messages.json",
                               f"fine_tuning/sql/{database}/21_40messages.json",
                               f"fine_tuning/sql/{database}/41_60messages.json",
                               f"fine_tuning/sql/{database}/61_80messages.json",
                               f"fine_tuning/sql/{database}/81_100messages.json",
                               f"fine_tuning/sql/{database}/101_120messages.json",
                               f"fine_tuning/sql/{database}/121_140messages.json",
                               f"fine_tuning/transforming/{database}/1_20messages.json",
                               f"fine_tuning/transforming/{database}/21_40messages.json",
                               f"fine_tuning/transforming/{database}/41_60messages.json",
                               f"fine_tuning/transforming/{database}/61_80messages.json",
                               f"fine_tuning/transforming/{database}/81_100messages.json"
                           ], split="train")
    dataset = dataset.shuffle(seed=seed)
    # 2. Configure Training
    trainer = SFTTrainer(
        model = model,
        tokenizer = tokenizer,
        train_dataset = dataset,
        dataset_text_field = "text", # {"text": "..."} format
        max_seq_length = max_seq_length,
        dataset_num_proc = 2,
        args = TrainingArguments(
            per_device_train_batch_size = 4,
            gradient_accumulation_steps = 4,
            warmup_steps = 5,
            max_steps = 6000, 
            learning_rate = 2e-4,
            fp16 = not torch.cuda.is_bf16_supported(),
            bf16 = torch.cuda.is_bf16_supported(),
            logging_steps = 1,
            optim = "adamw_8bit",
            weight_decay = 0.01,
            lr_scheduler_type = "linear",
            seed = seed,
            output_dir = "outputs",
        ),
    )

    # 3. Start Training
    print("Starting Phase 1: Knowledge Injection...")
    trainer.train()
    test_transformations(model, tokenizer)
    # 4. Save model
    model.save_pretrained("llama3.2_1b_instruct_chinook_facts")
    tokenizer.save_pretrained("llama3.2_1b_instruct_chinook_facts")
    print("Phase 1 complete. Model saved to 'llama3.2_1b_chinook_facts'")

if __name__ == "__main__" : 
    main()