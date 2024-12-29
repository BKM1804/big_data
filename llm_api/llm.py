import torch
from transformers import pipeline
from transformers import AutoModelForCausalLM , AutoTokenizer
from refine_prompt import test_fix_message_structure_for_prompt
class RequestInfo:
    def __init__(self, messages):
        self.messages = messages
class Engine:
    def __init__(self, model_name):
        self.model_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
model_id = "unsloth/Llama-3.2-3B"
# engine = Engine(model_name="tau-vision/llama-tokenizer-fix")
pipe = pipeline(
        "text-generation", 
        model=model_id, 
        torch_dtype=torch.bfloat16, 
        device_map="auto",
        max_length=200,
        truncation=True
    ) 
def llm_response(prompt):
    # starting_assistant_message = True
    # messages = [{"role": "User", "content": prompt} ]
    # formatted_prompt = engine.tokenizer.apply_chat_template(conversation=messages, tokenize=False, add_generation_prompt=starting_assistant_message)
    prompt = test_fix_message_structure_for_prompt(prompt , "" , True)
    return pipe(prompt)
if __name__ == "__main__":
    # formatted_prompt = "the capital of vietnam is"
    prompt = "where is the capital of vietnam?"
    print(llm_response(prompt))