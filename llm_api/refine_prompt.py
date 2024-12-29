from typing import Any, List
from transformers import AutoModelForCausalLM
from llama_index.core.llms import ChatMessage , MessageRole
import logging
from transformers import AutoModelForCausalLM , AutoTokenizer
class RequestInfo:
    def __init__(self, messages):
        self.messages = messages
class Engine:
    def __init__(self, model_name):
        self.model_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
def fix_message_structure_for_prompt(tokenizer: Any, messages: List[ChatMessage]) -> List[ChatMessage]:
    """
    Because the chat_formats in the tokenizer are fixed and don't allow things like system instructions (mistralai),
    or message ordering like usr -> usr -> assistant we need to fix the message structure before sending it to the model
    we do this by joining the system prompt to the next user (or a blank user message) for mistral
    and joining usr -> usr to become usr:concat(usr, usr), ass -> ass to become ass:concat(ass, ass)
    If there is a system message with mistral we join it with the next usr message so sys -> sys -> usr becomes usr:concat(sys, sys, usr)
    """

    # Mistral can't handle system prompts. If Mistral is detected, we need to join the system prompt with the next user message
    # if not missing_system_prompts(tokenizer):
    #     return messages
    
    logging.debug(f"Messages: {messages}")
    user_message_buffer: str = ""
    assistant_buffer: str = ""
    processed_messages: List[str] = []
    def _join_sequential_messages(buffer: str, message: str) -> str:
        return buffer + message
    def _add_assistant_buffer_to_processed_messages() -> None:
        nonlocal assistant_buffer

        # Edge case if assistant is the first messsage:
        if len(processed_messages) == 0:
            processed_messages.append(ChatMessage(role=MessageRole.USER, content=":"))

        if assistant_buffer:
            processed_messages.append(ChatMessage(role=MessageRole.ASSISTANT, content=assistant_buffer))
            assistant_buffer = ""

    def _add_user_buffer_to_processed_messages() -> None:
        nonlocal user_message_buffer

        if user_message_buffer:
            processed_messages.append(ChatMessage(role=MessageRole.USER, content=user_message_buffer))
            user_message_buffer = ""

    last_message_was_assistant: bool = False

    for message in messages:
        if message.role == MessageRole.SYSTEM:
            if last_message_was_assistant:
                _add_assistant_buffer_to_processed_messages()

            if len(user_message_buffer) == 0:
                user_message_buffer += SYSTEM_PROMPT_PREFIX

            user_message_buffer = _join_sequential_messages(user_message_buffer, message.content)
            last_message_was_assistant = False

        elif message.role == MessageRole.USER:
            if last_message_was_assistant:
                _add_assistant_buffer_to_processed_messages()

            user_message_buffer = _join_sequential_messages(user_message_buffer, message.content)
            last_message_was_assistant = False

        elif message.role == MessageRole.ASSISTANT:
            if not last_message_was_assistant:
                _add_user_buffer_to_processed_messages()

            assistant_buffer = _join_sequential_messages(assistant_buffer, message.content)

            last_message_was_assistant = True

    if assistant_buffer:
        _add_assistant_buffer_to_processed_messages()
    elif user_message_buffer:
        _add_user_buffer_to_processed_messages()

    logging.debug(f"Processed messages: {processed_messages}")
    return processed_messages

def test_fix_message_structure_for_prompt(user_prompt , part_of_response , starting_assistant_message):
    
    engine = Engine(model_name="tau-vision/llama-tokenizer-fix")
    request_info = RequestInfo(messages=[ChatMessage(role=MessageRole.USER  , content=user_prompt), ChatMessage(role=MessageRole.ASSISTANT, content=part_of_response)])
    messages_dict = [message.model_dump() for message in fix_message_structure_for_prompt(engine.tokenizer, request_info.messages)]
    formatted_prompt = engine.tokenizer.apply_chat_template(conversation=messages_dict, tokenize=False, add_generation_prompt=starting_assistant_message)
    if "llama-3" in engine.model_name.lower() and not starting_assistant_message:
        formatted_prompt = formatted_prompt[: formatted_prompt.rfind("<|eot_id|>")]

    end_of_string_token = engine.tokenizer.eos_token
    if not starting_assistant_message and formatted_prompt.rstrip().endswith(end_of_string_token):
        formatted_prompt = formatted_prompt.rstrip()[: -len(end_of_string_token)]
    return formatted_prompt
if __name__ == "__main__":
    print(test_fix_message_structure_for_prompt("Hello" , "Hello" , True))