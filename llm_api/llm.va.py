from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from meta_ai_api import MetaAI


ai = MetaAI()
import uvicorn

app = FastAPI()

# Định nghĩa mô hình dữ liệu đầu vào
class TextInput(BaseModel):
    text: str


@app.post("/get_llm_response")
async def get_llm_response(input: TextInput):
    try:
        # Assuming input.text contains two parts separated by a delimiter, e.g., a semicolon
        parts = input.text.rsplit(';', 1)
        if len(parts) != 2:
            raise ValueError("Input text must contain exactly two parts separated by a semicolon.")
        docs, question = parts[0].strip(), parts[1].strip()
        
        prompt = f"With this source context: {docs} \nAnswer the question: {question}"
        # print(prompt)
        response = ai.prompt(message=prompt)
        return {"response": response["message"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
if __name__ == "__main__":
    # Khởi chạy server FastAPI sử dụng Uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
