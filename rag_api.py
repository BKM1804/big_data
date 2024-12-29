from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from FlagEmbedding import BGEM3FlagModel
import uvicorn
import json
from crawl_data import crawl_content_from_url
from llm import llm_response
model = BGEM3FlagModel("BAAI/bge-m3", use_fp16=True)
app = FastAPI()

# Định nghĩa mô hình dữ liệu đầu vào
class TextInput(BaseModel):
    text: str

@app.post("/get_docs_embedding")
async def get_docs_embedding(input: TextInput):
    try:
        # Generate embedding for the input text using the model
        embedding = model.encode(input.text)['dense_vecs']
        return {"embedding": embedding.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/crawl_data")
async def crawl_data(input: TextInput):
    try:
        # Generate embedding for the input text using the model
        content = crawl_content_from_url(input.text)
        return {"content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/get_score")
async def get_score(input: TextInput):
    try:
        # Assuming input.text contains two embeddings separated by a delimiter, e.g., a comma
        embeddings = input.text.split(',')
        embedding1 = [float(x) for x in embeddings[0].strip().split()]
        embedding2 = [float(x) for x in embeddings[1].strip().split()]

        # Calculate cosine similarity
        dot_product = sum(a * b for a, b in zip(embedding1, embedding2))
        magnitude1 = sum(a * a for a in embedding1) ** 0.5
        magnitude2 = sum(b * b for b in embedding2) ** 0.5
        if magnitude1 == 0 or magnitude2 == 0:
            raise ValueError("One of the embeddings is zero vector, cannot compute cosine similarity.")
        cosine_similarity = dot_product / (magnitude1 * magnitude2)
        return {"score": cosine_similarity}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/get_llm_response")
async def get_llm_response(input: TextInput):
    try:
        # Assuming input.text contains two parts separated by a delimiter, e.g., a semicolon
        parts = input.text.split(';')
        if len(parts) != 2:
            raise ValueError("Input text must contain exactly two parts separated by a semicolon.")
        docs, question = parts[0].strip(), parts[1].strip()
        
        prompt = f"With this source context: {docs} \nAnswer the question: {question}"
        print(prompt)
        return {"response": llm_response(prompt)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
if __name__ == "__main__":
    # Khởi chạy server FastAPI sử dụng Uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

