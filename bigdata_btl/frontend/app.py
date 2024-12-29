# filename: app.py
import streamlit as st
import sys
import requests
import time
import json
sys.path.append('../')
from big_data_new.crawl import crawl_url
def get_file_content(url_input):
    # Định nghĩa URL endpoint
    url = "http://127.0.0.1:7728/get_file_from_hdfs"
    
    # Định nghĩa payload (dữ liệu gửi đi)
    payload = {
        "url": url_input
    }
    
    # Định nghĩa headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # Gửi yêu cầu POST
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        
        # Kiểm tra mã trạng thái HTTP
        if response.status_code == 200:
            # Giả sử API trả về JSON chứa nội dung
            data = response.json()
            if data['data'] == 0:
                return 0
            return data['data'][0]['file_name']
        else:
            print(f"Yêu cầu thất bại với mã trạng thái: {response.status_code}")
            print("Nội dung phản hồi:", response.text)
            return 0
    
    except requests.exceptions.RequestException as e:
        print("Đã xảy ra lỗi khi gửi yêu cầu:", e)
        return 0
def get_content_from_hdfs(file_name: str):
    from pydantic import BaseModel
    from typing import Optional, List, Union
    class ContentResponse(BaseModel):
        content: Optional[str]  # Trả về None nếu không tìm thấy
        embedding: Optional[list]
    url = "http://127.0.0.1:7728/get_content_from_hdfs"
    payload = {
        "file_name": file_name
    }
    headers = {
        "Content-Type": "application/json"
    }
    try:
        # Gửi yêu cầu POST
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        # Kiểm tra mã trạng thái HTTP
        if response.status_code == 200:
            data = response.json()
            # Tạo đối tượng ContentResponse từ dữ liệu phản hồi
            content_response = ContentResponse(**data)
            return content_response
        else:
            print(f"Yêu cầu thất bại với mã trạng thái: {response.status_code}")
            print("Nội dung phản hồi:", response.text)
            return ContentResponse(content=None, embedding=None)
    
    except requests.exceptions.RequestException as e:
        print("Đã xảy ra lỗi khi gửi yêu cầu:", e)
        return ContentResponse(content=None, embedding=None)
    except json.JSONDecodeError:
        print("Lỗi khi giải mã JSON phản hồi.")
        return ContentResponse(content=None, embedding=None)
    except Exception as e:
        print("Đã xảy ra lỗi không mong muốn:", e)
        return ContentResponse(content=None, embedding=None)

def get_embedding(text):
    url = "http://127.0.0.1:8000/get_docs_embedding"

    # Định nghĩa payload (dữ liệu gửi đi)
    payload = {
        "text": text
    }
    # Định nghĩa headers
    headers = {
        "Content-Type": "application/json"
    }
    try:
        # Gửi yêu cầu POST
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # Kiểm tra mã trạng thái HTTP
        if response.status_code == 200:
            # Giả sử API trả về JSON chứa embedding
            embedding = response.json().get("embedding")
            return embedding
        else:
            print(f"Yêu cầu thất bại với mã trạng thái: {response.status_code}")
            print("Nội dung phản hồi:", response.text)
            return 0

    except requests.exceptions.RequestException as e:
        print("Đã xảy ra lỗi khi gửi yêu cầu:", e)
def get_score(embedding1 , embedding2):
    str_list = map(str, embedding1)
    text1 = ' '.join(str_list)
    str_list = map(str, embedding2)
    text2 = ' '.join(str_list)
    url = "http://localhost:8000/get_score"
    
    # Định nghĩa payload (dữ liệu gửi đi)
    payload = {
        "text": text1 + ',' + text2
    }
    
    # Định nghĩa headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # Gửi yêu cầu POST
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        
        # Kiểm tra mã trạng thái HTTP
        if response.status_code == 200:
            data = response.json()
            # Tạo đối tượng ScoreResponse từ dữ liệu phản hồi
            return  data['score']

        else:
            print(f"Yêu cầu thất bại với mã trạng thái: {response.status_code}")
            print("Nội dung phản hồi:", response.text)
            return ScoreResponse(score=None)
    
    except requests.exceptions.RequestException as e:
        print("Đã xảy ra lỗi khi gửi yêu cầu:", e)
        return ScoreResponse(score=None)
    except json.JSONDecodeError:
        print("Lỗi khi giải mã JSON phản hồi.")
        return ScoreResponse(score=None)
    except Exception as e:
        print("Đã xảy ra lỗi không mong muốn:", e)
        return ScoreResponse(score=None)
def main():
    st.title("Ứng Dụng Nhập URL và Câu Hỏi")

    st.markdown("""
    **Hướng Dẫn:**
    - Nhập vào danh sách các URL, mỗi URL một dòng.
    - Nhập vào câu hỏi bạn muốn đặt.
    - Nhấn nút "Xử Lý" để nhận kết quả.
    """)

    # Input: danh sách các URL
    urls_input = st.text_area(
        "Nhập vào danh sách các URL (mỗi URL một dòng):",
        height=200,
        placeholder="https://example.com/page1\nhttps://example.com/page2\n..."
    )

    # Input: câu hỏi
    question = st.text_input(
        "Nhập vào câu hỏi:",
        placeholder="Bạn muốn biết gì về các URL này?"
    )

    # Nút xử lý
    if st.button("Xử Lý"):
        # Xử lý danh sách các URL
        urls = [url.strip() for url in urls_input.split("\n") if url.strip()]
        num_urls = len(urls)
        file_hdfs = []
        for url in urls:
            file = get_file_content(url)
            if  file == 0:
                crawl_url(url)
                time.sleep(10)
                file_hdfs.append(get_file_content(url))
            else:
                print(url)
                file_hdfs.append(file)
        outs = []
        for file in file_hdfs:
            outs.append(get_content_from_hdfs(file))
        
        # Lấy embedding của câu hỏi
        top_docs = ""
        if question:
            scores = []
            embedding_question = get_embedding(question)
            print(embedding_question)
            for out in outs:
                scores.append(get_score(embedding_question , out.embedding))
            print(scores)
            scores = [1,2,3,1,0]
            sorted_indices = sorted(range(len(scores)), key=lambda i: scores[i] , reverse = True)
            print(sorted_indices)
            top_docs = outs[sorted_indices[0]].content
        print(top_docs)

        st.markdown("### Kết Quả")

        # Hiển thị số lượng URL nhập vào
        st.write(f"Số lượng URL nhập vào: {num_urls}")

        # Hiển thị câu hỏi thêm chữ "oke"
        if question:
            processed_question = question + " oke"
            st.write(f"**Câu hỏi đã xử lý:** {processed_question}")
        else:
            st.warning("Vui lòng nhập vào câu hỏi.")

if __name__ == "__main__":
    main()
