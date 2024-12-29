from urllib.request import urlopen
import urllib
from bs4 import BeautifulSoup
def crawl_content_from_url(url):
    html = urlopen(url).read()
    soup = BeautifulSoup(html, features="html.parser")
    for script in soup(["script", "style"]):
        script.extract()    # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    # text = '\n'.join(chunk for chunk in chunks if chunk)
    ans = ""
    for chunk in chunks:
        if chunk:
            if len(chunk.strip().split()) > 30:
                ans += chunk    
    return ans
if __name__ == "__main__":
    print(crawl_content_from_url("https://edition.cnn.com/2024/12/19/politics/fani-willis-donald-trump-georgia/index.html"))