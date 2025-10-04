from bs4 import BeautifulSoup

with open('dataset/SalesTransactions.xml', 'r', encoding='utf-8') as file:
    content = file.read()

soup = BeautifulSoup(content, 'xml')
data = soup.find_all('UelSample')
print(data)


