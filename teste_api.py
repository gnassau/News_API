import requests

def api(max_pages=50):
    all_results = []
    for page in range(1, max_pages + 1):
        url = f"https://jsonplaceholder.typicode.com/posts?_page={page}&_limit=10"
        response = requests.get(url)
        data = response.json()
        if not data:
            break  # Para se não houver mais resultados
        last_item = data[-1]  # Pega o último item da página
        all_results.append(last_item['id'])
    return all_results

# Exemplo de uso:
resultados = api()
print(resultados)
