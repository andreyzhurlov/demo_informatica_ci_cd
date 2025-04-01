import requests

url = "https://emw1.dm-em.informaticacloud.com/saas/public/core/v3/export/291qkmixUtUfXGt5wk8YMr/package"
headers = {
    "INFA-SESSION-ID": "8m83VvqBOqFbO9z3EQoJ8b" # <<--< change it on actual session_id
}
output_path = "./exports/export_package.zip"

response = requests.get(url, headers=headers)

if response.status_code == 200:
    with open(output_path, "wb") as f:
        f.write(response.content)
    print(f"[V] File saved successfully as {output_path}")
else:
    print(f"[X] Error: status {response.status_code}")
    print(response.text)