import requests
number_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrive_a_store_url = ' https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}   



def list_number_of_stores(self):
    url = f'{self.base_url}number_stores'
    response = requests.get(url, headers=self.headers)
    if response.status_code == 200:
        number_of_stores = response.json()
        return number_of_stores
    else:
        print(f"Failed to retrieve the number of stores. Error: {response.text}")
        return None

