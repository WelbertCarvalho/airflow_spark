import requests
import json


class DataExtractor:
    def __init__(self, project_name: str, project_url: str):
        self._project_name = project_name
        self._projetc_url = project_url
        self._tot_items = 0
        self._list = []

    def __str__(self) -> str:
        return f'Project name: {self._project_name} \nProject URL: {self._projetc_url}'

    def get_json_data(self) -> dict:
        """
        This method gets data from an specific API URL and transforms it into a list.
        """
        base_url = self._projetc_url
        page = 0
        list_with_data = True
        json_data = []
        while list_with_data == True and page <= 9:
            page += 1
            try:
                r = requests.get(f'{base_url}?page={page}')
                json_data_page = json.loads(r.text)
                if len(json_data_page) > 0:
                    print(f'Page: {page} | Status: {r.status_code}')
                    json_data.append(json_data_page)
                else:
                    break
            except:
                print(f'Page number: {page} not valid.')    
                
        return {'page' : page, 'json_data' : json_data}
    

    def get_tot_items(self, dict_json_data: dict) -> int:
        for num_page, _ in enumerate(dict_json_data):
            self._tot_items += len(dict_json_data[num_page])

        return self._tot_items
    
    def flatten_list(self, list_to_be_flattened: list) -> list:
        for sublist in list_to_be_flattened:
            for item in sublist:
                self._list.append(item)
        
        json_data_in_flattened = self._list
        
        return json_data_in_flattened


if __name__ == '__main__':
    from extraction import DataExtractor

    extract_obj = DataExtractor(
        project_name = 'AbInbev project',
        project_url = 'https://api.openbrewerydb.org/breweries'
    )

    data = extract_obj.get_json_data()
    data_flattened = extract_obj.flatten_list(list_to_be_flattened = data['json_data'])
    print(data_flattened)
    tot_items = extract_obj.get_tot_items(dict_json_data = data['json_data'])

    print(f"Total pages: {data['page']} | Total Items: {tot_items}")
    