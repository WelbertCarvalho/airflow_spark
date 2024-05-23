import requests
import json


class DataExtractor:
    def __init__(self, project_name: str, project_url: str):
        self._project_name = project_name
        self._projetc_url = project_url

    def __str__(self) -> str:
        return f'Project name: {self._project_name} \nProject URL: {self._projetc_url}'

    def get_json_data(self) -> list:
        """
        This method gets data from an specific API URL and transforms it into a list.
        """
        complete_url = self._projetc_url 
        r = requests.get(complete_url)
        json_data = json.loads(r.text)
        return json_data
