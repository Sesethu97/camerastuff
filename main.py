from typing import Optional, List, Dict

import requests
from supabase import create_client, Client


def get_data(endpoint: str, page_limit: int = 10) -> List[Dict[str, str]]:

    total_pages = requests.get(endpoint).json().get("total_pages")
    data = []

    for i in range(total_pages):
        response = requests.get(f"{endpoint}?page={i + 1}&limit={page_limit}").json()
        results = response.get("results")
        for result in results:
            person_data = requests.get(result.get("url")).json().get("result").get("properties")
            result.update(person_data)
            data.append(result)
        
    return data


def get_person_mass_pounds(person: Dict[str, str]) -> Optional[int]:

    try:
        mass_kg = int(requests.get(person.get("url")).json().get("result").get("properties").get("mass"))
        return round(mass_kg * 2.205)
    except ValueError:
        return None


def insert_data(table_name: str, data: List[Dict[str, str]]) -> None:
    supabase: Client = create_client(
        "https://rwhckknnvgxzjetoarah.supabase.co",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ3aGNra25udmd4empldG9hcmFoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTM5NTgzNzEsImV4cCI6MjAyOTUzNDM3MX0.0zc0tXIpNouUoYSF0a3kR-ZU7DSC9SVQ5TIEjMPYMVI"
    )

    for item in data:
        if table_name == "people":
            item["mass"] = get_person_mass_pounds(item)
        for key, value in item.items():
            if value == "unknown":
                item[key] = None

        if table_name == "planets" and not item.get("name"):
            continue 
        supabase.table(table_name).insert(item).execute()






def main() -> None:

    endpoints = [
        {"url": "https://swapi.tech/api/people", "table": "people"},
        {"url": "https://swapi.tech/api/planets", "table": "planets"},
    ]
    for endpoint in endpoints:
        results = get_data(endpoint.get("url"))
        insert_data(endpoint.get("table"), results)


if __name__ == "__main__":
    main()
