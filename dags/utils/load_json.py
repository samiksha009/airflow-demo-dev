import json

def load_schema_config(json_path):
    with open(json_path, 'r') as f:
        return json.load(f)