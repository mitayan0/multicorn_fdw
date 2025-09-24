import json

def normalize_items(response_json):
    if isinstance(response_json, list):
        return response_json
    if isinstance(response_json, dict):
        for key in ("items", "results", "data"):
            if key in response_json and isinstance(response_json[key], list):
                return response_json[key]
        return [response_json]
    return [response_json]

def unwrap_object(data):
    if isinstance(data, dict):
        for key in ("data", "item", "result"):
            if key in data and isinstance(data[key], dict):
                return data[key]
    return data

def map_row(item, columns):
    row = {}
    for col in columns:
        val = item.get(col) if isinstance(item, dict) else None
        if isinstance(val, (dict, list)):
            row[col] = json.dumps(val)
        else:
            row[col] = val
    return row
