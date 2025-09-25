from multicorn.utils import log_to_postgres
from .utils import normalize_items, unwrap_object, map_row, build_request


def execute(self, quals, columns, sortkeys=None):
    filters = {}
    for q in quals:
        if q.field_name in self.columns and q.operator == "=":
            filters[q.field_name] = q.value

    if self.paginated:
        page = self.start_page
        while True:
            if self.pagination_style == "path":
                url, params = build_request(self.url, None, False, None, page, self.limit)
                data = self.client.fetch(url, params={**filters, **params})
            elif self.pagination_style == "params":
                params = {"page": page, "limit": self.limit, **filters}
                data = self.client.fetch(self.url, params=params)
            else:
                data = self.client.fetch(self.url, params=filters)

            items = normalize_items(data)
            if not items:
                break
            for item in items:
                yield map_row(item, self.columns)
            if len(items) < self.limit or self.only_first_page:
                break
            page += 1
    else:
        data = self.client.fetch(self.url, params=filters)
        items = normalize_items(data)
        for item in items:
            yield map_row(item, self.columns)


def insert(self, new_values):
    log_to_postgres(f"Inserting: {new_values}", level=10)
    resp = self.client.request("POST", self.url, json=new_values)
    try:
        data = unwrap_object(resp.json())
        return map_row(data, self.columns)
    except Exception as e:
        log_to_postgres(f"Error parsing JSON response after INSERT: {e}", level=20)
        return new_values


def update(self, rowid, new_values):
    url, params = build_request(self.url, rowid, self.pk_as_query_param, self.primary_key)
    log_to_postgres(f"Updating ID '{rowid}' at {url} with values: {new_values}", level=10)
    resp = self.client.request("PUT", url, json=new_values, params=params or None)
    try:
        data = unwrap_object(resp.json())
        return map_row(data, self.columns)
    except Exception as e:
        log_to_postgres(f"Error parsing JSON response after UPDATE: {e}", level=20)
        return new_values


def delete(self, rowid):
    url, params = build_request(self.url, rowid, self.pk_as_query_param, self.primary_key)
    try:
        log_to_postgres(f"DELETE -> {url} with params={params}", level=10)
        self.client.request("DELETE", url, params=params or None)
        return None
    except Exception as e:
        log_to_postgres(f"DELETE failed: {e}, trying JSON body.", level=20)

    # Fallback JSON body
    try:
        payload = {"control_environment_ids": [rowid]}
        log_to_postgres(f"DELETE JSON body -> {self.url} with {payload}", level=10)
        self.client.request("DELETE", self.url, json=payload)
    except Exception as e:
        log_to_postgres(f"DELETE JSON body failed: {e}", level=20)
    return None
    