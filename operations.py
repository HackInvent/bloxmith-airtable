"""The six Airtable operations, built from Session calls and pure logic.

Reads go through pages bounded by page_size, max_pages, max_records and the
output size limit, and always return an exact continuation offset. Writes are
validated and sized completely before the first request, then sent in batches
of at most 10 records; a failure reports the batches already committed.
"""

from . import logic


def identity(request, config):
    return {"request_id": request["request_id"], "operation": request["operation"], "base_id": config["base_id"],
            "table": request["table"]}


def list_page(session, request, config, size, offset):
    """GET one page; a URL beyond Airtable's 16,000-character limit uses POST listRecords with the same parameters."""
    params = logic.list_parameters(request, config, size, offset)
    path = logic.table_path(config["base_id"], request["table"])
    query = logic.list_query(params)
    if len(config["api_base_url"]) + len(path) + 1 + len(query) < logic.URL_LIMIT:
        return session.call("GET", path, query, mutation=False), "GET"
    body = logic.encoded(params, config["max_request_bytes"], "request_too_large")
    return session.call("POST", path + "/listRecords", None, body, mutation=False), "POST"


def list_records(session, request, config):
    """Read pages until the end, max_records, max_pages or the output limit; never drop a record of a read page."""
    limit = request["max_records"]
    records, total, pages, methods = [], 2, 0, []
    offset, stop, next_offset = request["offset"], "end", None
    while True:
        if pages >= config["max_pages"]:
            stop, next_offset = "max_pages", offset
            break
        # The last page asks only for the records still needed, so the returned offset continues exactly.
        size = min(request["page_size"], limit - len(records))
        body, method = list_page(session, request, config, size, offset)
        pages += 1
        methods.append(method)
        items, following = logic.page(body, size)
        weight = sum(logic.size_of(item) for item in items)
        if total + weight > logic.OUTPUT_LIMIT:
            if not records:
                logic.fail("output_too_large")
            stop, next_offset = "output_limit", offset
            break
        records.extend(items)
        total += weight
        offset = following
        if offset is None:
            break
        if len(records) >= limit:
            stop, next_offset = "limit", offset
            break
    result = {**identity(request, config), "view": request["view"] or None, "count": len(records),
              "truncated": stop != "end", "stop_reason": stop, "offset": next_offset, "pages": pages,
              "post_list_requests": methods.count("POST"), "requests": session.requests}
    return {"records": records, "result": result}


def get_record(session, request, config):
    path = logic.table_path(config["base_id"], request["table"], request["record_id"])
    query = "returnFieldsByFieldId=true" if config["return_fields_by_field_id"] else None
    item = logic.record(session.call("GET", path, query, mutation=False))
    if item["id"] != request["record_id"]:
        logic.fail("protocol")
    result = {**identity(request, config), "record_id": item["id"], "count": 1, "requests": session.requests}
    return {"records": [item], "result": result}


def committed(exc, done, total, ids):
    """Attach batch progress to a failure; earlier batches stay written and are listed."""
    exc.progress = {"completed_batches": done, "total_batches": total, "committed_record_ids": list(ids)}
    if done:
        exc.args = (str(exc) + " " + logic.MESSAGES["partial"],)
    return exc


def write_records(session, request, config):
    """create_records (POST), update_records (PATCH) and upsert_records (PATCH performUpsert) in batches of 10."""
    operation = request["operation"]
    method = "POST" if operation == "create_records" else "PATCH"
    path = logic.table_path(config["base_id"], request["table"])
    chunks, bodies = logic.batches(request["records"]), logic.write_bodies(request, config)
    records, created, updated, ids = [], [], [], []
    for index, (chunk, body) in enumerate(zip(chunks, bodies)):
        try:
            receipt = session.call(method, path, None, body, mutation=True)
            expected = [item["id"] for item in chunk] if operation == "update_records" else None
            items = logic.written(receipt, len(chunk), ids=expected)
            if operation == "upsert_records":
                new, old = logic.upserted(receipt, items)
                created.extend(new)
                updated.extend(old)
        except logic.AirtableError as exc:
            raise committed(exc, index, len(chunks), ids) from None
        records.extend(items)
        ids.extend(item["id"] for item in items)
    result = {**identity(request, config), "count": len(records), "record_ids": ids, "batches": len(chunks),
              "typecast": request["typecast"], "requests": session.requests}
    if operation == "upsert_records":
        result.update({"fields_to_merge_on": request["fields_to_merge_on"], "created_record_ids": created,
                       "updated_record_ids": updated})
    return {"records": records, "result": result}


def delete_records(session, request, config):
    """DELETE /v0/<base>/<table>?records[]=... in batches of 10."""
    path = logic.table_path(config["base_id"], request["table"])
    chunks = logic.batches(request["record_ids"])
    ids = []
    for index, chunk in enumerate(chunks):
        try:
            receipt = session.call("DELETE", path, logic.delete_query(chunk), mutation=True)
            ids.extend(logic.deleted(receipt, chunk))
        except logic.AirtableError as exc:
            raise committed(exc, index, len(chunks), ids) from None
    result = {**identity(request, config), "count": len(ids), "deleted_record_ids": ids, "batches": len(chunks),
              "requests": session.requests}
    return {"result": result}


HANDLERS = {"list_records": list_records, "get_record": get_record, "create_records": write_records,
            "update_records": write_records, "upsert_records": write_records, "delete_records": delete_records}


def run(session, request, config):
    return HANDLERS[request["operation"]](session, request, config)
