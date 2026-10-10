"""FB2/FB3/FB4/FB5: loopback Airtable protocol, pagination, batches, pacing, errors, cancellation and both runtimes.

Every provider exchange targets the loopback double in airtable_fixture.py. The
wallet and its fixture token exist only in disposable test storage.
"""

import json
from pathlib import Path
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests"), str(Path(__file__).resolve().parent)]
from blocs.airtable import logic, operations, transport
from blocs.airtable.block import AirtableBlock
from block_test_packages import install_test_package, prepare_release_run, surface_payload
from airtable_fixture import (ACTIVE_VIEW, BASE, EXPIRED, LIMIT_429, OTHER_BASE, TABLE, TABLE_ID, TOKEN, Table, airtable_api,
                              error, settings)
from ui_smoke_common import (create_project_api, create_run_api, data_edge, display_node, graph_payload, http_json,
                             isolated_server, stop_run_api, text_node, wait_for_run_predicate, wait_for_run_terminal)

ALL_WRITES = ["list_records", "get_record", "create_records", "update_records", "upsert_records", "delete_records"]


def run(config, value, token=TOKEN, cancel=lambda: False):
    """Execute one request through the real session and operations."""
    session = transport.Session(config, token, cancel)
    return operations.run(session, logic.parse_request(value, config), config)


def failure(call, code):
    try:
        call()
    except logic.AirtableError as exc:
        assert exc.code == code, (code, exc.code, str(exc))
        assert TOKEN not in json.dumps(exc.envelope())
        return exc
    raise AssertionError("Expected " + code)


def paged(api, count=25):
    rows = [{"n": index, "word": "w" + str(index)} for index in range(1, count + 1)]
    api.bases[BASE]["Paged"] = Table("Paged", "tblPagedFixture01", {"n": "number", "word": "text"}, rows, api.counter)
    return api.bases[BASE]["Paged"]


def names(result):
    return [item["fields"].get("Name") for item in result["records"]]


def read_protocol():
    """# - FB2 - # - FB4 - views, formulas, fields, sort, exact continuation offsets, POST listRecords and get_record."""
    with airtable_api() as api:
        config = logic.configuration(settings(api, allowed_operations=["list_records", "get_record"]))
        read = run(config, "go")
        assert names(read) == ["Ada", "Grace", "Linus"]
        assert read["records"][1] == {"id": "recFixture0000002", "created_time": "2026-01-03T10:00:00.000Z",
                                      "fields": {"Name": "Grace", "Email": "grace@example.test", "Status": "active",
                                                 "Amount": 75.5, "Done": True}}
        assert "Done" not in read["records"][0]["fields"], "Airtable leaves empty fields out"
        result = read["result"]
        assert result == {"request_id": None, "operation": "list_records", "base_id": BASE, "table": TABLE, "view": None,
                          "count": 3, "truncated": False, "stop_reason": "end", "offset": None, "pages": 1,
                          "post_list_requests": 0, "requests": 1}
        call = api.calls[-1]
        assert call["method"] == "GET" and call["path"] == "/v0/" + BASE + "/" + TABLE and call["query"] == {"pageSize": ["100"]}
        assert call["auth"] == "Bearer " + TOKEN and call["accept_encoding"] == "identity"
        assert names(run(config, {"view": "Active"})) == ["Grace"] and api.calls[-1]["query"]["view"] == ["Active"]
        assert names(run(logic.configuration(settings(api, view=ACTIVE_VIEW)), "go")) == ["Grace"]
        assert names(run(config, {"filter_by_formula": "{Status} = 'new'"})) == ["Ada", "Linus"]
        assert api.calls[-1]["query"]["filterByFormula"] == ["{Status} = 'new'"]
        assert names(run(config, {"sort": [{"field": "Name", "direction": "desc"}]})) == ["Linus", "Grace", "Ada"]
        assert api.calls[-1]["query"]["sort[0][field]"] == ["Name"] and api.calls[-1]["query"]["sort[0][direction]"] == ["desc"]
        slim = run(config, {"fields": ["Name", "Amount"], "table": TABLE_ID})
        assert [set(item["fields"]) for item in slim["records"]] == [{"Name", "Amount"}] * 3
        assert api.calls[-1]["query"]["fields[]"] == ["Name", "Amount"] and api.calls[-1]["path"].endswith("/" + TABLE_ID)
        by_id = run(logic.configuration(settings(api, return_fields_by_field_id=True)), {"max_records": 1})
        assert by_id["records"][0]["fields"]["fldFixture0000001"] == "Ada"
        assert api.calls[-1]["query"]["returnFieldsByFieldId"] == ["true"]
        # Pagination: pages of 10, a shorter last page for max_records, and an exact continuation offset.
        paged(api)
        pages = logic.configuration(settings(api, table="Paged", page_size=10))
        before = len(api.calls)
        first = run(pages, {"max_records": 15, "request_id": "p-1"})
        assert [call["query"]["pageSize"] for call in api.calls[before:]] == [["10"], ["5"]]
        assert [item["fields"]["n"] for item in first["records"]] == list(range(1, 16))
        assert first["result"]["stop_reason"] == "limit" and first["result"]["truncated"] and first["result"]["pages"] == 2
        assert first["result"]["offset"].endswith("/recFixture0000019") and first["result"]["request_id"] == "p-1"
        rest = run(pages, {"offset": first["result"]["offset"]})
        assert [item["fields"]["n"] for item in rest["records"]] == list(range(16, 26)), "no record skipped or repeated"
        assert rest["result"]["stop_reason"] == "end" and rest["result"]["offset"] is None
        capped = run(logic.configuration(settings(api, table="Paged", page_size=10, max_pages=2)), "go")
        assert capped["result"]["count"] == 20 and capped["result"]["stop_reason"] == "max_pages" and capped["result"]["truncated"]
        tail = run(pages, {"offset": capped["result"]["offset"]})
        assert [item["fields"]["n"] for item in tail["records"]] == [21, 22, 23, 24, 25]
        exact = run(pages, {"max_records": 25})
        assert exact["result"]["count"] == 25 and exact["result"]["stop_reason"] == "end" and exact["result"]["offset"] is None
        # The output limit keeps whole pages only, so the returned offset re-reads the page that did not fit.
        limit = logic.OUTPUT_LIMIT
        try:
            logic.OUTPUT_LIMIT = 400
            sized = run(logic.configuration(settings(api, table="Paged", page_size=2)), "go")
            assert sized["result"]["stop_reason"] == "output_limit" and sized["result"]["count"] == 4
            following = run(logic.configuration(settings(api, table="Paged", page_size=2)), {"offset": sized["result"]["offset"]})
            assert following["records"][0]["fields"]["n"] == 5
            logic.OUTPUT_LIMIT = 50
            failure(lambda: run(pages, "go"), "output_too_large")
        finally:
            logic.OUTPUT_LIMIT = limit
        # A URL above 16,000 characters switches to POST .../listRecords with the same parameters as JSON.
        long_formula = "{Name}" + " " * 15900 + "= 'Ada'"
        before = len(api.calls)
        posted = run(config, {"filter_by_formula": long_formula, "fields": ["Name"], "sort": [{"field": "Name"}]})
        assert names(posted) == ["Ada"] and posted["result"]["post_list_requests"] == 1
        call = api.calls[before]
        assert call["method"] == "POST" and call["path"].endswith("/" + TABLE + "/listRecords") and call["kind"] == "listpost"
        assert call["body"] == {"pageSize": 100, "filterByFormula": long_formula, "fields": ["Name"],
                                "sort": [{"field": "Name", "direction": "asc"}]}
        assert all(call["url_length"] < logic.URL_LIMIT for call in api.calls)
        # get_record and provider refusals for reads.
        one = run(config, {"operation": "get_record", "record_id": "recFixture0000003", "request_id": "g-1"})
        assert one["records"] == [read["records"][2]] and one["result"]["record_id"] == "recFixture0000003"
        assert api.calls[-1]["path"] == "/v0/" + BASE + "/" + TABLE + "/recFixture0000003"
        assert run(logic.configuration(settings(api, operation="get_record", return_fields_by_field_id=True)),
                   "recFixture0000001")["records"][0]["fields"]["fldFixture0000002"] == "ada@example.test"
        missing = failure(lambda: run(config, {"operation": "get_record", "record_id": "recMissingRecord1"}), "not_found")
        assert missing.provider_type == "NOT_FOUND" and missing.http_status == 404
        table = failure(lambda: run(config, {"table": "Missing"}), "not_found")
        assert table.provider_type == "TABLE_NOT_FOUND"
        api.bases[BASE]["Sales 2024/Q1"] = Table("Sales 2024/Q1", "tblSalesFixture01", {"Name": "text"}, [{"Name": "Q"}],
                                                 api.counter)
        assert names(run(config, {"table": "Sales 2024/Q1"})) == ["Q"]
        assert api.calls[-1]["path"] == "/v0/" + BASE + "/Sales%202024%2FQ1"
        for request, kind in (({"fields": ["Nope"]}, "UNKNOWN_FIELD_NAME"), ({"filter_by_formula": "LEN("}, "INVALID_FILTER_BY_FORMULA"),
                              ({"offset": "itrUnknownIter001/recFixture0000001"}, "LIST_RECORDS_ITERATOR_NOT_AVAILABLE"),
                              ({"view": "Nope"}, "VIEW_NAME_NOT_FOUND")):
            refused = failure(lambda request=request: run(config, request), "invalid_request")
            assert refused.provider_type == kind and refused.http_status == 422 and not refused.uncertain
        assert run(logic.configuration(settings(api, table="Empty")), "go")["result"]["count"] == 0
    print("[ok] Airtable list pages, continuation offsets, views, formulas, POST listRecords and get_record", flush=True)


def write_protocol():
    """# - FB3 - create, update, upsert and delete in batches of 10; partial progress is reported, never replayed."""
    with airtable_api() as api:
        config = logic.configuration(settings(api, allow_writes=True, allow_delete=True, fields_to_merge_on=["Email"],
                                              allowed_operations=ALL_WRITES))
        many = [{"fields": {"Name": "P" + str(index), "Email": f"p{index}@example.test", "Amount": index}} for index in range(23)]
        before = len(api.calls)
        created = run(config, {"operation": "create_records", "records": many, "request_id": "c-1"})
        calls = api.calls[before:]
        assert [call["kind"] for call in calls] == ["create"] * 3 and [len(call["body"]["records"]) for call in calls] == [10, 10, 3]
        assert all(call["method"] == "POST" and call["body"]["typecast"] is False for call in calls)
        assert created["result"]["count"] == 23 and created["result"]["batches"] == 3 and created["result"]["request_id"] == "c-1"
        assert created["result"]["record_ids"] == [item["id"] for item in created["records"]]
        assert [item["fields"]["Name"] for item in created["records"]] == ["P" + str(index) for index in range(23)]
        assert len(api.table().records) == 26
        before = len(api.calls)
        strict = failure(lambda: run(config, {"operation": "create_records", "record": {"fields": {"Name": "T", "Amount": "12"}}}),
                         "invalid_request")
        assert strict.provider_type == "INVALID_VALUE_FOR_COLUMN" and len(api.table().records) == 26
        assert strict.envelope()["completed_batches"] == 0 and strict.envelope()["committed_record_ids"] == []
        typed = run(config, {"operation": "create_records", "typecast": True,
                             "record": {"fields": {"Name": "T", "Amount": "12", "Status": "vip"}}})
        assert typed["records"][0]["fields"]["Amount"] == 12 and api.calls[-1]["body"]["typecast"] is True
        ada = "recFixture0000001"
        updated = run(config, {"operation": "update_records", "records": [{"id": ada, "fields": {"Status": "active"}}]})
        assert api.calls[-1]["method"] == "PATCH" and api.calls[-1]["kind"] == "update"
        assert api.calls[-1]["body"] == {"records": [{"id": ada, "fields": {"Status": "active"}}], "typecast": False}
        assert updated["records"][0]["fields"] == {"Name": "Ada", "Email": "ada@example.test", "Status": "active", "Amount": 120}
        assert updated["result"]["record_ids"] == [ada], "PATCH keeps the fields that were not given"
        failure(lambda: run(config, {"operation": "update_records", "records": [{"id": "recMissingRecord1", "fields": {"Name": "x"}}]}),
                "not_found")
        upsert = run(config, {"operation": "upsert_records", "records": [
            {"fields": {"Email": "ada@example.test", "Status": "lost"}}, {"fields": {"Email": "new@example.test", "Name": "New"}}]})
        body = api.calls[-1]["body"]
        assert api.calls[-1]["method"] == "PATCH" and body["performUpsert"] == {"fieldsToMergeOn": ["Email"]}
        result = upsert["result"]
        assert result["updated_record_ids"] == [ada] and len(result["created_record_ids"]) == 1
        assert result["fields_to_merge_on"] == ["Email"] and api.table().records[ada]["fields"]["Status"] == "lost"
        named = run(config, {"operation": "upsert_records", "fields_to_merge_on": ["Name"],
                             "record": {"fields": {"Name": "New", "Amount": 5}}})
        assert named["result"]["updated_record_ids"] == result["created_record_ids"]
        doomed = created["result"]["record_ids"][:12]
        before = len(api.calls)
        removed = run(config, {"operation": "delete_records", "record_ids": doomed})
        calls = api.calls[before:]
        assert [call["method"] for call in calls] == ["DELETE", "DELETE"] and [len(call["query"]["records[]"]) for call in calls] == [10, 2]
        assert removed["result"]["deleted_record_ids"] == doomed and "records" not in removed
        assert not set(doomed) & set(api.table().records)
        # A refused second batch leaves the first one written and says so.
        api.script["create"] = ["pass", {"status": 422, "body": error("INVALID_RECORDS", "Bad record")}]
        before_records = set(api.table().records)
        partial = failure(lambda: run(config, {"operation": "create_records", "records": many[:15]}), "invalid_request")
        envelope = partial.envelope()
        added = set(api.table().records) - before_records
        assert envelope["completed_batches"] == 1 and envelope["total_batches"] == 2 and not envelope["uncertain"]
        assert set(envelope["committed_record_ids"]) == added and len(added) == 10
        assert "committed_record_ids" in envelope["message"]
        # A lost connection on a later batch is uncertain and never replayed.
        api.script["update"] = ["pass", "lost"]
        targets = [{"id": ident, "fields": {"Amount": 1}} for ident in created["result"]["record_ids"][12:23]]
        before = len(api.calls)
        lost = failure(lambda: run(config, {"operation": "update_records", "records": targets}), "network")
        assert lost.uncertain and lost.envelope()["completed_batches"] == 1 and len(lost.envelope()["committed_record_ids"]) == 10
        assert api.kinds()[before:] == ["update", "update"]
        api.script["delete"] = ["pass", {"status": 404, "body": {"error": "NOT_FOUND"}}]
        gone = failure(lambda: run(config, {"operation": "delete_records", "record_ids": created["result"]["record_ids"][12:23]}),
                       "not_found")
        assert gone.envelope()["committed_record_ids"] == created["result"]["record_ids"][12:22]
        # The write switches are enforced before any request.
        before = len(api.calls)
        failure(lambda: run(logic.configuration(settings(api)), {"operation": "create_records", "record": {"fields": {}}}),
                "operation_not_allowed")
        failure(lambda: logic.parse_request({"operation": "delete_records", "record_id": ada},
                                            {**config, "allow_delete": False}), "delete_disabled")
        assert len(api.calls) == before
    print("[ok] Airtable batched create, update, upsert, delete and partial-progress reporting", flush=True)


def pacing_protocol():
    """# - FB4 - at most max_requests_per_sec requests per base in any one-second window, shared by the process."""
    with airtable_api() as api:
        paged(api)
        config = logic.configuration(settings(api, table="Paged", page_size=1))
        started = time.monotonic()
        result = run(config, {"max_records": 11})["result"]
        elapsed = time.monotonic() - started
        assert result["requests"] == 11 and elapsed >= 1.8, elapsed
        times = [call["at"] for call in api.calls]
        assert all(times[index + 5] - times[index] >= 0.8 for index in range(len(times) - 5)), times
        # Two sessions on the same base share one window.
        before, results = len(api.calls), []
        workers = [threading.Thread(target=lambda: results.append(run(config, {"max_records": 6})["result"]["count"]))
                   for _ in range(2)]
        started = time.monotonic()
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(30)
        assert results == [6, 6] and time.monotonic() - started >= 1.8
        times = [call["at"] for call in api.calls[before:]]
        assert len(times) == 12 and all(times[index + 5] - times[index] >= 0.8 for index in range(len(times) - 5)), times
        slow = logic.configuration(settings(api, table="Paged", page_size=1, max_requests_per_sec=2))
        before = len(api.calls)
        run(slow, {"max_records": 4})
        times = [call["at"] for call in api.calls[before:]]
        assert times[2] - times[0] >= 0.8 and times[3] - times[1] >= 0.8, times
    print("[ok] Airtable per-base pacing", flush=True)


def error_protocol():
    """# - FB4 - 401/403/429/5xx, lost connections, oversized bodies, cancellation, deadlines and TLS."""
    with airtable_api() as api:
        config = logic.configuration(settings(api, allow_writes=True, allowed_operations=["create_records"],
                                              max_retry_wait_sec=2))
        create = {"operation": "create_records", "record": {"fields": {"Name": "Z"}}}
        expired = failure(lambda: run(config, "go", EXPIRED), "unauthorized")
        assert expired.provider_type == "AUTHENTICATION_REQUIRED" and expired.http_status == 401 and not expired.uncertain
        denied = failure(lambda: run(logic.configuration(settings(api, base_id=OTHER_BASE)), "go"), "forbidden")
        assert denied.provider_type == "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND" and denied.http_status == 403
        # 429: Retry-After is honoured within bounds for reads and writes; without it Airtable asks for 30 s.
        api.script["list"] = [{"status": 429, "body": LIMIT_429, "headers": {"Retry-After": "1"}}]
        started = time.monotonic()
        assert run(config, "go")["result"]["requests"] == 2 and time.monotonic() - started >= 1
        api.script["create"] = [{"status": 429, "body": LIMIT_429, "headers": {"Retry-After": "1"}}]
        assert run(config, create)["result"]["count"] == 1 and api.kinds()[-2:] == ["create", "create"]
        api.script["list"] = [{"status": 429, "body": LIMIT_429}]
        limited = failure(lambda: run(config, "go"), "rate_limited")
        assert limited.retry_after == 30 and limited.attempts == 1 and limited.provider_type == "RATE_LIMIT_REACHED"
        api.script["list"] = [{"status": 429, "body": LIMIT_429, "headers": {"Retry-After": "1"}}] * 3
        assert failure(lambda: run(logic.configuration(settings(api, max_retries=0)), "go"), "rate_limited").attempts == 1
        api.script["list"] = []
        # Reads retry 5xx and lost connections; writes never replay an ambiguous outcome.
        api.script["list"] = ["html502", "lost"]
        before = len(api.calls)
        assert run(config, "go")["result"]["count"] == 4 and api.kinds()[before:] == ["list"] * 3
        for scripted, code in (({"status": 503, "body": error("SERVICE_UNAVAILABLE", "Try again later.")}, "provider_unavailable"),
                               ("html502", "provider_unavailable"), ("lost", "network"), ("oversized", "protocol"),
                               ("gzip", "protocol")):
            api.script["create"] = [scripted]
            before = len(api.calls)
            outcome = failure(lambda: run(logic.configuration({**settings(api, allow_writes=True,
                                                                          allowed_operations=["create_records"]),
                                                               "max_response_bytes": 65536}), create), code)
            assert outcome.uncertain and api.kinds()[before:] == ["create"], scripted
        api.script["list"] = ["oversized"]
        assert not failure(lambda: run(logic.configuration(settings(api, max_response_bytes=65536, max_retries=0)), "go"),
                           "protocol").uncertain
        api.script["list"] = [{"status": 400, "body": error("INVALID_REQUEST_UNKNOWN", "Invalid request")}]
        assert failure(lambda: run(config, "go"), "bad_request").provider_type == "INVALID_REQUEST_UNKNOWN"
        api.script["list"] = [{"status": 402, "body": error("PUBLIC_API_BILLING_LIMIT_EXCEEDED", "Billing limit")}]
        assert failure(lambda: run(config, "go"), "quota_exceeded").http_status == 402
        api.script["create"] = [{"status": 413, "body": error("REQUEST_TOO_LARGE", "Request body too large")}]
        assert not failure(lambda: run(config, create), "provider_too_large").uncertain
        # Cancellation and the deadline shut the socket down while Airtable holds the request.
        # Each case first lets the one-second pacing window drain, so the request is sent at once.
        time.sleep(1.1)
        api.script["create"] = ["hold"]
        started = time.monotonic()
        cancelled = failure(lambda: run(config, create, cancel=lambda: api.holding >= 1), "cancelled")
        assert cancelled.uncertain and time.monotonic() - started < 2.5
        api.release.set()
        time.sleep(0.2)
        api.release.clear()
        time.sleep(1.1)
        api.script["list"] = ["hold"]
        started = time.monotonic()
        quiet = failure(lambda: run(config, "go", cancel=lambda: api.holding >= 1), "cancelled")
        assert not quiet.uncertain and time.monotonic() - started < 2.5
        api.release.set()
        time.sleep(0.2)
        api.release.clear()
        time.sleep(1.1)
        api.script["create"] = ["hold"]
        started = time.monotonic()
        timed = failure(lambda: run(logic.configuration(settings(api, allow_writes=True, allowed_operations=["create_records"],
                                                                 timeout_sec=5)), create), "timeout")
        assert timed.uncertain and 4 <= time.monotonic() - started < 8
        api.release.set()
        time.sleep(0.2)
        assert not [thread for thread in threading.enumerate() if thread.name == "airtable-http"]
        assert not failure(lambda: run(logic.configuration(settings(api, api_base_url="http://127.0.0.1:9", max_retries=0)), "go"),
                           "network").uncertain
        before = len(api.calls)
        failure(lambda: run(config, "go", cancel=lambda: True), "cancelled")
        assert len(api.calls) == before
    with airtable_api(tls=True) as api:
        config = logic.configuration(settings(api))
        assert run(config, "go")["result"]["count"] == 3
        before = len(api.calls)
        untrusted = failure(lambda: run(logic.configuration(settings(api, ca_file="")), "go"), "tls")
        assert not untrusted.uncertain and len(api.calls) == before
    print("[ok] Airtable errors, retries, uncertainty, cancellation, deadline and TLS", flush=True)


def value(data, port):
    raw = data.get("output_values", {}).get("base:" + str(port), {}).get("value")
    return json.loads(raw) if raw else None


def wallet(server, secret):
    http_json(server.base_url, "/api/application/secrets/init", method="POST", payload={"password": "disposable-airtable-fixture"})
    http_json(server.base_url, "/api/application/secrets", method="POST",
              payload={"name": "airtable_fixture", "value": secret})


def graph_runs():
    """# - FB5 - real graphs in One Shot and Active Runtime for bundled, managed and linked packages, with prompt Stop."""
    block = AirtableBlock()
    for mode in ("centralized", "zeromq_active"):
        for origin in (None, "managed", "linked"):
            full = origin is None
            with airtable_api() as api, isolated_server() as server:
                node = block.build_node_payload(node_id="base", position={"x": 380, "y": 160}, config_overrides=settings(
                    api, allow_writes=True, allowed_operations=["list_records", "create_records"], timeout_sec=20))
                if origin:
                    model = install_test_package(server, "airtable", origin=origin)
                    node["block_version"] = model["version"]
                    for surface in ("modal", "inspector_panel", "node_card"):
                        html = surface_payload(server, model, node, surface=surface)["html"]
                        assert "{{" not in html and TOKEN not in html
                        if surface != "node_card":
                            assert 'data-block-config-field="allowed_operations"' in html
                wallet(server, json.dumps({"token": TOKEN}) if full else TOKEN)
                sinks = [display_node("records", "Records", 760, 40), display_node("result", "Result", 760, 200),
                         display_node("problem", "Problem", 760, 360)]
                edges = [data_edge("r", "base", 1, "records", 1), data_edge("s", "base", 2, "result", 1),
                         data_edge("e", "base", 3, "problem", 1)]
                if mode == "centralized":
                    def once(text, expect="success"):
                        graph = graph_payload("Airtable one shot", [text_node("source", "Source", text, 60, 160), node, *sinks],
                                              [data_edge("in", "source", 1, "base", 1), *edges])
                        project = create_project_api(server, document=graph)["project"]
                        created = create_run_api(server, graph, project_id=project["graph_id"], runtime_mode=mode)
                        data = wait_for_run_terminal(server, created["run_id"], timeout_sec=40)
                        assert data["status"] == expect, (origin, data.get("status"), data.get("logs"))
                        assert TOKEN not in json.dumps(data)
                        return data
                    data = once(json.dumps({"request_id": "q-1", "max_records": 2}))
                    assert [item["fields"]["Name"] for item in value(data, 1)] == ["Ada", "Grace"]
                    assert value(data, 2)["request_id"] == "q-1" and value(data, 2)["truncated"] is True
                    data = once(json.dumps({"operation": "create_records", "record": {"fields": {"Name": "Graph"}}}))
                    assert value(data, 2)["count"] == 1 and value(data, 1)[0]["fields"] == {"Name": "Graph"}
                    if full:
                        before = len(api.calls)
                        data = once(json.dumps({"operation": "delete_records", "record_id": "recFixture0000001"}))
                        assert value(data, 3)["code"] == "operation_not_allowed" and len(api.calls) == before
                        assert data["results"]["base"]["airtable"]["code"] == "operation_not_allowed"
                        http_json(server.base_url, "/api/application/secrets/lock", method="POST", payload={})
                        data = once("go")
                        assert value(data, 3)["code"] == "secret" and len(api.calls) == before
                        http_json(server.base_url, "/api/application/secrets/unlock", method="POST",
                                  payload={"password": "disposable-airtable-fixture"})
                        # Stop while Airtable holds a read: the run ends promptly and nothing is retried.
                        api.script["list"] = ["hold"]
                        graph = graph_payload("Airtable stop", [text_node("source", "Source", "go", 60, 160), node, *sinks],
                                              [data_edge("in", "source", 1, "base", 1), *edges])
                        project = create_project_api(server, document=graph)["project"]
                        rid = create_run_api(server, graph, project_id=project["graph_id"], runtime_mode=mode)["run_id"]
                        deadline = time.monotonic() + 20
                        while api.holding < 1 and time.monotonic() < deadline:
                            time.sleep(0.05)
                        assert api.holding == 1, "held read not reached"
                        held, started = len(api.calls), time.monotonic()
                        stop_run_api(server, rid)
                        stopped = wait_for_run_terminal(server, rid, timeout_sec=20)
                        assert time.monotonic() - started < 10, stopped.get("logs")
                        assert stopped["status"] == "cancelled", (stopped["status"], value(stopped, 3), stopped.get("logs"))
                        assert value(stopped, 1) is None and value(stopped, 2) is None, "a stopped read publishes no records"
                        assert len(api.calls) == held, "the held read is neither retried nor followed by another request"
                        assert "shutdown_timeout" not in json.dumps(stopped.get("logs"))
                        api.release.set()
                else:
                    graph = graph_payload("Airtable active", [text_node("cmd", "Command", "", 60, 160), node, *sinks],
                                          [data_edge("in", "cmd", 1, "base", 1), *edges])
                    project = create_project_api(server, document=graph)["project"]

                    def publish(rid, text):
                        http_json(server.base_url, f"/api/runs/{rid}/active/control", method="POST", payload={
                            "action": "publish_output", "node_id": "cmd", "port_id": 1, "value": text,
                            "content_type": "text/plain"})

                    def wait(rid, predicate, label):
                        data = wait_for_run_predicate(server, rid, lambda item: item.get("status") == "failed" or predicate(item),
                                                      label, timeout_sec=30)
                        assert data.get("status") != "failed", data.get("logs")
                        assert TOKEN not in json.dumps(data)
                        return data

                    def stop(rid):
                        started = time.monotonic()
                        stop_run_api(server, rid)
                        stopped = wait_for_run_terminal(server, rid, timeout_sec=20)
                        assert time.monotonic() - started < 10, stopped.get("logs")
                        assert "shutdown_timeout" not in json.dumps(stopped.get("logs"))
                        return stopped

                    rid = prepare_release_run(server, project["graph_id"], graph)["run_id"]
                    http_json(server.base_url, f"/api/runs/{rid}/play", method="POST", payload={})
                    try:
                        publish(rid, json.dumps({"request_id": "live-1", "max_records": 3}))
                        data = wait(rid, lambda item: (value(item, 2) or {}).get("request_id") == "live-1", "read missing")
                        assert [item["id"] for item in value(data, 1)] == ["recFixture0000001", "recFixture0000002",
                                                                           "recFixture0000003"]
                        publish(rid, json.dumps({"request_id": "live-2", "operation": "create_records",
                                                 "records": [{"fields": {"Name": "Live"}}]}))
                        data = wait(rid, lambda item: (value(item, 2) or {}).get("request_id") == "live-2", "create missing")
                        assert value(data, 2)["count"] == 1 and value(data, 1)[0]["fields"]["Name"] == "Live"
                        if full:
                            publish(rid, json.dumps({"request_id": "live-3", "table": "Missing"}))
                            data = wait(rid, lambda item: (value(item, 3) or {}).get("request_id") == "live-3", "error missing")
                            assert value(data, 3)["code"] == "not_found"
                            api.script["list"] = ["hold"]
                            publish(rid, json.dumps({"request_id": "live-4"}))
                            deadline = time.monotonic() + 20
                            while api.holding < 1 and time.monotonic() < deadline:
                                time.sleep(0.05)
                            assert api.holding == 1, "held read not reached"
                    finally:
                        stop(rid)
                        api.release.set()
                assert TOKEN not in "\n".join(server.logs)
            print("[ok] Airtable graph " + mode + " " + str(origin), flush=True)


def main():
    read_protocol()
    write_protocol()
    pacing_protocol()
    error_protocol()
    graph_runs()


if __name__ == "__main__":
    main()
