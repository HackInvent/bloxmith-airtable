"""FB1/FB2/FB3/FB4/FB6: pure settings, requests, query strings, batches, provider errors and block hooks.

No socket is opened and no provider is contacted; the wallet resolver is a local double.
"""

import json
from pathlib import Path
import sys
from types import MappingProxyType, SimpleNamespace
from urllib.parse import parse_qsl

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests"), str(Path(__file__).resolve().parent)]
from blocs.airtable import logic, transport
from blocs.airtable.block import AirtableBlock
from bloxsmith_app.block_api import BlockRuntimeContext, BlockRuntimePreparationContext
from airtable_fixture import BASE, REF, TOKEN

BASE_SETTINGS = {"enabled": True, "secret_ref": REF, "base_id": BASE, "table": "Leads"}
WRITER = {**BASE_SETTINGS, "allow_writes": True, "fields_to_merge_on": ["Email"],
          "allowed_operations": ["list_records", "get_record", "create_records", "update_records", "upsert_records"]}
DELETER = {**WRITER, "allow_delete": True, "allowed_operations": [*WRITER["allowed_operations"], "delete_records"]}
REC = "recFixture0000001"
REC2 = "recFixture0000002"


def rejected(call, code=None):
    """A contract violation must raise a coded error, never fall back silently."""
    try:
        call()
    except logic.AirtableError as exc:
        assert code is None or exc.code == code, (code, exc.code, str(exc))
        assert TOKEN not in str(exc)
        return exc
    raise AssertionError("Invalid value accepted")


def settings_contract():
    """# - FB1 - remote access and writes stay off by default and every setting is bounded."""
    defaults = logic.configuration({})
    assert defaults["enabled"] is False and defaults["allow_writes"] is False and defaults["allow_delete"] is False
    assert defaults["api_base_url"] == logic.OFFICIAL_API == "https://api.airtable.com"
    assert defaults["typecast"] is False and defaults["max_requests_per_sec"] == 5 and defaults["page_size"] == 100
    assert logic.selectable_operations(defaults) == ["list_records"]
    assert set(AirtableBlock().default_config()) == set(logic.DEFAULTS)
    config = logic.configuration({**WRITER, "execution": {"inhibited": False}, "base_id": " " + BASE + " ",
                                  "table": " Leads ", "view": " Grid view "})
    assert config["base_id"] == BASE and config["table"] == "Leads" and config["view"] == "Grid view"
    assert logic.selectable_operations(config) == list(WRITER["allowed_operations"])
    frozen = logic.configuration(MappingProxyType({**WRITER, "allowed_operations": ("create_records",),
                                                   "fields_to_merge_on": ("Email", "Name")}))
    assert frozen["allowed_operations"] == ["create_records"] and frozen["fields_to_merge_on"] == ["Email", "Name"]
    assert logic.selectable_operations(frozen) == ["list_records", "create_records"]
    deleting = logic.configuration(DELETER)
    assert "delete_records" in logic.selectable_operations(deleting)
    for raw in ({"enabled": True}, {**BASE_SETTINGS, "secret_ref": ""}, {**BASE_SETTINGS, "base_id": ""},
                {**BASE_SETTINGS, "table": ""}, {**BASE_SETTINGS, "table": "   "},
                {**BASE_SETTINGS, "secret_ref": TOKEN}, {**BASE_SETTINGS, "secret_ref": "env://AIRTABLE"},
                {**BASE_SETTINGS, "token": TOKEN}, {**BASE_SETTINGS, "api_key": TOKEN},
                {**BASE_SETTINGS, "base_id": "https://airtable.com/" + BASE + "/tbl"}, {**BASE_SETTINGS, "base_id": "app123"},
                {**BASE_SETTINGS, "base_id": "tblLeadsFixture01"}, {**BASE_SETTINGS, "table": "x" * 256},
                {**BASE_SETTINGS, "table": "Tab\nTwo"}, {**BASE_SETTINGS, "view": "V\x00"}, {**BASE_SETTINGS, "table": 7},
                {**BASE_SETTINGS, "operation": "drop_table"}, {**BASE_SETTINGS, "operation": "create_records"},
                {**BASE_SETTINGS, "allowed_operations": ["upsert_records"]}, {**BASE_SETTINGS, "allowed_operations": []},
                {**BASE_SETTINGS, "allowed_operations": ["list_records", "list_records"]},
                {**BASE_SETTINGS, "allowed_operations": "list_records"}, {**WRITER, "allowed_operations": ["delete_records"]},
                {**BASE_SETTINGS, "allow_delete": True, "allowed_operations": ["delete_records"]},
                {**BASE_SETTINGS, "enabled": "yes"}, {**BASE_SETTINGS, "allow_writes": 1}, {**BASE_SETTINGS, "typecast": "false"},
                {**BASE_SETTINGS, "fields_to_merge_on": "Email"}, {**BASE_SETTINGS, "fields_to_merge_on": ["a", "b", "c", "d"]},
                {**BASE_SETTINGS, "fields_to_merge_on": ["Email", "Email"]}, {**BASE_SETTINGS, "fields_to_merge_on": [""]},
                {**BASE_SETTINGS, "fields_to_merge_on": [7]},
                {**BASE_SETTINGS, "ca_file": "ca.pem"}, {**BASE_SETTINGS, "ca_file": "/etc/../ca.pem"},
                {**BASE_SETTINGS, "api_base_url": "http://api.airtable.com"},
                {**BASE_SETTINGS, "api_base_url": "https://api.airtable.com/v0"},
                {**BASE_SETTINGS, "api_base_url": "https://user:pw@api.airtable.com"},
                {**BASE_SETTINGS, "api_base_url": "https://x.test/?a=1"}, {**BASE_SETTINGS, "api_base_url": "ftp://api.airtable.com"},
                {**BASE_SETTINGS, "api_base_url": "https://bad host"}, {**BASE_SETTINGS, "api_base_url": "https://-bad-.example"},
                {**BASE_SETTINGS, "api_base_url": "http://example.test:8080"}):
        rejected(lambda raw=raw: logic.configuration(raw), "config")
    for key, (low, high) in logic.BOUNDS.items():
        for bad in (low - 1, high + 1, True, float(low), str(low)):
            rejected(lambda key=key, bad=bad: logic.configuration({**BASE_SETTINGS, key: bad}), "config")
        assert logic.configuration({**BASE_SETTINGS, key: low})[key] == low
        assert logic.configuration({**BASE_SETTINGS, key: high})[key] == high
    assert logic.BOUNDS["page_size"] == (1, 100) and logic.BOUNDS["max_requests_per_sec"] == (1, 5)
    origins = {"https://api.airtable.com/": "https://api.airtable.com", "http://127.0.0.1:8081": "http://127.0.0.1:8081",
               "http://localhost:9": "http://localhost:9", "http://[::1]:7": "http://[::1]:7",
               "https://Gateway.Example.test:8443": "https://gateway.example.test:8443"}
    for raw, expected in origins.items():
        assert logic.configuration({**BASE_SETTINGS, "api_base_url": raw})["api_base_url"] == expected


def credential_contract():
    """# - FB1 - # - FB4 - wallet values: bare token, {token} or {access_token}; never echoed."""
    assert logic.credentials(" " + TOKEN + "\n") == TOKEN
    assert logic.credentials({"token": TOKEN}) == TOKEN
    assert logic.credentials(json.dumps({"access_token": TOKEN})) == TOKEN
    for bad in ("", None, 12, {"token": ""}, {"token": "a b"}, {"token": TOKEN, "extra": "x"}, {"api_key": TOKEN},
                '{"token": ', json.dumps({"token": 5}), {"token": TOKEN, "access_token": TOKEN}, "x" * 9000):
        error = rejected(lambda bad=bad: logic.credentials(bad), "secret")
        assert TOKEN not in json.dumps(error.envelope())
    text = logic.redact("Bearer " + TOKEN + " again " + TOKEN, [TOKEN, ""])
    assert TOKEN not in text and text.count("***") == 2


def request_contract():
    """# - FB2 - # - FB3 - one explicit request; the operation allowlist and write switches apply before the wallet."""
    reader = logic.configuration({**BASE_SETTINGS, "view": "Grid view",
                                  "allowed_operations": ["list_records", "get_record"]})
    writer = logic.configuration(WRITER)
    deleter = logic.configuration(DELETER)
    trigger = logic.parse_request("go", reader)
    assert trigger == {"operation": "list_records", "request_id": None, "table": "Leads", "view": "Grid view",
                       "filter_by_formula": "", "fields": [], "sort": [], "max_records": 1000, "page_size": 100,
                       "offset": None}
    listed = logic.parse_request({"request_id": "r-1", "table": "tblLeadsFixture01", "view": "", "fields": ["Name", "fldX"],
                                  "filter_by_formula": " {Status} = 'new'\n", "max_records": 5, "page_size": 2,
                                  "sort": [{"field": "Name"}, {"field": "Amount", "direction": "desc"}],
                                  "offset": "itrFixture0000001/recFixture0000003"}, reader)
    assert listed["table"] == "tblLeadsFixture01" and listed["view"] == "" and listed["filter_by_formula"] == "{Status} = 'new'"
    assert listed["sort"] == [{"field": "Name", "direction": "asc"}, {"field": "Amount", "direction": "desc"}]
    assert (listed["max_records"], listed["page_size"], listed["request_id"]) == (5, 2, "r-1")
    assert logic.parse_request({"operation": "get_record", "record_id": REC}, reader)["record_id"] == REC
    as_get = logic.configuration({**BASE_SETTINGS, "operation": "get_record"})
    assert logic.parse_request(" " + REC + "\n", as_get) == {"operation": "get_record", "request_id": None,
                                                             "table": "Leads", "record_id": REC}
    assert logic.parse_request(REC, reader)["operation"] == "list_records", "a record ID only addresses get/delete"
    created = logic.parse_request([{"fields": {"Name": "Bob", "Tags": ["a", "b"], "Owner": {"email": "x@example.test"}}}],
                                  logic.configuration({**WRITER, "operation": "create_records"}))
    assert created["operation"] == "create_records" and created["typecast"] is False
    assert created["records"] == [{"fields": {"Name": "Bob", "Tags": ["a", "b"], "Owner": {"email": "x@example.test"}}}]
    assert logic.parse_request({"operation": "create_records", "record": {"fields": {}}}, writer)["records"] == [{"fields": {}}]
    updated = logic.parse_request({"operation": "update_records", "typecast": True,
                                   "records": [{"id": REC, "fields": {"Status": "done"}}]}, writer)
    assert updated["records"] == [{"id": REC, "fields": {"Status": "done"}}] and updated["typecast"] is True
    upsert = logic.parse_request({"operation": "upsert_records", "record": {"fields": {"Email": "a@example.test", "Name": "A"}}},
                                 writer)
    assert upsert["fields_to_merge_on"] == ["Email"]
    custom = logic.parse_request({"operation": "upsert_records", "fields_to_merge_on": ["Name", "Email"],
                                  "records": [{"fields": {"Email": "a@example.test", "Name": "A"}}]}, writer)
    assert custom["fields_to_merge_on"] == ["Name", "Email"]
    removal = logic.parse_request({"operation": "delete_records", "record_ids": [REC, REC2]}, deleter)
    assert removal["record_ids"] == [REC, REC2]
    assert logic.parse_request({"operation": "delete_records", "record_id": REC}, deleter)["record_ids"] == [REC]
    as_delete = logic.configuration({**DELETER, "operation": "delete_records"})
    assert logic.parse_request(REC, as_delete)["record_ids"] == [REC]
    tiny = logic.configuration({**BASE_SETTINGS, "max_request_bytes": 1024})
    small = logic.configuration({**WRITER, "max_write_records": 3})
    no_table = logic.configuration({**BASE_SETTINGS, "enabled": False, "table": ""})
    for config, value, code in (
            (reader, {"operation": "create_records", "record": {"fields": {}}}, "operation_not_allowed"),
            (reader, {"operation": "drop"}, "invalid_input"), (reader, {"max_records": 1001}, "invalid_input"),
            (reader, {"max_records": 0}, "invalid_input"), (reader, {"page_size": 101}, "invalid_input"),
            (reader, {"page_size": True}, "invalid_input"), (reader, {"fields": []}, "invalid_input"),
            (reader, {"fields": "Name"}, "invalid_input"), (reader, {"fields": ["Name", "Name"]}, "invalid_input"),
            (reader, {"fields": ["x" * 256]}, "invalid_input"), (reader, {"sort": {"field": "Name"}}, "invalid_input"),
            (reader, {"sort": [{"field": "Name", "direction": "up"}]}, "invalid_input"),
            (reader, {"sort": [{"direction": "asc"}]}, "invalid_input"),
            (reader, {"sort": [{"field": "Name", "order": 1}]}, "invalid_input"),
            (reader, {"sort": [{"field": "a"}] * 21}, "invalid_input"),
            (reader, {"filter_by_formula": "x" * 16001}, "invalid_input"), (reader, {"filter_by_formula": "a\x01"}, "invalid_input"),
            (reader, {"filter_by_formula": 3}, "invalid_input"), (reader, {"offset": "has space"}, "invalid_input"),
            (reader, {"offset": 5}, "invalid_input"), (reader, {"table": ""}, "invalid_input"),
            (reader, {"view": "a\nb"}, "invalid_input"), (reader, {"record_id": REC}, "invalid_input"),
            (reader, {"operation": "get_record"}, "invalid_input"),
            (reader, {"operation": "get_record", "record_id": "rec123"}, "invalid_input"),
            (reader, '{"max_records": 1, "max_records": 2}', "invalid_input"), (reader, "{broken", "invalid_input"),
            (reader, "[1]", "invalid_input"), (reader, {"request_id": "bad id"}, "invalid_input"),
            (no_table, "go", "invalid_input"),
            (writer, {"operation": "create_records"}, "invalid_input"),
            (writer, {"operation": "create_records", "records": []}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"Name": "flat"}]}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"id": REC, "fields": {}}]}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"fields": []}]}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"fields": {"": 1}}]}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"fields": {"a": 1}}], "record": {"fields": {}}}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"fields": {"a": 1}}], "typecast": "yes"}, "invalid_input"),
            (writer, {"operation": "create_records", "records": [{"fields": {"a": 1}}], "max_records": 2}, "invalid_input"),
            (small, {"operation": "create_records", "records": [{"fields": {}}] * 4}, "too_many_records"),
            (writer, {"operation": "update_records", "records": [{"fields": {"a": 1}}]}, "invalid_input"),
            (writer, {"operation": "update_records", "records": [{"id": "recBad", "fields": {"a": 1}}]}, "invalid_input"),
            (writer, {"operation": "update_records", "records": [{"id": REC, "fields": {}}]}, "invalid_input"),
            (writer, {"operation": "update_records", "records": [{"id": REC, "fields": {"a": 1}}] * 2}, "invalid_input"),
            (writer, {"operation": "upsert_records", "records": [{"fields": {"Name": "no email"}}]}, "invalid_input"),
            (writer, {"operation": "upsert_records", "records": [{"fields": {"Email": None}}]}, "invalid_input"),
            (writer, {"operation": "upsert_records", "fields_to_merge_on": [], "records": [{"fields": {"Email": "x"}}]},
             "invalid_input"),
            (logic.configuration({**WRITER, "fields_to_merge_on": []}),
             {"operation": "upsert_records", "records": [{"fields": {"Email": "x"}}]}, "invalid_input"),
            (writer, {"operation": "upsert_records", "fields_to_merge_on": ["a", "b", "c", "d"],
                      "records": [{"fields": {"a": 1}}]}, "invalid_input"),
            (writer, {"operation": "delete_records", "record_id": REC}, "operation_not_allowed"),
            (deleter, {"operation": "delete_records"}, "invalid_input"),
            (deleter, {"operation": "delete_records", "record_ids": [REC, REC]}, "invalid_input"),
            (deleter, {"operation": "delete_records", "record_ids": [REC], "record_id": REC}, "invalid_input"),
            (deleter, {"operation": "delete_records", "record_ids": ["nope"]}, "invalid_input"),
            (logic.configuration({**DELETER, "max_write_records": 1}), {"operation": "delete_records",
                                                                        "record_ids": [REC, REC2]}, "too_many_records"),
            (tiny, {"filter_by_formula": "x" * 2000}, "request_too_large"), (tiny, "y" * 2000, "request_too_large")):
        rejected(lambda config=config, value=value: logic.parse_request(value, config), code)
    # A tampered configuration (validation bypassed) still cannot write or delete.
    rejected(lambda: logic.parse_request({"operation": "create_records", "record": {"fields": {}}},
                                         {**writer, "allow_writes": False}), "writes_disabled")
    rejected(lambda: logic.parse_request({"operation": "delete_records", "record_id": REC},
                                         {**deleter, "allow_delete": False}), "delete_disabled")


def encoding_contract():
    """# - FB2 - # - FB3 - Airtable parameter names, percent-encoded tables, batches of 10 sized before any write."""
    assert logic.table_path(BASE, "Leads") == "/v0/" + BASE + "/Leads"
    assert logic.table_path(BASE, "Sales 2024/Q1 ?#") == "/v0/" + BASE + "/Sales%202024%2FQ1%20%3F%23"
    assert logic.table_path(BASE, "Leads", REC) == "/v0/" + BASE + "/Leads/" + REC
    config = logic.configuration({**BASE_SETTINGS, "return_fields_by_field_id": True})
    request = logic.parse_request({"view": "Active", "filter_by_formula": "{Status} = 'new'", "fields": ["Name", "E mail"],
                                   "sort": [{"field": "Name", "direction": "desc"}]}, config)
    params = logic.list_parameters(request, config, 7, "itrFixture0000001/recFixture0000003")
    assert params == {"pageSize": 7, "offset": "itrFixture0000001/recFixture0000003", "view": "Active",
                      "filterByFormula": "{Status} = 'new'", "fields": ["Name", "E mail"],
                      "sort": [{"field": "Name", "direction": "desc"}], "returnFieldsByFieldId": True}
    assert parse_qsl(logic.list_query(params)) == [
        ("pageSize", "7"), ("offset", "itrFixture0000001/recFixture0000003"), ("view", "Active"),
        ("filterByFormula", "{Status} = 'new'"), ("fields[]", "Name"), ("fields[]", "E mail"),
        ("sort[0][field]", "Name"), ("sort[0][direction]", "desc"), ("returnFieldsByFieldId", "true")]
    plain = logic.parse_request("go", logic.configuration(BASE_SETTINGS))
    assert logic.list_parameters(plain, logic.configuration(BASE_SETTINGS), 100, None) == {"pageSize": 100}
    assert [len(chunk) for chunk in logic.batches(list(range(23)))] == [10, 10, 3]
    assert logic.delete_query([REC, REC2]) == "records%5B%5D=" + REC + "&records%5B%5D=" + REC2
    writer = logic.configuration({**WRITER, "allowed_operations": ["create_records", "upsert_records"]})
    records = [{"fields": {"Email": f"p{index}@example.test"}} for index in range(12)]
    bodies = logic.write_bodies(logic.parse_request({"operation": "upsert_records", "records": records, "typecast": True},
                                                    writer), writer)
    assert len(bodies) == 2
    first = json.loads(bodies[0])
    assert first == {"performUpsert": {"fieldsToMergeOn": ["Email"]}, "records": records[:10], "typecast": True}
    assert json.loads(bodies[1])["records"] == records[10:]
    created = json.loads(logic.write_bodies(logic.parse_request({"operation": "create_records", "records": records[:1]},
                                                                writer), writer)[0])
    assert created == {"records": records[:1], "typecast": False}
    # Every batch body is built and sized before the first write: an oversized second batch refuses them all.
    late = [{"fields": {"Name": "a"}}] * 10 + [{"fields": {"Name": "x" * 3000}}]
    parsed = logic.parse_request({"operation": "create_records", "records": late}, writer)
    assert len(logic.write_bodies(parsed, writer)) == 2
    rejected(lambda: logic.write_bodies(parsed, {**writer, "max_request_bytes": 2048}), "request_too_large")


def provider_contract():
    """# - FB4 - Airtable error envelopes map to stable codes; writes after 5xx are uncertain; nothing leaks."""
    secrets = [TOKEN]
    bodies = ({"error": {"type": "SOME_TYPE", "message": "Detail with " + TOKEN + "\n" + "x" * 400}},
              {"error": "SOME_TYPE", "message": "Detail with " + TOKEN},
              {"errors": [{"error": "SOME_TYPE", "message": "Detail with " + TOKEN}]})
    for status, code in ((400, "bad_request"), (401, "unauthorized"), (402, "quota_exceeded"), (403, "forbidden"),
                         (404, "not_found"), (409, "provider_error"), (413, "provider_too_large"),
                         (422, "invalid_request"), (429, "rate_limited"), (500, "provider_unavailable"),
                         (503, "provider_unavailable")):
        for body in bodies:
            for mutation in (False, True):
                error = rejected(lambda status=status, body=body, mutation=mutation: logic.provider_failure(
                    status, body, secrets, mutation=mutation, retry_after=7), code)
                envelope = error.envelope(operation="create_records", request_id=None)
                assert envelope["http_status"] == status and envelope["provider_type"] == "SOME_TYPE"
                assert len(envelope["provider_message"]) <= 200 and "\n" not in envelope["provider_message"]
                assert TOKEN not in json.dumps(envelope) and "request_id" not in envelope
                assert error.uncertain is (mutation and status >= 500)
                assert envelope.get("retry_after") == (7 if status == 429 else None)
    waited = rejected(lambda: logic.provider_failure(429, bodies[2], secrets, mutation=False), "rate_limited")
    assert waited.retry_after == logic.RATE_LIMIT_WAIT == 30, "no Retry-After: Airtable documents a 30 s wait"
    bare = rejected(lambda: logic.provider_failure(502, None, secrets, mutation=False), "provider_unavailable")
    assert bare.provider_type is None and bare.provider_message is None
    raw = {"id": REC, "createdTime": "2026-01-02T10:00:00.000Z", "fields": {"Name": "Ada"}, "commentCount": 2}
    assert logic.record(raw) == {"id": REC, "created_time": "2026-01-02T10:00:00.000Z", "fields": {"Name": "Ada"},
                                 "comment_count": 2}
    assert logic.record({"id": REC})["fields"] == {}
    for bad in ({"id": "nope", "fields": {}}, {"fields": {}}, {"id": REC, "fields": []}, "x"):
        assert not rejected(lambda bad=bad: logic.record(bad), "protocol").uncertain
        assert rejected(lambda bad=bad: logic.record(bad, mutation=True), "protocol").uncertain
    items, offset = logic.page({"records": [raw], "offset": "itrA/recB"}, 1)
    assert items[0]["id"] == REC and offset == "itrA/recB"
    assert logic.page({"records": []}, 5) == ([], None)
    for bad in ({"records": [raw, raw]}, {"records": "x"}, {}, {"records": [], "offset": 7}, {"records": [], "offset": "a b"}):
        rejected(lambda bad=bad: logic.page(bad, 1), "protocol")
    assert [item["id"] for item in logic.written({"records": [raw]}, 1, ids=[REC])] == [REC]
    for bad, ids in (({"records": []}, None), ({"records": [raw]}, [REC2]), ({}, None)):
        assert rejected(lambda bad=bad, ids=ids: logic.written(bad, 1, ids=ids), "protocol").uncertain
    returned = logic.written({"records": [raw]}, 1)
    assert logic.upserted({"createdRecords": [], "updatedRecords": [REC]}, returned) == ([], [REC])
    assert rejected(lambda: logic.upserted({"createdRecords": [REC2], "updatedRecords": []}, returned), "protocol").uncertain
    assert rejected(lambda: logic.upserted({"records": [raw]}, returned), "protocol").uncertain
    assert logic.deleted({"records": [{"id": REC, "deleted": True}]}, [REC]) == [REC]
    assert rejected(lambda: logic.deleted({"records": [{"id": REC, "deleted": False}]}, [REC]), "protocol").uncertain
    # Pacing: at most `rate` slots in any one-second window per base, shared by the process.
    key = ("https://pace.test", "appPaceFixture001")
    assert [transport.pace_slot(key, 2, now=100.0), transport.pace_slot(key, 2, now=100.2)] == [0.0, 0.0]
    assert abs(transport.pace_slot(key, 2, now=100.5) - 0.5) < 1e-9
    assert transport.pace_slot(key, 2, now=101.01) == 0.0
    assert abs(transport.pace_slot(key, 2, now=101.05) - 0.15) < 1e-9
    assert transport.pace_slot(("https://pace.test", "appOtherFixture01"), 2, now=101.05) == 0.0


def block_contract():
    """# - FB1 - # - FB4 - # - FB6 - block hooks fail before side effects, and surfaces stay bounded and translated."""
    block = AirtableBlock()
    ports_in = tuple(SimpleNamespace(id=port["id"], name=port["name"]) for port in block.model["ports"]["inputs"])
    ports_out = tuple(SimpleNamespace(id=port["id"], name=port["name"]) for port in block.model["ports"]["outputs"])
    assert [port.name for port in ports_out] == ["records", "result", "error"]
    assert all(port["execution_requirement"] == "required_for_execution" for port in block.model["ports"]["inputs"])
    calls = []

    def resolver(ref):
        calls.append(ref)
        return "not json {" + TOKEN

    def run(config, inputs):
        context = BlockRuntimeContext(config={**block.default_config(), **config}, inputs=inputs, input_ports=ports_in,
                                      output_ports=ports_out, services={"resolve_secret": resolver})
        result = block.execute_runtime(context)
        assert result.status in ("success", "skipped"), result
        return result, {output.port_name: json.loads(output.value) for output in result.outputs}

    assert run(BASE_SETTINGS, {})[0].status == "skipped"
    for config, inputs, code in (({**BASE_SETTINGS, "enabled": False}, {"request": "go"}, "disabled"),
                                 (BASE_SETTINGS, {"request": {"operation": "create_records", "record": {"fields": {}}}},
                                  "operation_not_allowed"),
                                 (BASE_SETTINGS, {"request": '{"max_records":'}, "invalid_input"),
                                 (WRITER, {"request": {"operation": "create_records", "records": [{"fields": {}}] * 101}},
                                  "too_many_records"),
                                 ({**BASE_SETTINGS, "secret_ref": TOKEN}, {"request": "go"}, "config")):
        result, outputs = run(config, inputs)
        assert set(outputs) == {"error"} and outputs["error"]["code"] == code, outputs
        assert result.metadata["airtable"] == {"status": "error", "code": code, "uncertain": False}
        assert TOKEN not in json.dumps(outputs) and TOKEN not in " ".join(result.logs)
    assert not calls, "The wallet must not be read before settings and request validation"
    result, outputs = run(BASE_SETTINGS, {"request": {"request_id": "req-1", "max_records": 3}})
    assert outputs["error"]["code"] == "secret" and outputs["error"]["request_id"] == "req-1" and calls == [REF]
    assert outputs["error"]["base_id"] == BASE and outputs["error"]["table"] == "Leads"
    assert TOKEN not in json.dumps(outputs) and result.logs == ["[airtable-error] list_records: secret"]

    def prepared(config):
        context = BlockRuntimePreparationContext.from_context(SimpleNamespace(config=config, runtime_mode="zeromq_active"))
        return block.prepare_runtime(context)

    assert prepared(WRITER).listen_on_run is False
    rejected(lambda: prepared({**BASE_SETTINGS, "secret_ref": TOKEN}), "config")
    node = block.build_node_payload(node_id="table", position={"x": 0, "y": 0})
    accepted = block.handle_ui_action(node=node, action="save_settings", values={"node_patch": {"config": WRITER}})
    operation = accepted["graph_operations"][-1]
    assert operation["op"] == "update_node_config" and operation["replace"] is True
    assert operation["config"]["allowed_operations"] == WRITER["allowed_operations"]
    assert operation["config"]["fields_to_merge_on"] == ["Email"]
    refused = block.handle_ui_action(node=node, action="save_settings",
                                     values={"node_patch": {"config": {**BASE_SETTINGS, "operation": "create_records"}}})
    assert "allow_writes" in refused["error"] and "graph_operations" not in refused
    html = block.render_modal(node=node)["html"]
    assert 'data-block-config-field="secret_ref"' in html and 'data-airtable-show="write" hidden' in html
    assert 'data-block-value-type="json"' in html and "{{" not in html
    writing = {**node, "config": {**node["config"], **WRITER}}
    assert 'data-airtable-show="write" hidden' not in block.render_inspector_panel(node=writing)["html"]
    card = block.render_node_card(node=node)["html"]
    assert card.count("Remote access off") == 1 and 'data-i18n="block.airtable.card_disabled" data-i18n-params=""' in card
    assert "Read only · list_records" in block.render_node_card(node={**node, "config": {**node["config"], **BASE_SETTINGS}})["html"]
    card = block.render_node_card(node=writing)["html"]
    assert ("Read and write · list_records" in card and 'data-i18n="block.airtable.card_write" '
            'data-i18n-params="{&quot;operation&quot;: &quot;list_records&quot;}"' in card)
    deleting = {**node, "config": {**node["config"], **DELETER, "operation": "delete_records"}}
    assert "Read, write and delete · delete_records" in block.render_node_card(node=deleting)["html"]
    assert REF not in card and BASE not in card
    french = block.translate("block.airtable.card_write", {"operation": "upsert_records"}, lang="fr")
    assert french == "Lecture et écriture · upsert_records"


def main():
    settings_contract()
    credential_contract()
    request_contract()
    encoding_contract()
    provider_contract()
    block_contract()
    print("[ok] Airtable settings, credentials, requests, encoding, batches, provider errors, pacing and hooks")


if __name__ == "__main__":
    main()
