"""Loopback-only Airtable Web API v0 double. No Airtable account or credential is used.

It reproduces the documented request/response shapes used by the block:
list records (GET with pageSize, offset, view, filterByFormula, fields[],
sort[n][field|direction], returnFieldsByFieldId, and POST .../listRecords with
the same parameters as JSON), get record, create (POST), update (PATCH),
upsert (PATCH with performUpsert/fieldsToMergeOn, createdRecords and
updatedRecords), delete (DELETE with records[]), the 10-records-per-request
limit, typecast, offsets as "itr.../rec...", empty fields omitted from
records, and the error envelopes {"error": {"type", "message"}},
{"error": "NOT_FOUND"} and {"errors": [{"error", "message"}]} on 429.
Only a tiny formula subset is understood: TRUE() and {Field} = 'text'.
"""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlsplit

BASE = "appFixtureBase001"
OTHER_BASE = "appMissingBase001"
TOKEN = "patFixtureToken01.fixture-airtable-token-not-a-real-one-0123456789"
EXPIRED = "patExpiredToken01.fixture-airtable-token-revoked-0123456789"
REF = "secret://workspace/airtable_fixture"
TABLE = "Leads"
TABLE_ID = "tblLeadsFixture01"
ACTIVE_VIEW = "viwActiveFixture1"
SCHEMA = {"Name": "text", "Email": "text", "Status": "select", "Amount": "number", "Done": "checkbox"}
PEOPLE = [{"Name": "Ada", "Email": "ada@example.test", "Status": "new", "Amount": 120},
          {"Name": "Grace", "Email": "grace@example.test", "Status": "active", "Amount": 75.5, "Done": True},
          {"Name": "Linus", "Email": "linus@example.test", "Status": "new", "Amount": 0}]
ITERATOR = re.compile(r"(itr[A-Za-z0-9]{14})/(rec[A-Za-z0-9]{14})\Z")
EQUALS = re.compile(r"""\{([^{}]+)\}\s*=\s*(?:'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)")\Z""")
LIMIT_429 = {"errors": [{"error": "RATE_LIMIT_REACHED", "message": "Rate limit exceeded. Please try again later"}]}


def settings(api, **extra):
    """Block settings aimed at this fixture only."""
    return {"enabled": True, "secret_ref": REF, "base_id": BASE, "table": TABLE, "api_base_url": api.base_url,
            "ca_file": api.ca_file, "timeout_sec": 20, **extra}


def error(status_type, message):
    return {"error": {"type": status_type, "message": message}}


class Table:
    """One table: a field schema, select options and records in creation order."""

    def __init__(self, title, ident, schema, rows, counter):
        self.title, self.id, self.schema, self.counter = title, ident, dict(schema), counter
        self.field_ids = {field: "fld" + f"Fixture{index:07d}" for index, field in enumerate(schema, 1)}
        self.options = {"Status": {"new", "active", "lost"}}
        self.records = {}
        for row in rows:
            self.add(dict(row))

    def add(self, fields):
        self.counter[0] += 1
        number = self.counter[0]
        ident = "rec" + f"Fixture{number:07d}"
        self.records[ident] = {"id": ident, "createdTime": f"2026-01-{1 + number % 28:02d}T10:00:00.000Z",
                               "fields": {key: value for key, value in fields.items()}}
        return ident

    def public(self, ident, by_id=False, only=None):
        """Records as Airtable returns them: empty values are left out of fields."""
        item = self.records[ident]
        fields = {}
        for key, value in item["fields"].items():
            if value is None or value is False or value == "" or value == [] or (only and key not in only):
                continue
            fields[self.field_ids[key] if by_id else key] = value
        return {"id": item["id"], "createdTime": item["createdTime"], "fields": fields}


def text_of(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return "" if value is None else str(value)


@contextmanager
def airtable_api(tls=False):
    """Run the double on a dynamic loopback port; every owned thread is joined on exit."""
    with tempfile.TemporaryDirectory() as folder:
        directory = Path(folder)
        lock = threading.Lock()
        api = SimpleNamespace(calls=[], script={}, release=threading.Event(), closing=threading.Event(),
                              tokens={TOKEN}, holding=0, iterators={}, counter=[0])
        api.bases = {BASE: {TABLE: Table(TABLE, TABLE_ID, SCHEMA, PEOPLE, api.counter),
                            "Empty": Table("Empty", "tblEmptyFixture01", {"Name": "text"}, [], api.counter)}}

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def reply(self, status, body, *, raw=None, content_type="application/json; charset=utf-8", headers=None):
                data = raw if raw is not None else json.dumps(body).encode("utf-8")
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Connection", "close")
                    for key, value in (headers or {}).items():
                        self.send_header(key, value)
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionError, ssl.SSLError):
                    pass
                finally:
                    self.close_connection = True

            def do_GET(self):
                self.dispatch("GET")

            def do_POST(self):
                self.dispatch("POST")

            def do_PATCH(self):
                self.dispatch("PATCH")

            def do_PUT(self):
                self.dispatch("PUT")

            def do_DELETE(self):
                self.dispatch("DELETE")

            def scripted(self, kind):
                """Apply one scripted action; return True when the reply has been handled."""
                queue = api.script.get(kind)
                if not queue:
                    return False
                action = queue.pop(0)
                if action == "pass":
                    return False
                if action == "hold":
                    with lock:
                        api.holding += 1
                    try:
                        api.release.wait(20)
                    finally:
                        with lock:
                            api.holding -= 1
                    return api.closing.is_set()
                if action == "lost":
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.close_connection = True
                    return True
                if action == "html502":
                    self.reply(502, None, raw=b"<html>Bad Gateway</html>", content_type="text/html")
                    return True
                if action == "oversized":
                    self.reply(200, {"records": [{"id": "rec" + "Oversized000001", "createdTime": "x",
                                                  "fields": {"Name": "y" * 70000}}]})
                    return True
                if action == "gzip":
                    self.reply(200, None, raw=b"\x1f\x8b", headers={"Content-Encoding": "gzip"})
                    return True
                self.reply(action["status"], action["body"], headers=action.get("headers"))
                return True

            def dispatch(self, method):
                length = int(self.headers.get("Content-Length", "0"))
                assert 0 <= length <= 16 * 1024 * 1024
                raw = self.rfile.read(length)
                parts = urlsplit(self.path)
                pairs = parse_qs(parts.query, keep_blank_values=True)
                call = {"method": method, "path": parts.path, "query": pairs, "at": time.monotonic(),
                        "url_length": len(self.path), "auth": self.headers.get("Authorization"),
                        "accept_encoding": self.headers.get("Accept-Encoding"),
                        "content_type": self.headers.get("Content-Type"), "kind": None, "table": None, "body": None}
                api.calls.append(call)
                match = re.fullmatch(r"/v0/([^/]+)/([^/]+)(?:/([^/]+))?", parts.path)
                if not match:
                    self.reply(404, {"error": "NOT_FOUND"})
                    return
                base, table, tail = match.group(1), unquote(match.group(2)), match.group(3)
                call["table"] = table
                if tail is None:
                    kind = {"GET": "list", "POST": "create", "PATCH": "update", "DELETE": "delete"}.get(method)
                elif tail == "listRecords" and method == "POST":
                    kind = "listpost"
                elif method == "GET":
                    kind = "get"
                else:
                    kind = None
                if kind is None:
                    self.reply(404, {"error": "NOT_FOUND"})
                    return
                if raw:
                    assert call["content_type"].startswith("application/json"), call["content_type"]
                    call["body"] = json.loads(raw)
                if kind == "update" and isinstance(call["body"], dict) and "performUpsert" in call["body"]:
                    kind = "upsert"
                call["kind"] = kind
                token = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                if token not in api.tokens:
                    self.reply(401, error("AUTHENTICATION_REQUIRED", "Authentication required"))
                    return
                if self.scripted(kind):
                    return
                tables = api.bases.get(base)
                if tables is None:
                    self.reply(403, error("INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
                                          "Invalid permissions, or the requested model was not found. Check that both your "
                                          "user and your token have the required permissions, and that the model names "
                                          "and/or ids are correct."))
                    return
                found = next((item for item in tables.values() if table in (item.title, item.id)), None)
                if found is None:
                    self.reply(404, error("TABLE_NOT_FOUND", f"Could not find table {table} in application {base}"))
                    return
                with lock:
                    getattr(self, "op_" + kind)(found, call, tail)

            # -- reads -----------------------------------------------------------

            def unknown_field(self, table, names):
                for item in names:
                    if item not in table.schema and item not in table.field_ids.values():
                        self.reply(422, error("UNKNOWN_FIELD_NAME", f'Unknown field name: "{item}"'))
                        return True
                return False

            def field_key(self, table, item):
                return next((key for key, ident in table.field_ids.items() if ident == item), item)

            def listing(self, table, params):
                size = params.get("pageSize", 100)
                if type(size) is not int or not 1 <= size <= 100:
                    self.reply(422, error("INVALID_REQUEST_UNKNOWN", "Invalid request: parameter validation failed. "
                                                                     "Check your request data."))
                    return
                fields = params.get("fields") or []
                sort = params.get("sort") or []
                if self.unknown_field(table, fields) or self.unknown_field(table, [item["field"] for item in sort]):
                    return
                offset = params.get("offset")
                if offset:
                    matched = ITERATOR.fullmatch(offset)
                    snapshot = api.iterators.get(matched.group(1)) if matched else None
                    if snapshot is None or matched.group(2) not in snapshot:
                        self.reply(422, error("LIST_RECORDS_ITERATOR_NOT_AVAILABLE", "The offset is no longer valid."))
                        return
                    iterator, start = matched.group(1), snapshot.index(matched.group(2))
                else:
                    ids = list(table.records)
                    view = params.get("view")
                    if view and view not in ("Grid view", "Active", ACTIVE_VIEW):
                        self.reply(422, error("VIEW_NAME_NOT_FOUND", f'Could not find view "{view}" in table'))
                        return
                    if view in ("Active", ACTIVE_VIEW):
                        ids = [ident for ident in ids if table.records[ident]["fields"].get("Status") == "active"]
                    formula = params.get("filterByFormula")
                    if formula:
                        text = formula.strip()
                        equal = EQUALS.fullmatch(text)
                        if text == "TRUE()":
                            pass
                        elif equal and self.field_key(table, equal.group(1)) in table.schema:
                            key = self.field_key(table, equal.group(1))
                            wanted = (equal.group(2) if equal.group(2) is not None else equal.group(3))
                            wanted = re.sub(r"\\(.)", r"\1", wanted)
                            ids = [ident for ident in ids if text_of(table.records[ident]["fields"].get(key)) == wanted]
                        else:
                            self.reply(422, error("INVALID_FILTER_BY_FORMULA",
                                                  "The formula for filtering records is invalid: Invalid formula. "
                                                  "Please check your formula text."))
                            return
                    for item in reversed(sort):
                        key = self.field_key(table, item["field"])
                        ids.sort(key=lambda ident: (str(type(table.records[ident]["fields"].get(key, ""))),
                                                    table.records[ident]["fields"].get(key, "")),
                                 reverse=item.get("direction") == "desc")
                    iterator = "itr" + f"Fixture{len(api.iterators) + 1:07d}"
                    api.iterators[iterator] = ids
                    start = 0
                snapshot = api.iterators[iterator]
                chosen = [ident for ident in snapshot[start:start + size] if ident in table.records]
                only = {self.field_key(table, item) for item in fields} or None
                by_id = params.get("returnFieldsByFieldId") is True
                body = {"records": [table.public(ident, by_id, only) for ident in chosen]}
                if start + size < len(snapshot):
                    body["offset"] = iterator + "/" + snapshot[start + size]
                self.reply(200, body)

            def op_list(self, table, call, _tail):
                query = call["query"]
                params = {}
                for key, values in query.items():
                    if key == "fields[]":
                        params["fields"] = values
                    elif key.startswith("sort["):
                        continue
                    elif key == "pageSize":
                        params["pageSize"] = int(values[0]) if values[0].isdigit() else -1
                    elif key == "returnFieldsByFieldId":
                        params["returnFieldsByFieldId"] = values[0] == "true"
                    else:
                        params[key] = values[0]
                sort, index = [], 0
                while f"sort[{index}][field]" in query:
                    sort.append({"field": query[f"sort[{index}][field]"][0],
                                 "direction": query.get(f"sort[{index}][direction]", ["asc"])[0]})
                    index += 1
                if sort:
                    params["sort"] = sort
                self.listing(table, params)

            def op_listpost(self, table, call, _tail):
                self.listing(table, call["body"] or {})

            def op_get(self, table, call, tail):
                if tail not in table.records:
                    self.reply(404, {"error": "NOT_FOUND"})
                    return
                by_id = call["query"].get("returnFieldsByFieldId", [""])[0] == "true"
                self.reply(200, table.public(tail, by_id))

            # -- writes ----------------------------------------------------------

            def converted(self, table, fields, typecast):
                """Return Airtable-checked fields, or reply 422 and return None."""
                result = {}
                for key, value in fields.items():
                    name = self.field_key(table, key)
                    kind = table.schema.get(name)
                    if kind is None:
                        self.reply(422, error("UNKNOWN_FIELD_NAME", f'Unknown field name: "{key}"'))
                        return None
                    if kind == "number" and value is not None and type(value) not in (int, float):
                        if typecast and isinstance(value, str) and re.fullmatch(r"-?[0-9]+(\.[0-9]+)?", value):
                            value = float(value) if "." in value else int(value)
                        else:
                            self.reply(422, error("INVALID_VALUE_FOR_COLUMN",
                                                  f'Field "{name}" cannot accept the provided value'))
                            return None
                    if kind == "select" and value is not None and value not in table.options[name]:
                        if typecast and isinstance(value, str):
                            table.options[name].add(value)
                        else:
                            self.reply(422, error("INVALID_MULTIPLE_CHOICE_OPTIONS",
                                                  f'Insufficient permissions to create new select option ""{value}""'))
                            return None
                    if kind == "checkbox" and value is not None and type(value) is not bool:
                        self.reply(422, error("INVALID_VALUE_FOR_COLUMN", f'Field "{name}" cannot accept the provided value'))
                        return None
                    result[name] = value
                return result

            def batch(self, body):
                records = body.get("records") if isinstance(body, dict) else None
                if not isinstance(records, list) or not records:
                    self.reply(422, error("INVALID_REQUEST_MISSING_FIELDS", 'Could not find field "records" in the request body'))
                    return None
                if len(records) > 10:
                    self.reply(422, error("INVALID_RECORDS", "You may only create, update or delete 10 records per request"))
                    return None
                return records

            def op_create(self, table, call, _tail):
                body = call["body"]
                records = self.batch(body)
                if records is None:
                    return
                prepared = []
                for item in records:
                    fields = self.converted(table, item.get("fields", {}), body.get("typecast") is True)
                    if fields is None:
                        return
                    prepared.append(fields)
                ids = [table.add(fields) for fields in prepared]
                by_id = body.get("returnFieldsByFieldId") is True
                self.reply(200, {"records": [table.public(ident, by_id) for ident in ids]})

            def op_update(self, table, call, _tail):
                body = call["body"]
                records = self.batch(body)
                if records is None:
                    return
                prepared = []
                for item in records:
                    if item.get("id") not in table.records:
                        self.reply(404, error("ROW_DOES_NOT_EXIST", f"Record ID {item.get('id')} does not exist in this table"))
                        return
                    fields = self.converted(table, item.get("fields", {}), body.get("typecast") is True)
                    if fields is None:
                        return
                    prepared.append((item["id"], fields))
                for ident, fields in prepared:
                    table.records[ident]["fields"].update(fields)
                by_id = body.get("returnFieldsByFieldId") is True
                self.reply(200, {"records": [table.public(ident, by_id) for ident, _ in prepared]})

            def op_upsert(self, table, call, _tail):
                body = call["body"]
                records = self.batch(body)
                if records is None:
                    return
                merge = body["performUpsert"].get("fieldsToMergeOn")
                if not isinstance(merge, list) or not 1 <= len(merge) <= 3:
                    self.reply(422, error("INVALID_REQUEST_UNKNOWN", "fieldsToMergeOn must list 1 to 3 fields"))
                    return
                if self.unknown_field(table, merge):
                    return
                keys = [self.field_key(table, item) for item in merge]
                plan = []
                for item in records:
                    fields = self.converted(table, item.get("fields", {}), body.get("typecast") is True)
                    if fields is None:
                        return
                    if any(key not in fields for key in keys):
                        self.reply(422, error("INVALID_REQUEST_UNKNOWN", "Each record must include every merge field"))
                        return
                    hits = [ident for ident, row in table.records.items()
                            if all(text_of(row["fields"].get(key)) == text_of(fields[key]) for key in keys)]
                    if len(hits) > 1:
                        self.reply(422, error("INVALID_VALUE_FOR_COLUMN", "Upsert matched more than one record"))
                        return
                    plan.append((hits[0] if hits else None, fields))
                created, updated, ids = [], [], []
                for ident, fields in plan:
                    if ident is None:
                        ident = table.add(fields)
                        created.append(ident)
                    else:
                        table.records[ident]["fields"].update(fields)
                        updated.append(ident)
                    ids.append(ident)
                by_id = body.get("returnFieldsByFieldId") is True
                self.reply(200, {"records": [table.public(ident, by_id) for ident in ids], "createdRecords": created,
                                 "updatedRecords": updated})

            def op_delete(self, table, call, _tail):
                ids = call["query"].get("records[]", [])
                if not ids or len(ids) > 10:
                    self.reply(422, error("INVALID_RECORDS", "You may only create, update or delete 10 records per request"))
                    return
                if any(ident not in table.records for ident in ids):
                    self.reply(404, {"error": "NOT_FOUND"})
                    return
                for ident in ids:
                    del table.records[ident]
                self.reply(200, {"records": [{"id": ident, "deleted": True} for ident in ids]})

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        ca = ""
        if tls:
            assert shutil.which("openssl"), "OpenSSL is required for the disposable HTTPS fixture"
            certificate, key = directory / "ca.pem", directory / "key.pem"
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                            "-keyout", str(key), "-out", str(certificate), "-subj", "/CN=localhost",
                            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], check=True, timeout=30,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certificate, key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            ca = str(certificate)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        api.base_url = ("https" if tls else "http") + "://127.0.0.1:" + str(server.server_port)
        api.ca_file = ca
        api.table = lambda title=TABLE: api.bases[BASE][title]
        api.kinds = lambda: [call["kind"] for call in api.calls]
        try:
            yield api
        finally:
            api.closing.set()
            api.release.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive()
