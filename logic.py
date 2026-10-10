"""Pure Airtable Web API v0 contracts: settings, requests, record batches, responses and errors.

No network, filesystem, framework or third-party import happens here. Every
function is deterministic, so the policy can be checked directly by the tests.
"""

from collections.abc import Mapping
import ipaddress
import json
import math
import re
from urllib.parse import quote, urlencode, urlsplit

OFFICIAL_API = "https://api.airtable.com"
OPERATIONS = ("list_records", "get_record", "create_records", "update_records", "upsert_records", "delete_records")
WRITES = frozenset(("create_records", "update_records", "upsert_records", "delete_records"))
BATCH_SIZE = 10  # Airtable accepts at most 10 records per create, update, upsert or delete request.
URL_LIMIT = 16000  # Airtable refuses URLs of 16,000 characters or more; longer list requests use POST listRecords.
RATE_LIMIT_WAIT = 30  # Airtable documents a 30-second wait after HTTP 429 when no Retry-After is given.
OUTPUT_LIMIT = 16777216
DEFAULTS = {
    "enabled": False, "secret_ref": "", "base_id": "", "table": "", "view": "",
    "operation": "list_records", "allowed_operations": ["list_records"], "allow_writes": False, "allow_delete": False,
    "typecast": False, "fields_to_merge_on": [], "max_write_records": 100, "read_limit": 1000, "page_size": 100,
    "max_pages": 20, "return_fields_by_field_id": False, "timeout_sec": 60, "max_requests_per_sec": 5,
    "max_retries": 2, "max_retry_wait_sec": 35, "max_request_bytes": 1048576, "max_response_bytes": 4194304,
    "api_base_url": OFFICIAL_API, "ca_file": "",
}
FRAMEWORK_KEYS = {"execution", "runtime_path", "runtime_path_label"}
BOUNDS = {
    "max_write_records": (1, 1000), "read_limit": (1, 10000), "page_size": (1, 100), "max_pages": (1, 100),
    "timeout_sec": (5, 300), "max_requests_per_sec": (1, 5), "max_retries": (0, 5), "max_retry_wait_sec": (1, 120),
    "max_request_bytes": (1024, 8388608), "max_response_bytes": (65536, 33554432),
}
BOOLEANS = ("enabled", "allow_writes", "allow_delete", "typecast", "return_fields_by_field_id")
SECRET_REF = re.compile(r"secret://(?:workspace/[A-Za-z0-9_.-]{1,80}|project/[A-Za-z0-9_.-]{1,80}/[A-Za-z0-9_.-]{1,80})\Z")
BASE_ID = re.compile(r"app[A-Za-z0-9]{14}\Z")
RECORD_ID = re.compile(r"rec[A-Za-z0-9]{14}\Z")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
TOKEN_TEXT = re.compile(r"[\x21-\x7e]{1,8192}\Z")
OFFSET_TEXT = re.compile(r"[\x21-\x7e]{1,512}\Z")
HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
NAME_LIMIT = 255
FORMULA_LIMIT = 16000
MAX_FIELDS = 500
MAX_SORT = 20
MAX_MERGE_FIELDS = 3
REQUEST_KEYS = {
    "list_records": {"operation", "request_id", "table", "view", "filter_by_formula", "fields", "sort", "max_records",
                     "page_size", "offset"},
    "get_record": {"operation", "request_id", "table", "record_id"},
    "create_records": {"operation", "request_id", "table", "records", "record", "typecast"},
    "update_records": {"operation", "request_id", "table", "records", "record", "typecast"},
    "upsert_records": {"operation", "request_id", "table", "records", "record", "typecast", "fields_to_merge_on"},
    "delete_records": {"operation", "request_id", "table", "record_ids", "record_id"},
}

MESSAGES = {
    "config": "Invalid Airtable settings.",
    "disabled": "Remote access is disabled; enable the block explicitly before contacting Airtable.",
    "secret": "Unlock the wallet and store an Airtable personal access token, or JSON with token, under secret_ref.",
    "invalid_input": "The request does not follow the documented contract; nothing was sent.",
    "operation_not_allowed": "This operation is not listed in allowed_operations; nothing was sent.",
    "writes_disabled": "Writes are disabled; enable allow_writes in the properties; nothing was written.",
    "delete_disabled": "delete_records needs allow_delete in the properties; nothing was deleted.",
    "too_many_records": "The request has more records than max_write_records; nothing was written.",
    "request_too_large": "The request exceeds max_request_bytes; nothing was sent.",
    "provider_too_large": "Airtable refused the request body as too large (HTTP 413); nothing was changed.",
    "output_too_large": "One page of records exceeds the 16 MiB output limit; lower page_size or select fewer fields.",
    "bad_request": "Airtable rejected the request (HTTP 400); nothing was changed.",
    "unauthorized": "Airtable rejected the token (HTTP 401); check or replace the wallet secret.",
    "quota_exceeded": "The Airtable workspace reached a plan limit (HTTP 402); nothing was changed.",
    "forbidden": "The token has no access to this base or table, or the model does not exist (HTTP 403).",
    "not_found": "The table or record was not found (HTTP 404); check base_id, table and record IDs.",
    "invalid_request": "Airtable could not process the request (HTTP 422), for example an unknown field or formula error.",
    "rate_limited": "Airtable rate limit reached (HTTP 429); retry after the indicated delay.",
    "provider_error": "Airtable returned an unexpected error status; nothing is assumed changed.",
    "provider_unavailable": "Airtable is temporarily unavailable (HTTP 5xx); nothing was written.",
    "provider_unavailable_uncertain": "Airtable answered HTTP 5xx after the write was sent; the change is uncertain.",
    "network": "The connection to Airtable failed before the request was complete; nothing was changed.",
    "network_uncertain": "The connection failed after the write was sent; the change is uncertain.",
    "tls": "TLS verification failed for the Airtable API origin; nothing was sent.",
    "timeout": "The operation exceeded timeout_sec before any write was complete; nothing was changed.",
    "timeout_uncertain": "The operation exceeded timeout_sec after the write was sent; the change is uncertain.",
    "cancelled": "The operation was cancelled after the write was sent; the change is uncertain.",
    "cancelled_before_send": "The operation was cancelled before the next request was sent.",
    "protocol": "Airtable returned an invalid, compressed or oversized response.",
    "protocol_uncertain": "Airtable accepted the write but returned an unexpected receipt; the change is likely.",
    "partial": "Earlier batches were already written; committed_record_ids lists them.",
    "internal": "The Airtable operation failed without a complete result.",
}


class AirtableError(ValueError):
    """A content-free diagnostic with a stable machine code and an English message."""

    def __init__(self, code, detail=None, *, uncertain=False, http_status=None, retry_after=None,
                 provider_type=None, provider_message=None, retryable=False):
        super().__init__(detail or MESSAGES.get(code, MESSAGES["internal"]))
        self.code, self.uncertain, self.retryable = code, bool(uncertain), bool(retryable)
        self.http_status, self.retry_after = http_status, retry_after
        self.provider_type, self.provider_message = provider_type, provider_message
        self.attempts = 0
        self.progress = None

    def envelope(self, **context):
        """Return the public error document; callers add identifiers only, never field data or tokens."""
        result = {"code": self.code, "message": str(self), "uncertain": self.uncertain}
        for key, value in (("http_status", self.http_status), ("retry_after", self.retry_after),
                           ("provider_type", self.provider_type), ("provider_message", self.provider_message)):
            if value is not None:
                result[key] = value
        if self.attempts:
            result["attempts"] = self.attempts
        if self.progress:
            result.update(self.progress)
        for key, value in context.items():
            if value is not None:
                result[key] = value
        return result


def fail(code, detail=None, **extra):
    """Raise a structured failure."""
    raise AirtableError(code, detail, **extra)


def thaw(value):
    """Turn immutable preparation snapshots back into plain JSON containers."""
    if isinstance(value, Mapping):
        return {key: thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw(item) for item in value]
    return value


def encoded(value, limit, code="invalid_input"):
    """Serialize strict finite JSON with bounded depth, element count and bytes."""
    pending, count = [(value, 0)], 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 24 or count > 2000000:
            fail(code, "JSON exceeds its depth or element limit.")
        if isinstance(item, dict):
            if any(type(key) is not str for key in item):
                fail(code, "JSON object keys must be strings.")
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) not in (str, int, float, bool):
            fail(code, "A strict JSON value is required.")
        elif type(item) is float and not math.isfinite(item):
            fail(code, "Non-finite numbers are not accepted.")
    try:
        data = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        fail(code, "Finite UTF-8 JSON is required.")
    if len(data) > limit:
        fail("request_too_large" if code == "invalid_input" else code, "JSON exceeds its byte limit.")
    return data


def strict_json(text, limit, code="invalid_input"):
    """Parse JSON text, rejecting duplicate keys, non-finite numbers and oversized input."""
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8", errors="strict")
        except UnicodeError:
            fail(code, "UTF-8 JSON is required.")
    if not isinstance(text, str):
        fail(code, "JSON text is required.")
    if len(text.encode("utf-8", errors="replace")) > limit:
        fail("request_too_large" if code == "invalid_input" else code, "JSON exceeds its byte limit.")

    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                fail(code, "Duplicate JSON property.")
            result[key] = item
        return result

    def constant(_name):
        fail(code, "Non-finite numbers are not accepted.")

    try:
        value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    except AirtableError:
        raise
    except (ValueError, RecursionError):
        fail(code, "Malformed JSON.")
    encoded(value, limit, code)
    return value


def boolean(value, name, code="config"):
    if type(value) is not bool:
        fail(code, f"{name} must be a boolean.")
    return value


def integer(value, name, low, high, code="config"):
    if type(value) is not int or not low <= value <= high:
        fail(code, f"{name} must be an integer between {low} and {high}.")
    return value


def path_text(value, name):
    if not isinstance(value, str) or len(value) > 4096 or any(ord(char) < 32 or ord(char) == 127 for char in value):
        fail("config", f"{name} must be a bounded path without control characters.")
    value = value.strip()
    if value and (not value.startswith("/") or ".." in value.split("/")):
        fail("config", f"{name} must be empty or an absolute path without '..' components.")
    return value


def is_loopback(host):
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def api_origin(value, name):
    """Accept an HTTPS origin; plain HTTP only for a loopback host such as a local test double."""
    if not isinstance(value, str) or not 1 <= len(value) <= 2048 or any(ord(char) <= 32 or ord(char) >= 127 for char in value):
        fail("config", f"{name} must be an ASCII URL without spaces.")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        fail("config", f"{name} has an invalid host or port.")
    host = (parsed.hostname or "").lower()
    if (parsed.scheme not in ("https", "http") or not host or parsed.username is not None
            or parsed.password is not None or parsed.query or parsed.fragment or "?" in value or "#" in value
            or parsed.path not in ("", "/") or "\\" in value or "%" in host):
        fail("config", f"{name} must be a bare https:// origin without credentials, path, query or fragment.")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        if len(host) > 253 or host.endswith(".") or any(not HOST_LABEL.fullmatch(label) for label in host.split(".")):
            fail("config", f"{name} has an invalid host name.")
    if port is not None and not 1 <= port <= 65535:
        fail("config", f"{name} has an invalid port.")
    if parsed.scheme == "http" and not is_loopback(host):
        fail("config", f"Plain HTTP is accepted for {name} only with a loopback test host; use HTTPS.")
    netloc = "[" + host + "]" if ":" in host else host
    return parsed.scheme + "://" + netloc + (":" + str(port) if port is not None else "")


def visible(text):
    """No control characters (C0, DEL, C1) in names shown or sent as identifiers."""
    return not any(ord(char) < 32 or 127 <= ord(char) < 160 for char in text)


def name(value, label, code, *, empty=False):
    """A table, view or field name or ID: trimmed, 1-255 visible characters."""
    if not isinstance(value, str):
        fail(code, f"{label} must be text.")
    text = value.strip()
    if not text and empty:
        return ""
    if not text or len(text) > NAME_LIMIT or not visible(text):
        fail(code, f"{label} must be a name or ID of 1-{NAME_LIMIT} characters without control characters.")
    return text


def field_name(value, label, code="invalid_input"):
    """A field name or ID used as a key: kept exactly, 1-255 visible characters, not blank."""
    if not isinstance(value, str) or not value.strip() or len(value) > NAME_LIMIT or not visible(value):
        fail(code, f"{label} must be a field name or ID of 1-{NAME_LIMIT} characters without control characters.")
    return value


def field_names(value, label, code, low, high):
    if not isinstance(value, list) or not low <= len(value) <= high:
        fail(code, f"{label} must be a JSON array of {low}-{high} field names.")
    names = [field_name(item, label, code) for item in value]
    if len(set(names)) != len(names):
        fail(code, f"{label} must not repeat a field.")
    return names


def operations_list(value):
    if (not isinstance(value, list) or not 1 <= len(value) <= len(OPERATIONS)
            or any(item not in OPERATIONS for item in value) or len(set(value)) != len(value)):
        fail("config", "allowed_operations must list distinct values among " + ", ".join(OPERATIONS) + ".")
    return value


def configuration(raw):
    """Pure settings validation; no wallet, filesystem or network access."""
    raw = thaw(raw)
    if not isinstance(raw, Mapping):
        fail("config", "Settings must be a JSON object.")
    unknown = set(raw) - set(DEFAULTS) - FRAMEWORK_KEYS
    if unknown:
        fail("config", "Unknown setting: " + sorted(unknown)[0] + ". Credentials belong in the wallet.")
    config = {**{key: thaw(value) for key, value in DEFAULTS.items()},
              **{key: value for key, value in raw.items() if key in DEFAULTS}}
    for key in BOOLEANS:
        boolean(config[key], key)
    for key, (low, high) in BOUNDS.items():
        integer(config[key], key, low, high)
    ref = config["secret_ref"]
    if not isinstance(ref, str) or len(ref) > 256:
        fail("config", "secret_ref must be a wallet reference such as secret://workspace/airtable.")
    ref = ref.strip()
    if ref and not SECRET_REF.fullmatch(ref):
        fail("config", "secret_ref must be a wallet reference such as secret://workspace/airtable, never a token.")
    config["secret_ref"] = ref
    base = config["base_id"]
    if not isinstance(base, str):
        fail("config", "base_id must be text.")
    base = base.strip()
    if base and not BASE_ID.fullmatch(base):
        fail("config", "base_id must be the base ID from the Airtable URL: app followed by 14 letters or digits.")
    config["base_id"] = base
    config["table"] = name(config["table"], "table", "config", empty=True)
    config["view"] = name(config["view"], "view", "config", empty=True)
    if config["operation"] not in OPERATIONS:
        fail("config", "operation must be one of: " + ", ".join(OPERATIONS) + ".")
    config["allowed_operations"] = operations_list(config["allowed_operations"])
    selectable = set(config["allowed_operations"]) | {config["operation"]}
    if selectable & WRITES and not config["allow_writes"]:
        fail("config", "create_records, update_records, upsert_records and delete_records need allow_writes; "
                       "enable it or remove them.")
    if "delete_records" in selectable and not config["allow_delete"]:
        fail("config", "delete_records needs allow_delete; enable it or remove delete_records.")
    if config["fields_to_merge_on"] != []:
        config["fields_to_merge_on"] = field_names(config["fields_to_merge_on"], "fields_to_merge_on", "config",
                                                   1, MAX_MERGE_FIELDS)
    config["ca_file"] = path_text(config["ca_file"], "ca_file")
    config["api_base_url"] = api_origin(config["api_base_url"], "api_base_url")
    if config["enabled"]:
        if not config["secret_ref"]:
            fail("config", "Remote access requires secret_ref, a wallet reference holding the Airtable token.")
        if not config["base_id"]:
            fail("config", "Remote access requires base_id.")
        if not config["table"]:
            fail("config", "Remote access requires table, a table name or ID.")
    return config


def selectable_operations(config):
    return [item for item in OPERATIONS if item == config["operation"] or item in config["allowed_operations"]]


def credentials(value):
    """Validate the wallet value: a bare token, {"token": ...} or {"access_token": ...}; return the token."""
    secret = value
    if isinstance(secret, str):
        text = secret.strip()
        if text.startswith("{"):
            try:
                secret = strict_json(text, 32768, "secret")
            except AirtableError:
                fail("secret")
        else:
            secret = {"token": text}
    if not isinstance(secret, Mapping) or set(secret) not in ({"token"}, {"access_token"}):
        fail("secret")
    token = next(iter(secret.values()))
    if not isinstance(token, str) or not TOKEN_TEXT.fullmatch(token):
        fail("secret")
    return token


def redact(text, secrets):
    """Mask every credential value wherever it could appear."""
    text = "" if text is None else str(text)
    for secret in secrets:
        if secret and len(secret) >= 4:
            text = text.replace(secret, "***")
    return text


def clean(value, limit):
    """Bounded single-line provider text with control characters removed."""
    if not isinstance(value, str):
        return None
    value = "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in value).strip()
    return value[:limit] if value else None


# ---------------------------------------------------------------------------
# Requests


def identifier(value):
    if value is None:
        return None
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        fail("invalid_input", "request_id needs 1-128 ASCII letters, digits or ._:- characters.")
    return value


def record_id(value, label="record_id"):
    if not isinstance(value, str) or not RECORD_ID.fullmatch(value):
        fail("invalid_input", f"{label} must be an Airtable record ID: rec followed by 14 letters or digits.")
    return value


def document(value, config):
    """Read a JSON object or array; a bare record ID or other text is a trigger for the configured operation."""
    if isinstance(value, Mapping):
        result = thaw(value)
        encoded(result, config["max_request_bytes"])
        return result
    if isinstance(value, (list, tuple)):
        result = thaw(value)
        encoded(result, config["max_request_bytes"])
        return {"records": result}
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="strict")
        except UnicodeError:
            fail("invalid_input", "The request must be UTF-8 text or JSON.")
    if not isinstance(value, str):
        fail("invalid_input", "The request must be UTF-8 text or JSON.")
    text = value.strip()
    if text.startswith("{") or text.startswith("["):
        parsed = strict_json(text, config["max_request_bytes"])
        if isinstance(parsed, list):
            return {"records": parsed}
        if not isinstance(parsed, dict):
            fail("invalid_input", "A value starting with { must be one JSON object.")
        return parsed
    if len(value.encode("utf-8", errors="replace")) > config["max_request_bytes"]:
        fail("request_too_large")
    if RECORD_ID.fullmatch(text) and config["operation"] in ("get_record", "delete_records"):
        return {"record_id": text}
    return {}


def formula(value):
    """filterByFormula text: bounded, line breaks and tabs allowed, other control characters refused."""
    if not isinstance(value, str) or len(value) > FORMULA_LIMIT:
        fail("invalid_input", f"filter_by_formula must be text of at most {FORMULA_LIMIT} characters.")
    if any((ord(char) < 32 and char not in "\t\r\n") or ord(char) == 127 for char in value):
        fail("invalid_input", "filter_by_formula cannot contain control characters.")
    return value.strip()


def sort_spec(value):
    if not isinstance(value, list) or len(value) > MAX_SORT:
        fail("invalid_input", f"sort must be a JSON array of at most {MAX_SORT} {{field, direction}} objects.")
    result = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or "field" not in item or not set(item) <= {"field", "direction"}:
            fail("invalid_input", f"sort[{index}] must be an object with field and an optional direction.")
        direction = item.get("direction", "asc")
        if direction not in ("asc", "desc"):
            fail("invalid_input", f"sort[{index}].direction must be asc or desc.")
        result.append({"field": field_name(item["field"], f"sort[{index}].field"), "direction": direction})
    return result


def fields_object(value, label):
    """A record's fields: an object keyed by field names or IDs; values are any bounded JSON."""
    if not isinstance(value, dict):
        fail("invalid_input", f"{label} must be a JSON object keyed by field names.")
    for key in value:
        field_name(key, f"{label} key")
    return value


def write_records(data, operation, config):
    """Validate the records of a create, update or upsert request."""
    records, single = data.get("records"), data.get("record")
    if records is not None and single is not None:
        fail("invalid_input", "Give either records or record, not both.")
    if single is not None:
        records = [single]
    if not isinstance(records, list) or not records:
        fail("invalid_input", "records must be a non-empty JSON array of {\"fields\": {...}} objects.")
    if len(records) > config["max_write_records"]:
        fail("too_many_records")
    allowed = {"id", "fields"} if operation == "update_records" else {"fields"}
    result, seen = [], set()
    for index, item in enumerate(records):
        label = f"records[{index}]"
        if not isinstance(item, dict) or "fields" not in item or not set(item) <= allowed:
            shape = '{"id": "rec...", "fields": {...}}' if operation == "update_records" else '{"fields": {...}}'
            fail("invalid_input", f"{label} must be an object shaped {shape}.")
        fields = fields_object(item["fields"], label + ".fields")
        if operation == "update_records":
            ident = record_id(item.get("id"), label + ".id")
            if ident in seen:
                fail("invalid_input", f"{label}.id repeats a record of this request; nothing was written.")
            seen.add(ident)
            if not fields:
                fail("invalid_input", f"{label}.fields must name at least one field to update.")
            result.append({"id": ident, "fields": fields})
        else:
            result.append({"fields": fields})
    return result


def delete_ids(data, config):
    ids, single = data.get("record_ids"), data.get("record_id")
    if ids is not None and single is not None:
        fail("invalid_input", "Give either record_ids or record_id, not both.")
    if single is not None:
        ids = [single]
    if not isinstance(ids, list) or not ids:
        fail("invalid_input", "record_ids must be a non-empty JSON array of record IDs.")
    if len(ids) > config["max_write_records"]:
        fail("too_many_records")
    result = [record_id(item, f"record_ids[{index}]") for index, item in enumerate(ids)]
    if len(set(result)) != len(result):
        fail("invalid_input", "record_ids must not repeat a record; nothing was deleted.")
    return result


def parse_request(value, config):
    """Return one normalized operation request; enforce the operation allowlist before any wallet access."""
    data = document(value, config)
    operation = data.get("operation", config["operation"])
    if operation not in OPERATIONS:
        fail("invalid_input", "operation must be one of: " + ", ".join(OPERATIONS) + ".")
    request = {"operation": operation, "request_id": identifier(data.get("request_id")), "table": config["table"]}
    if operation not in selectable_operations(config):
        fail("operation_not_allowed")
    if operation in WRITES and not config["allow_writes"]:
        fail("writes_disabled")
    if operation == "delete_records" and not config["allow_delete"]:
        fail("delete_disabled")
    unknown = set(data) - REQUEST_KEYS[operation]
    if unknown:
        fail("invalid_input", "Unknown field for " + operation + ": " + sorted(unknown)[0][:64] + ".")
    if "table" in data:
        request["table"] = name(data["table"], "table", "invalid_input")
    if not request["table"]:
        fail("invalid_input", "No table: set table in the properties or in the request.")
    if operation == "list_records":
        view = data.get("view", config["view"])
        request["view"] = name(view, "view", "invalid_input", empty=True)
        request["filter_by_formula"] = formula(data.get("filter_by_formula", ""))
        request["fields"] = field_names(data["fields"], "fields", "invalid_input", 1, MAX_FIELDS) if "fields" in data else []
        request["sort"] = sort_spec(data.get("sort", []))
        request["max_records"] = integer(data.get("max_records", config["read_limit"]), "max_records", 1,
                                         config["read_limit"], "invalid_input")
        request["page_size"] = integer(data.get("page_size", config["page_size"]), "page_size", 1, 100, "invalid_input")
        offset = data.get("offset")
        if offset is not None and (not isinstance(offset, str) or not OFFSET_TEXT.fullmatch(offset)):
            fail("invalid_input", "offset must be the offset text returned by a previous list_records result.")
        request["offset"] = offset
        return request
    if operation == "get_record":
        request["record_id"] = record_id(data.get("record_id"))
        return request
    if operation == "delete_records":
        request["record_ids"] = delete_ids(data, config)
        return request
    typecast = data.get("typecast", config["typecast"])
    request["typecast"] = boolean(typecast, "typecast", "invalid_input")
    request["records"] = write_records(data, operation, config)
    if operation == "upsert_records":
        merge = data.get("fields_to_merge_on", config["fields_to_merge_on"])
        if merge in (None, []):
            fail("invalid_input", "upsert_records needs fields_to_merge_on: 1-3 field names, in the request or the properties.")
        merge = field_names(merge, "fields_to_merge_on", "invalid_input", 1, MAX_MERGE_FIELDS)
        for index, item in enumerate(request["records"]):
            if any(item["fields"].get(key) is None for key in merge):
                fail("invalid_input", f"records[{index}].fields needs a value for every fields_to_merge_on field.")
        request["fields_to_merge_on"] = merge
    return request


# ---------------------------------------------------------------------------
# Paths, query strings and batches


def table_path(base_id, table, *suffix):
    """/v0/<base>/<table>[/<suffix>]: the table name or ID is always percent-encoded."""
    parts = ["", "v0", base_id, quote(table, safe="")] + [quote(item, safe="") for item in suffix]
    return "/".join(parts)


def list_parameters(request, config, size, offset):
    """List parameters as Airtable names them, shared by GET query strings and POST listRecords bodies."""
    params = {"pageSize": size}
    if offset:
        params["offset"] = offset
    if request["view"]:
        params["view"] = request["view"]
    if request["filter_by_formula"]:
        params["filterByFormula"] = request["filter_by_formula"]
    if request["fields"]:
        params["fields"] = list(request["fields"])
    if request["sort"]:
        params["sort"] = [dict(item) for item in request["sort"]]
    if config["return_fields_by_field_id"]:
        params["returnFieldsByFieldId"] = True
    return params


def list_query(params):
    """Encode list parameters for GET: fields[]=..., sort[0][field]=..., sort[0][direction]=..."""
    pairs = []
    for key, value in params.items():
        if key == "fields":
            pairs.extend(("fields[]", item) for item in value)
        elif key == "sort":
            for index, item in enumerate(value):
                pairs.append((f"sort[{index}][field]", item["field"]))
                pairs.append((f"sort[{index}][direction]", item["direction"]))
        elif value is True:
            pairs.append((key, "true"))
        else:
            pairs.append((key, str(value)))
    return urlencode(pairs)


def batches(items, size=BATCH_SIZE):
    return [items[index:index + size] for index in range(0, len(items), size)]


def write_bodies(request, config):
    """Build and size every batch body before the first write, so a late batch cannot fail locally."""
    operation = request["operation"]
    if operation == "delete_records":
        return [None for _chunk in batches(request["record_ids"])]
    bodies = []
    for chunk in batches(request["records"]):
        body = {"records": chunk, "typecast": request["typecast"]}
        if operation == "upsert_records":
            body = {"performUpsert": {"fieldsToMergeOn": list(request["fields_to_merge_on"])}, **body}
        if config["return_fields_by_field_id"]:
            body["returnFieldsByFieldId"] = True
        bodies.append(encoded(body, config["max_request_bytes"], "request_too_large"))
    return bodies


def delete_query(ids):
    return urlencode([("records[]", item) for item in ids])


# ---------------------------------------------------------------------------
# Provider responses


def provider_detail(body):
    """Airtable error bodies: {"error": {"type", "message"}}, {"error": "NOT_FOUND"} or {"errors": [{"error", "message"}]}."""
    if not isinstance(body, dict):
        return None, None
    error = body.get("error")
    if isinstance(error, dict):
        return error.get("type"), error.get("message")
    if isinstance(error, str):
        return error, body.get("message")
    errors = body.get("errors")
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        return errors[0].get("error"), errors[0].get("message")
    return None, None


def map_status(status):
    return {400: "bad_request", 401: "unauthorized", 402: "quota_exceeded", 403: "forbidden", 404: "not_found",
            413: "provider_too_large", 422: "invalid_request", 429: "rate_limited"}.get(
                status, "provider_unavailable" if status >= 500 else "provider_error")


def provider_failure(status, body, secrets, *, mutation, retry_after=None):
    """Raise the coded error for a non-2xx Airtable response."""
    kind, message = provider_detail(body)
    code = map_status(status)
    uncertain = mutation and code == "provider_unavailable"
    if code == "rate_limited" and retry_after is None:
        retry_after = RATE_LIMIT_WAIT
    fail(code, MESSAGES["provider_unavailable_uncertain"] if uncertain else None, uncertain=uncertain, http_status=status,
         retry_after=retry_after if code == "rate_limited" else None, provider_type=clean(kind, 64),
         provider_message=clean(redact(message, secrets), 200))


def created_text(value):
    return value[:64] if isinstance(value, str) else None


def record(item, *, mutation=False):
    """Normalize one Airtable record to {"id", "created_time", "fields"}; empty fields are absent, as Airtable sends them."""
    if (not isinstance(item, dict) or not isinstance(item.get("id"), str) or not RECORD_ID.fullmatch(item["id"])
            or not isinstance(item.get("fields", {}), dict)):
        fail("protocol", MESSAGES["protocol_uncertain"] if mutation else None, uncertain=mutation)
    result = {"id": item["id"], "created_time": created_text(item.get("createdTime")), "fields": item.get("fields", {})}
    if type(item.get("commentCount")) is int:
        result["comment_count"] = item["commentCount"]
    return result


def page(body, size):
    """One list page: at most `size` records and an optional continuation offset."""
    records = body.get("records") if isinstance(body, dict) else None
    offset = body.get("offset") if isinstance(body, dict) else None
    if not isinstance(records, list) or len(records) > size:
        fail("protocol")
    if offset is not None and (not isinstance(offset, str) or not OFFSET_TEXT.fullmatch(offset)):
        fail("protocol")
    return [record(item) for item in records], offset


def written(body, expected, *, ids=None):
    """Validate a create/update/upsert receipt: one returned record per record sent, IDs matching updates."""
    records = body.get("records") if isinstance(body, dict) else None
    if not isinstance(records, list) or len(records) != expected:
        fail("protocol", MESSAGES["protocol_uncertain"], uncertain=True)
    result = [record(item, mutation=True) for item in records]
    if ids is not None and sorted(item["id"] for item in result) != sorted(ids):
        fail("protocol", MESSAGES["protocol_uncertain"], uncertain=True)
    return result


def upserted(body, records):
    """createdRecords / updatedRecords of a performUpsert receipt, both lists of returned record IDs."""
    created, updated = body.get("createdRecords"), body.get("updatedRecords")
    returned = {item["id"] for item in records}
    for value in (created, updated):
        if (not isinstance(value, list) or any(not isinstance(item, str) or item not in returned for item in value)):
            fail("protocol", MESSAGES["protocol_uncertain"], uncertain=True)
    return list(created), list(updated)


def deleted(body, ids):
    records = body.get("records") if isinstance(body, dict) else None
    if (not isinstance(records, list) or len(records) != len(ids)
            or any(not isinstance(item, dict) or item.get("deleted") is not True for item in records)
            or sorted(str(item.get("id")) for item in records) != sorted(ids)):
        fail("protocol", MESSAGES["protocol_uncertain"], uncertain=True)
    return [item["id"] for item in records]


def size_of(item):
    return len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) + 1
