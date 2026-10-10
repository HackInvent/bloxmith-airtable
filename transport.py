"""Bounded, cancellable and paced HTTPS exchanges with the Airtable Web API.

Standard library only. Each exchange runs in a helper thread so cancellation,
Stop and the activation deadline can shut the socket down at once. Requests to
one base are paced to at most max_requests_per_sec in any one-second window,
across every node of this process. Reads are retried after HTTP 429, 5xx or a
lost connection; writes only after HTTP 429, which Airtable did not process.
Ambiguous writes are reported, never replayed.
"""

from collections import deque
import http.client
import json
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit

from . import logic

CHUNK = 65536
USER_AGENT = "BloxSmith-Airtable/0.1.0"
WINDOW = 1.0
_PACE_LOCK = threading.Lock()
_PACE = {}


class Failure(Exception):
    """A content-free transport outcome: code and the phase it happened in."""

    def __init__(self, code, phase):
        super().__init__(code)
        self.code, self.phase = code, phase


def reject_constant(_value):
    raise ValueError("Non-finite JSON number.")


def remaining(deadline, phase):
    left = deadline - time.monotonic()
    if left <= 0:
        raise Failure("timeout", phase)
    return max(0.001, left)


def pace_slot(key, rate, now=None):
    """Take a request slot for this base now (0.0), or return how long to wait for one.

    The window is shared by every session of the process, so several nodes on the
    same base respect Airtable's per-base limit together.
    """
    with _PACE_LOCK:
        now = time.monotonic() if now is None else now
        window = _PACE.get(key)
        if window is None:
            if len(_PACE) >= 256:
                for stale in [item for item, times in _PACE.items() if not times or now - times[-1] >= WINDOW]:
                    del _PACE[stale]
            window = _PACE[key] = deque()
        while window and now - window[0] >= WINDOW:
            window.popleft()
        if len(window) < rate:
            window.append(now)
            return 0.0
        return max(0.001, WINDOW - (now - window[-rate]))


def perform(state, origin, ca_file, method, path, headers, body, deadline, limit):
    """One request/response without redirects, cookies, proxies or decompression."""
    parsed = urlsplit(origin)
    host, port = parsed.hostname, parsed.port
    try:
        if parsed.scheme == "https":
            context = ssl.create_default_context(cafile=ca_file or None)
            conn = http.client.HTTPSConnection(host, port or 443, timeout=remaining(deadline, "connect"), context=context)
        else:
            conn = http.client.HTTPConnection(host, port or 80, timeout=remaining(deadline, "connect"))
        state["conn"] = conn
        conn.connect()
        if state.get("aborted"):
            raise Failure("network", "connect")
        state["phase"] = "send"
        conn.putrequest(method, path, skip_host=False, skip_accept_encoding=True)
        for key, value in headers.items():
            conn.putheader(key, value)
        conn.putheader("Content-Length", str(len(body or b"")))
        conn.sock.settimeout(remaining(deadline, "send"))
        conn.endheaders(message_body=body or None)
        state["phase"] = "receive"
        conn.sock.settimeout(remaining(deadline, "receive"))
        response = conn.getresponse()
        chunks, total = [], 0
        while total <= limit:
            if conn.sock is not None:
                conn.sock.settimeout(remaining(deadline, "receive"))
            piece = response.read1(min(CHUNK, limit + 1 - total))
            if not piece:
                break
            total += len(piece)
            chunks.append(piece)
        found = {key.lower(): value for key, value in response.getheaders()}
        retry = found.get("retry-after", "").strip()
        result = {"status": response.status, "retry_after": int(retry) if retry.isdigit() and len(retry) <= 6 else None,
                  "body": None, "body_error": None}
        media = found.get("content-type", "").split(";", 1)[0].strip().lower()
        if total > limit:
            result["body_error"] = "too_large"
        elif found.get("content-encoding", "identity").strip().lower() not in ("identity", ""):
            result["body_error"] = "encoded"
        elif media != "application/json":
            result["body_error"] = "not_json"
        else:
            try:
                result["body"] = json.loads(b"".join(chunks).decode("utf-8", errors="strict"),
                                            parse_constant=reject_constant)
            except (ValueError, UnicodeError, RecursionError):
                result["body_error"] = "invalid_json"
        return result
    except Failure:
        raise
    except ssl.SSLCertVerificationError:
        raise Failure("tls", state["phase"]) from None
    except ssl.SSLError:
        raise Failure("tls" if state["phase"] == "connect" else "network", state["phase"]) from None
    except (socket.timeout, TimeoutError):
        raise Failure("timeout", state["phase"]) from None
    except (OSError, http.client.HTTPException, ValueError, AttributeError):
        raise Failure("network", state["phase"]) from None
    finally:
        conn = state.get("conn")
        if conn is not None:
            conn.close()


def abort(state):
    """Shut the socket down from the supervising thread; the helper thread then fails fast."""
    state["aborted"] = True
    conn = state.get("conn")
    sock = getattr(conn, "sock", None) if conn is not None else None
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def exchange(origin, ca_file, method, path, headers, body, *, deadline, cancel, limit):
    """Run one request in a helper thread and supervise cancellation and the deadline."""
    state, box = {"phase": "connect", "conn": None}, {}

    def target():
        try:
            box["result"] = perform(state, origin, ca_file, method, path, headers, body, deadline, limit)
        except Failure as failure:
            box["failure"] = failure
        except Exception:  # Defensive: an unexpected library error is a transport failure.
            box["failure"] = Failure("network", state["phase"])

    worker = threading.Thread(target=target, name="airtable-http", daemon=True)
    worker.start()
    while worker.is_alive():
        worker.join(0.05)
        if not worker.is_alive():
            break
        if cancel():
            phase = state["phase"]
            abort(state)
            worker.join(2)
            raise Failure("cancelled", phase)
        if time.monotonic() >= deadline + 0.5:
            phase = state["phase"]
            abort(state)
            worker.join(2)
            raise Failure("timeout", phase)
    if "failure" in box:
        raise box["failure"]
    return box["result"]


class Session:
    """One activation: one deadline, one token, paced and bounded requests."""

    def __init__(self, config, token, cancel=lambda: False):
        self.config, self.token = config, token
        self.cancel = cancel if callable(cancel) else (lambda: False)
        self.deadline = time.monotonic() + config["timeout_sec"]
        self.secrets = [token]
        self.requests = 0
        self.pace_key = (config["api_base_url"], config["base_id"])

    def pause(self, seconds):
        """Cancellation-aware wait before a paced request or a retry."""
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if self.cancel():
                logic.fail("cancelled", logic.MESSAGES["cancelled_before_send"])
            time.sleep(min(0.05, max(0.0, end - time.monotonic())))

    def paced(self):
        """Wait for a slot in the per-base rate window, within the operation deadline."""
        while True:
            wait = pace_slot(self.pace_key, self.config["max_requests_per_sec"])
            if wait <= 0:
                return
            if time.monotonic() + wait > self.deadline - 0.2:
                logic.fail("timeout")
            self.pause(wait)

    def send(self, method, path, headers, body, *, mutation):
        """One exchange mapped to a structured outcome; never raises a raw library error."""
        if self.cancel():
            logic.fail("cancelled", logic.MESSAGES["cancelled_before_send"])
        if self.deadline - time.monotonic() <= 0.2:
            logic.fail("timeout")
        self.paced()
        if self.cancel():
            logic.fail("cancelled", logic.MESSAGES["cancelled_before_send"])
        self.requests += 1
        try:
            return exchange(self.config["api_base_url"], self.config["ca_file"], method, path, headers, body,
                            deadline=self.deadline, cancel=self.cancel, limit=self.config["max_response_bytes"])
        except Failure as failure:
            after_send = failure.phase == "receive"
            uncertain = mutation and after_send
            if failure.code == "tls":
                logic.fail("tls")
            if failure.code == "timeout":
                logic.fail("timeout", logic.MESSAGES["timeout_uncertain" if uncertain else "timeout"], uncertain=uncertain)
            if failure.code == "cancelled":
                logic.fail("cancelled", logic.MESSAGES["cancelled" if uncertain else "cancelled_before_send"],
                           uncertain=uncertain)
            logic.fail("network", logic.MESSAGES["network_uncertain" if uncertain else "network"], uncertain=uncertain,
                       retryable=not mutation)

    def retrying(self, attempt, *, mutation):
        """Run attempt() with bounded retries: 429 always; 5xx and lost connections for reads only."""
        attempts = 0
        while True:
            attempts += 1
            try:
                return attempt()
            except logic.AirtableError as exc:
                exc.attempts = attempts
                retryable = exc.code == "rate_limited" or (not mutation and (
                    exc.code == "provider_unavailable" or getattr(exc, "retryable", False)))
                wait = exc.retry_after if exc.retry_after is not None else min(2 ** (attempts - 1), 8)
                if (retryable and attempts <= self.config["max_retries"] and wait <= self.config["max_retry_wait_sec"]
                        and time.monotonic() + wait + 1 < self.deadline):
                    self.pause(wait)
                    continue
                raise

    def call(self, method, path, query=None, payload=None, *, mutation):
        """Call <api_base_url><path>[?query] and return its JSON body or raise a coded error."""
        target = path + ("?" + query if query else "")
        headers = {"Authorization": "Bearer " + self.token, "Accept": "application/json", "Accept-Encoding": "identity",
                   "User-Agent": USER_AGENT, "Connection": "close"}
        if payload is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"

        def attempt():
            outcome = self.send(method, target, headers, payload, mutation=mutation)
            status = outcome["status"]
            if 200 <= status < 300:
                if outcome["body_error"] or not isinstance(outcome["body"], dict):
                    logic.fail("protocol", logic.MESSAGES["protocol_uncertain"] if mutation else None, uncertain=mutation)
                return outcome["body"]
            if status >= 500 and outcome["body_error"]:
                logic.fail("provider_unavailable", logic.MESSAGES["provider_unavailable_uncertain" if mutation else
                                                                  "provider_unavailable"], uncertain=mutation, http_status=status)
            logic.provider_failure(status, outcome["body"], self.secrets, mutation=mutation,
                                   retry_after=outcome["retry_after"])

        return self.retrying(attempt, mutation=mutation)
