"""Airtable records through the Web API v0 with a wallet-held personal access token."""

import json
from bloxsmith_app.block_api import (APPLICATION_JSON, BlockDefinition, BlockRuntimeOutput, BlockRuntimePreparation,
                                     BlockRuntimeResult)
from . import logic, operations, transport, ui


# FB1 - Keep remote access off by default; require a wallet token reference, a base ID, a table and an HTTPS API origin.
# FB2 - List records (view, formula, fields, sort) in bounded pages with an exact continuation offset, and get one record.
# FB3 - Create, update (PATCH) and upsert (performUpsert) in batches of 10 only with allow_writes; delete also needs allow_delete.
# FB4 - Pace to the per-base rate limit, bound size, time and retries, honour 429 Retry-After, never replay ambiguous writes.
# FB5 - Apply one contract in both runtimes and every package origin, with prompt cancellation of pending requests.
# FB6 - Own translated responsive settings with draft Apply, a conditional write section and release-scoped assets.
class AirtableBlock(BlockDefinition):
    """Field values read from or written to a table are data, never instructions for this block."""

    kind = "airtable"

    def ui_assets(self, surface="modal"):
        """Every installation uses the same static release manifest."""
        return list(self.model.get("ui_assets", {}).get(surface, []))

    def prepare_runtime(self, context):
        """Validate settings without wallet or network access."""
        logic.configuration(context.config)
        return BlockRuntimePreparation()

    def fresh_inputs(self, context):
        """Only fresh deliveries count: a retained request is never executed again."""
        names = {int(port.id): str(port.name) for port in context.input_ports}
        if context.input_events:
            return [(names.get(event.input_port_id), event.value) for event in context.input_events]
        fresh = []
        for ident, name in names.items():
            attribute = context.input_attribute(name, str(ident))
            if attribute is not None and attribute.status == "updated" and attribute.value not in (None, ""):
                fresh.append((name, attribute.value))
        return fresh

    def outputs(self, context, values):
        return [BlockRuntimeOutput(port_id=int(port.id), port_name=port.name, content_type=APPLICATION_JSON,
                                   value=json.dumps(values[port.name], ensure_ascii=False, allow_nan=False))
                for port in context.output_ports if port.name in values]

    @staticmethod
    def summary(operation, result):
        if operation == "list_records":
            return f"{result['count']} record(s) in {result['pages']} page(s)"
        if operation == "get_record":
            return "1 record"
        if operation == "upsert_records":
            return (f"{len(result['created_record_ids'])} created, {len(result['updated_record_ids'])} updated "
                    f"in {result['batches']} batch(es)")
        verb = {"create_records": "created", "update_records": "updated", "delete_records": "deleted"}[operation]
        return f"{result['count']} record(s) {verb} in {result['batches']} batch(es)"

    def execute_runtime(self, context):
        """One request per activation; business failures go to the error port and the run continues."""
        request, secrets, config = {}, [], {}
        try:
            config = logic.configuration(context.config)
            items = self.fresh_inputs(context)
            if not items:
                return BlockRuntimeResult(status="skipped", outputs=[])
            if len(items) != 1 or items[0][0] != "request":
                logic.fail("invalid_input", "Send one request per activation; nothing was sent.")
            if not config["enabled"]:
                logic.fail("disabled")
            request = logic.parse_request(items[0][1], config)
            try:
                raw = context.resolve_secret(config["secret_ref"])
            except Exception:
                logic.fail("secret")
            token = logic.credentials(raw)
            secrets = [token]
            session = transport.Session(config, token, context.services.get("cancel_requested", lambda: False))
            values = operations.run(session, request, config)
            result = values["result"]
            operation = request["operation"]
            summary = self.summary(operation, result)
            return BlockRuntimeResult(status="success", outputs=self.outputs(context, values),
                content_type=APPLICATION_JSON, last_message=operation + ": " + summary,
                metadata={self.kind: {"status": "success", "operation": operation, "requests": result["requests"]}},
                logs=[f"[airtable] {operation}: {summary} after {result['requests']} request(s)."])
        except logic.AirtableError as exc:
            error = exc
        except (ValueError, TypeError, KeyError, OSError, RuntimeError):
            # Raw exception text could carry field data or credentials: publish a fixed diagnostic.
            error = logic.AirtableError("internal")
        operation = request.get("operation")
        envelope = error.envelope(operation=operation, request_id=request.get("request_id"),
                                  base_id=config.get("base_id") or None, table=request.get("table"))
        text = context.redact_secrets(logic.redact(json.dumps(envelope, ensure_ascii=False, allow_nan=False), secrets))
        return BlockRuntimeResult(status="success",
            outputs=[BlockRuntimeOutput(port_id=int(port.id), port_name=port.name, value=text,
                                        content_type=APPLICATION_JSON)
                     for port in context.output_ports if port.name == "error"],
            content_type=APPLICATION_JSON, last_message=error.code,
            metadata={self.kind: {"status": "error", "code": error.code, "uncertain": error.uncertain}},
            logs=[f"[airtable-error] {operation or 'request'}: {error.code}"])

    def handle_ui_action(self, *, node, action, values, payload=None):
        """Apply validates settings only; it never contacts Airtable or reads the wallet."""
        if action in {"save_settings", "modal_update_fields", "inspector_update_fields"}:
            patch = (values or {}).get("node_patch") or {}
            if "config" in patch:
                try:
                    logic.configuration({**self.default_config(), **(node.get("config") or {}), **patch["config"]})
                except (ValueError, TypeError) as exc:
                    detail = str(exc) if isinstance(exc, logic.AirtableError) else logic.MESSAGES["config"]
                    return {"error": self.translate("block.airtable.error", {"detail": detail},
                                                    fallback="Airtable settings rejected: " + detail)}
        result = super().handle_ui_action(node=node, action="modal_update_fields" if action == "save_settings" else action,
                                          values=values, payload=payload)
        return ui.replace_config(result, node)

    def render_modal(self, *, node, payload=None):
        return ui.modal(self, node, payload)

    def render_inspector_panel(self, *, node, payload=None):
        return ui.inspector(self, node, payload)

    def render_node_card(self, *, node, payload=None):
        """Show the access mode and default operation, never the wallet reference or base ID."""
        config = {**self.default_config(), **(node.get("config") or {})}
        operation = config.get("operation") if config.get("operation") in logic.OPERATIONS else "list_records"
        params = {"operation": operation}
        if config.get("enabled") is not True:
            key, params, fallback = "block.airtable.card_disabled", {}, "Remote access off"
        elif config.get("allow_writes") is not True:
            key, fallback = "block.airtable.card_read", f"Read only · {operation}"
        elif config.get("allow_delete") is True:
            key, fallback = "block.airtable.card_delete", f"Read, write and delete · {operation}"
        else:
            key, fallback = "block.airtable.card_write", f"Read and write · {operation}"
        return ui.card(self, node, key, params, self.translate(key, params, fallback=fallback))
