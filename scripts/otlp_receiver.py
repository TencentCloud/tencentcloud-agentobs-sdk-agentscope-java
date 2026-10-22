#!/usr/bin/env python3
"""
Minimal OTLP/HTTP trace receiver used to verify what the SDK actually puts on the wire.

Stands in for CLS at `/v1/traces`: accepts the protobuf `ExportTraceServiceRequest` the SDK
POSTs, decodes the few fields we care about (span name / ids / attributes) with a small
hand-rolled protobuf reader — no dependencies — and prints the reconstructed span tree.

Verifies three things an in-process span exporter cannot:
  1. the exporter really connects, authenticates and POSTs,
  2. the `Authorization` / `topic_id` headers CLS requires are present,
  3. the attributes survive serialisation intact.

Usage: python3 scripts/otlp_receiver.py [port]
"""
import sys
import os
import struct
import http.server


def read_varint(buf, i):
    val = 0
    shift = 0
    while True:
        b = buf[i]
        i += 1
        val |= (b & 0x7F) << shift
        if not b & 0x80:
            return val, i
        shift += 7


def fields(buf):
    """Yield (field_number, wire_type, value) for one protobuf message."""
    i = 0
    while i < len(buf):
        key, i = read_varint(buf, i)
        fnum, wtype = key >> 3, key & 0x7
        if wtype == 0:
            val, i = read_varint(buf, i)
        elif wtype == 1:
            val, i = buf[i:i + 8], i + 8
        elif wtype == 2:
            ln, i = read_varint(buf, i)
            val, i = buf[i:i + ln], i + ln
        elif wtype == 5:
            val, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"unsupported wire type {wtype}")
        yield fnum, wtype, val


def get(buf, want):
    return [v for f, _, v in fields(buf) if f == want]


def any_value(buf):
    for f, _, v in fields(buf):
        if f == 1:
            return v.decode("utf-8", "replace")
        if f == 2:
            return bool(v)
        if f == 3:
            return v - (1 << 64) if v >= 1 << 63 else v
        if f == 4:
            # double_value: wire type 1, little-endian IEEE 754. Without this branch every
            # floating-point attribute (temperature, top_p, the per-token latency) decoded as
            # an empty string and looked as though the SDK had never recorded it.
            return struct.unpack("<d", v)[0]
    return ""


def parse_span(buf):
    span = {"attrs": {}, "trace": "", "id": "", "parent": "", "name": ""}
    for fnum, _, val in fields(buf):
        if fnum == 1:
            span["trace"] = val.hex()
        elif fnum == 2:
            span["id"] = val.hex()
        elif fnum == 4:
            span["parent"] = val.hex()
        elif fnum == 5:
            span["name"] = val.decode("utf-8", "replace")
        elif fnum == 9:
            key = get(val, 1)[0].decode()
            raw = get(val, 2)
            span["attrs"][key] = any_value(raw[0]) if raw else ""
    return span


def parse_request(body):
    spans = []
    for rs in get(body, 1):                 # resource_spans
        for ss in get(rs, 2):               # scope_spans
            for sp in get(ss, 2):           # spans
                spans.append(parse_span(sp))
    return spans


def render_tree(spans):
    by_parent = {}
    for s in spans:
        by_parent.setdefault(s["parent"], []).append(s)
    roots = [s for s in spans if s["parent"] in ("", "0" * 16)
             or s["parent"] not in {x["id"] for x in spans}]

    lines = []

    def walk(span, depth):
        a = span["attrs"]
        tags = []
        for k in ("gen_ai.span.kind", "gen_ai.model.call.origin",
                  "gen_ai.step.type", "gen_ai.step.id", "gen_ai.session.id"):
            if k in a:
                tags.append(f"{k.split('.')[-1]}={a[k]}")
        lines.append("    " * depth
                     + f"{span['name']}  <{span['id']}>  [{', '.join(tags)}]")
        for child in by_parent.get(span["id"], []):
            walk(child, depth + 1)

    for r in roots:
        walk(r, 0)
    return lines


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

        print("\n=== OTLP POST %s ===" % self.path)
        print("  Authorization : %s" % self.headers.get("Authorization"))
        print("  topic_id      : %s" % self.headers.get("topic_id"))
        print("  Content-Type  : %s  (%d bytes)"
              % (self.headers.get("Content-Type"), len(body)))
        try:
            spans = parse_request(body)
        except Exception as exc:                      # noqa: BLE001 - diagnostic tool
            print("  !! failed to decode payload: %r" % exc)
            return
        print("  spans received: %d" % len(spans))
        for line in render_tree(spans):
            print("    " + line)
        # Dump the full attribute set of every span by default. Narrowing this has produced three
        # separate false "the SDK never recorded it" conclusions: first only framework-origin
        # spans were dumped (hiding the middleware path), then only chat spans (hiding agent and
        # tool attributes). Set OTLP_DUMP_KINDS to a comma-separated list of gen_ai.span.kind
        # values to narrow it deliberately.
        wanted = os.environ.get("OTLP_DUMP_KINDS", "").strip()
        kinds = {k.strip() for k in wanted.split(",") if k.strip()} if wanted else None
        for s in spans:
            kind = s["attrs"].get("gen_ai.span.kind", "?")
            if kinds is not None and kind not in kinds:
                continue
            origin = s["attrs"].get("gen_ai.model.call.origin")
            label = f"{kind}, origin={origin}" if origin else kind
            print("  --- %s attributes (%s) ---" % (s["name"], label))
            for k in sorted(s["attrs"]):
                v = str(s["attrs"][k])
                print("      %-42s %s" % (k, v[:120] + ("…" if len(v) > 120 else "")))
        sys.stdout.flush()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 14318
    print("[otlp-receiver] listening on http://127.0.0.1:%d/v1/traces" % port)
    sys.stdout.flush()
    http.server.HTTPServer(("127.0.0.1", port), Handler).serve_forever()
