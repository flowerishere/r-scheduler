#!/usr/bin/env python3
"""Local demo webhook receiver; no third-party packages required.

The in-memory deduplication is only for demonstration. A real consumer should
persist run_id together with its business changes in a single transaction.
"""
import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--fail-first", type=int, default=0)
    args = parser.parse_args()
    state = {"requests": [], "accepted": {}}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, data):
            body = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                self.reply(200, {"status": "ok"})
            elif self.path == "/requests":
                with lock:
                    snapshot = list(state["requests"])
                self.reply(200, snapshot)
            else:
                self.reply(404, {"error": "not found"})

        def do_POST(self):
            if self.path != "/hook":
                self.reply(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 1024 * 1024:
                    raise ValueError("invalid body length")
                body = json.loads(self.rfile.read(length))
                key = self.headers.get("Idempotency-Key")
                if not key or key != body["run_id"]:
                    raise ValueError("missing or inconsistent idempotency key")
            except (ValueError, KeyError, TypeError) as error:
                self.reply(400, {"error": str(error)})
                return
            with lock:
                state["requests"].append(body)
                number = len(state["requests"])
                if number <= args.fail_first:
                    status, response = 503, {"error": "intentional demo failure"}
                elif key in state["accepted"]:
                    status, response = 200, {"accepted": True, "duplicate": True}
                else:
                    state["accepted"][key] = body
                    status, response = 200, {"accepted": True, "duplicate": False}
            print(json.dumps({"status": status, "event": body}, ensure_ascii=False), flush=True)
            self.reply(status, response)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer((args.bind, args.port), Handler)
    print(f"Webhook receiver: http://{args.bind}:{server.server_port}/hook", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
