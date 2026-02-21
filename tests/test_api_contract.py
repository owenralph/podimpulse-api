import json
import os
import re
import unittest
from pathlib import Path


# Ensure imports that initialize blob clients do not fail in test imports.
os.environ["BLOB_CONNECTION_STRING"] = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=testaccount;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "EndpointSuffix=core.windows.net"
)

import function_app  # noqa: E402


HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


class _FakeRequest:
    method = "GET"


class ApiContractTests(unittest.TestCase):
    def _parse_routes_from_function_app(self):
        text = Path("function_app.py").read_text(encoding="utf-8")
        pattern = re.compile(r'@app\.route\(route="([^"]+)"(?:,\s*methods=\[([^\]]+)\])?\)')

        routes = {}
        for match in pattern.finditer(text):
            path = "/" + match.group(1)
            methods_raw = match.group(2)
            if methods_raw:
                methods = {m.strip().strip('"\'').upper() for m in methods_raw.split(",")}
            else:
                methods = {"GET"}
            if path.startswith("/v1/podcasts"):
                routes[path] = methods
        return routes

    def _parse_routes_from_openapi(self):
        lines = Path("podimpulse.yaml").read_text(encoding="utf-8").splitlines()
        routes = {}
        current_path = None

        for line in lines:
            if re.match(r"^  /v1/podcasts.*:$", line):
                current_path = line.strip()[:-1]
                routes[current_path] = set()
                continue

            if current_path is None:
                continue

            # leaving current path block
            if line.startswith("  /") and re.match(r"^  /.+:$", line):
                current_path = None
                continue

            m = re.match(r"^\s{4}([a-z]+):\s*$", line)
            if m and m.group(1).upper() in HTTP_METHODS:
                routes[current_path].add(m.group(1).upper())

        return routes

    def test_podcast_route_methods_match_openapi(self):
        app_routes = self._parse_routes_from_function_app()
        spec_routes = self._parse_routes_from_openapi()
        self.assertEqual(app_routes, spec_routes)

    def test_function_app_uses_function_auth_level(self):
        app_source = Path("function_app.py").read_text(encoding="utf-8")
        self.assertIn("func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)", app_source)

    def test_openapi_declares_function_key_security(self):
        spec_text = Path("podimpulse.yaml").read_text(encoding="utf-8")
        self.assertIn("securitySchemes:", spec_text)
        self.assertIn("functionKeyQuery:", spec_text)
        self.assertIn("functionKeyHeader:", spec_text)
        self.assertIn("name: code", spec_text)
        self.assertIn("name: x-functions-key", spec_text)

    def test_legacy_compute_routes_return_410(self):
        legacy_targets = [
            ("/v1/ingest", "/v1/podcasts/{podcast_id}/ingest"),
            ("/v1/missing", "/v1/podcasts/{podcast_id}/missing"),
            ("/v1/trend", "/v1/podcasts/{podcast_id}/trend"),
            ("/v1/impact", "/v1/podcasts/{podcast_id}/impact"),
            ("/v1/analyze_regression", "/v1/podcasts/{podcast_id}/regression"),
            ("/v1/predict", "/v1/podcasts/{podcast_id}/predict"),
        ]

        app_source = Path("function_app.py").read_text(encoding="utf-8")

        for route, replacement in legacy_targets:
            with self.subTest(route=route):
                # Verify source wiring maps legacy endpoint to the replacement path.
                self.assertIn(f'route="{route[1:]}"', app_source)
                self.assertIn(f'_legacy_route_gone("{replacement}")', app_source)

                # Verify shared legacy helper response shape/status.
                resp = function_app._legacy_route_gone(replacement)
                self.assertEqual(resp.status_code, 410)
                body = json.loads(resp.get_body().decode("utf-8"))
                self.assertIn("deprecated", body["message"].lower())
                self.assertIn(replacement, body["message"])


if __name__ == "__main__":
    unittest.main()
