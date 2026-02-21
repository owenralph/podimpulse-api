import json
import os
import unittest
from unittest.mock import patch

import pandas as pd

from utils.retry import retry_with_backoff
from utils.spike_clustering import determine_optimal_clusters
from utils.episode_counts import add_episode_counts_and_titles
from utils.rss_parser import parse_rss_feed


# Ensure blob client initialization does not fail in test imports.
os.environ["BLOB_CONNECTION_STRING"] = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=testaccount;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "EndpointSuffix=core.windows.net"
)

from functions.v1.trend import trend  # noqa: E402
from functions.v1.predict import predict  # noqa: E402


class FakeRequest:
    def __init__(self, method="GET", route_params=None, params=None):
        self.method = method
        self.route_params = route_params or {}
        self.params = params or {}
        self.headers = {}


class RuntimeFixesTests(unittest.TestCase):
    def test_retry_with_backoff_retries_and_succeeds(self):
        state = {"attempts": 0}

        def flaky():
            state["attempts"] += 1
            if state["attempts"] < 3:
                raise RuntimeError("temporary")
            return "ok"

        wrapped = retry_with_backoff(
            flaky,
            exceptions=(RuntimeError,),
            max_attempts=3,
            initial_delay=0,
            backoff_factor=1,
        )
        self.assertEqual(wrapped(), "ok")
        self.assertEqual(state["attempts"], 3)

    def test_determine_optimal_clusters_single_sample(self):
        self.assertEqual(determine_optimal_clusters([[1.0, 2.0, 3.0]], max_clusters=10), 1)

    def test_episode_counts_handles_empty_titles(self):
        downloads_df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Downloads": [100, 120],
            }
        )
        episode_df = pd.DataFrame(
            {
                "Date": ["2026-01-01"],
                "Title": [None],
            }
        )
        result = add_episode_counts_and_titles(downloads_df, episode_df)
        self.assertIn("Clustered_Episode_Titles", result.columns)
        self.assertEqual(len(result), 2)

    @patch("utils.rss_parser.feedparser.parse")
    def test_parse_rss_feed_empty_result_has_expected_columns(self, mock_parse):
        class _Feed:
            entries = []

        mock_parse.return_value = _Feed()
        df = parse_rss_feed("https://example.com/feed.xml")
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ["Date", "Title"])

    @patch("functions.v1.trend.load_from_blob_storage")
    def test_trend_reads_payload_data(self, mock_load):
        mock_load.return_value = json.dumps(
            {
                "data": [
                    {"Date": "2026-01-01T00:00:00Z", "Downloads": 100},
                    {"Date": "2026-01-02T00:00:00Z", "Downloads": 110},
                    {"Date": "2026-01-03T00:00:00Z", "Downloads": 120},
                    {"Date": "2026-01-04T00:00:00Z", "Downloads": 130},
                ]
            }
        )
        req = FakeRequest(
            method="GET",
            route_params={"podcast_id": "pod123"},
            params={"days": "2"},
        )
        resp = trend(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertIn("trend_data", body["result"])

    @patch("functions.v1.predict.load_from_blob_storage")
    def test_predict_get_returns_saved_result(self, mock_load):
        mock_load.return_value = json.dumps(
            {
                "message": "Prediction completed successfully.",
                "result": [{"Date": "2026-01-01T00:00:00Z", "Downloads": 100}],
                "total_downloads": 100.0,
            }
        )
        req = FakeRequest(method="GET", route_params={"podcast_id": "pod123"})
        resp = predict(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertEqual(body["total_downloads"], 100.0)


if __name__ == "__main__":
    unittest.main()
