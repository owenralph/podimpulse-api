import json
import os
import unittest
import uuid
import importlib
import warnings
from unittest.mock import patch

import pandas as pd


# Ensure module imports do not fail on blob client initialization.
os.environ["BLOB_CONNECTION_STRING"] = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=testaccount;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "EndpointSuffix=core.windows.net"
)

ingest_module = importlib.import_module("functions.v1.ingest")  # noqa: E402
regression_module = importlib.import_module("functions.v1.regression")  # noqa: E402
predict_module = importlib.import_module("functions.v1.predict")  # noqa: E402
trend_module = importlib.import_module("functions.v1.trend")  # noqa: E402
impact_module = importlib.import_module("functions.v1.impact")  # noqa: E402


class FakeRequest:
    def __init__(self, method="GET", route_params=None, params=None, json_body=None, headers=None):
        self.method = method
        self.route_params = route_params or {}
        self.params = params or {}
        self._json_body = json_body
        self.headers = headers or {}
        self.files = {}
        self.form = {}

    def get_json(self):
        if self._json_body is None:
            raise ValueError("Invalid JSON body")
        return self._json_body


class _FakeHttpResponse:
    def __init__(self, content=b"Date,Downloads\n2026-01-01,100\n"):
        self.content = content
        self.status_code = 200

    def raise_for_status(self):
        return None


class HandlerFlowTests(unittest.TestCase):
    def setUp(self):
        self.podcast_id = "pod-flow-1"
        self.store = {
            self.podcast_id: json.dumps(
                {"title": "Flow Podcast", "rss_url": "https://example.com/feed.xml"}
            )
        }
        self.patchers = []

        def fake_save_to_blob_storage(data, instance_id=None):
            if not instance_id:
                instance_id = f"auto-{uuid.uuid4().hex}"
            self.store[instance_id] = data
            return instance_id

        def fake_load_from_blob_storage(instance_id, binary=False):
            if instance_id not in self.store:
                raise RuntimeError("Blob not found")
            value = self.store[instance_id]
            if binary:
                if isinstance(value, bytes):
                    return value
                return str(value).encode("utf-8")
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return value

        def fake_parse_csv(_file_stream):
            dates = pd.date_range("2026-01-01", periods=45, freq="D", tz="UTC")
            downloads = [100 + (i % 9) * 15 + i for i in range(45)]
            return pd.DataFrame({"Date": dates, "Downloads": downloads})

        def fake_parse_rss_feed(_rss_url):
            episode_dates = pd.date_range("2026-01-01", periods=8, freq="7D", tz="UTC")
            return pd.DataFrame(
                {
                    "Date": episode_dates,
                    "Title": [f"Episode {i+1}" for i in range(len(episode_dates))],
                }
            )

        def fake_add_episode_counts_and_titles(downloads_df, _episode_df):
            df = downloads_df.copy()
            df["Episodes Released"] = 0
            df.loc[df.index % 7 == 0, "Episodes Released"] = 1
            df["Episode_Titles"] = df["Episodes Released"].apply(
                lambda count: ["Episode"] if count else []
            )
            df["Clustered_Episode_Titles"] = df["Episode_Titles"].apply(
                lambda titles: [{"title": t, "cluster": 0} for t in titles]
            )
            return df

        def fake_perform_spike_clustering(downloads_df, max_clusters=10):
            df = downloads_df.copy()
            df["is_spike"] = False
            df["is_anomalous"] = False
            return df

        def fake_mark_missing(downloads_df, _episode_dates, return_missing=False):
            df = downloads_df.copy()
            df["potential_missing_episode"] = False
            df["deduced_episodes_released"] = df["Episodes Released"]
            if return_missing:
                return df, []
            return df

        def fake_requests_get(_url, timeout=10):
            return _FakeHttpResponse()

        def fake_load_json_from_blob(token):
            if token not in self.store:
                raise RuntimeError("Blob not found")
            value = self.store[token]
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return value

        patch_specs = [
            ("functions.v1.ingest.load_podcast_blob", fake_load_from_blob_storage),
            ("functions.v1.ingest.save_podcast_blob", fake_save_to_blob_storage),
            ("functions.v1.ingest.parse_csv", fake_parse_csv),
            ("functions.v1.ingest.parse_rss_feed", fake_parse_rss_feed),
            ("functions.v1.ingest.add_episode_counts_and_titles", fake_add_episode_counts_and_titles),
            ("functions.v1.ingest.perform_spike_clustering", fake_perform_spike_clustering),
            ("functions.v1.ingest.mark_potential_missing_episodes", fake_mark_missing),
            ("functions.v1.ingest.requests.get", fake_requests_get),
            ("functions.v1.regression.load_podcast_blob", fake_load_from_blob_storage),
            ("functions.v1.regression.save_to_blob_storage", fake_save_to_blob_storage),
            ("functions.v1.predict.load_podcast_blob", fake_load_from_blob_storage),
            ("functions.v1.predict.load_from_blob_storage", fake_load_from_blob_storage),
            ("functions.v1.predict.save_to_blob_storage", fake_save_to_blob_storage),
            ("functions.v1.trend.load_podcast_blob", fake_load_from_blob_storage),
            ("functions.v1.impact.load_json_from_blob", fake_load_json_from_blob),
        ]

        for target, replacement in patch_specs:
            p = patch(target, replacement)
            self.patchers.append(p)
            p.start()

    def tearDown(self):
        for p in reversed(self.patchers):
            p.stop()

    def test_handler_flow_ingest_regression_predict_trend_impact(self):
        ingest_req = FakeRequest(
            method="POST",
            route_params={"podcast_id": self.podcast_id},
            json_body={"csv_url": "https://example.com/downloads.csv"},
        )
        ingest_resp = ingest_module.ingest(ingest_req)
        self.assertEqual(ingest_resp.status_code, 200)
        ingested_payload = json.loads(self.store[self.podcast_id])
        self.assertIn("data", ingested_payload)
        self.assertGreater(len(ingested_payload["data"]), 0)

        regression_req = FakeRequest(
            method="POST",
            route_params={"podcast_id": self.podcast_id},
            json_body={"target_col": "Downloads"},
        )
        regression_resp = regression_module.regression(regression_req)
        self.assertEqual(regression_resp.status_code, 200)
        self.assertIn(f"{self.podcast_id}_ridge_model.joblib", self.store)
        self.assertIn(f"{self.podcast_id}_regression_result.json", self.store)

        predict_post_req = FakeRequest(
            method="POST",
            route_params={"podcast_id": self.podcast_id},
            json_body={"episodes": 4, "release_dates": ["2026-02-20", "2026-02-27"]},
        )
        predict_post_resp = predict_module.predict(predict_post_req)
        self.assertEqual(predict_post_resp.status_code, 200)
        self.assertIn(f"{self.podcast_id}_prediction_result", self.store)

        predict_get_req = FakeRequest(method="GET", route_params={"podcast_id": self.podcast_id})
        predict_get_resp = predict_module.predict(predict_get_req)
        self.assertEqual(predict_get_resp.status_code, 200)
        predict_get_body = json.loads(predict_get_resp.get_body().decode("utf-8"))
        self.assertIn("total_downloads", predict_get_body)

        trend_req = FakeRequest(
            method="GET",
            route_params={"podcast_id": self.podcast_id},
            params={"days": "7"},
        )
        trend_resp = trend_module.trend(trend_req)
        self.assertEqual(trend_resp.status_code, 200)
        trend_body = json.loads(trend_resp.get_body().decode("utf-8"))
        self.assertIn("trend_data", trend_body["result"])

        impact_req = FakeRequest(method="GET", route_params={"podcast_id": self.podcast_id})
        impact_resp = impact_module.impact(impact_req)
        self.assertEqual(impact_resp.status_code, 200)
        impact_body = json.loads(impact_resp.get_body().decode("utf-8"))
        self.assertIn("impact_per_day", impact_body["result"])

    def test_trend_missing_days_returns_400(self):
        req = FakeRequest(method="GET", route_params={"podcast_id": self.podcast_id}, params={})
        resp = trend_module.trend(req)
        self.assertEqual(resp.status_code, 400)

    def test_impact_missing_podcast_returns_404(self):
        req = FakeRequest(method="GET", route_params={"podcast_id": "does-not-exist"})
        resp = impact_module.impact(req)
        self.assertEqual(resp.status_code, 404)

    def test_regression_post_avoids_duplicate_and_dtype_warnings(self):
        ingest_req = FakeRequest(
            method="POST",
            route_params={"podcast_id": self.podcast_id},
            json_body={"csv_url": "https://example.com/downloads.csv"},
        )
        ingest_resp = ingest_module.ingest(ingest_req)
        self.assertEqual(ingest_resp.status_code, 200)

        regression_req = FakeRequest(
            method="POST",
            route_params={"podcast_id": self.podcast_id},
            json_body={"target_col": "Downloads"},
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            regression_resp = regression_module.regression(regression_req)

        self.assertEqual(regression_resp.status_code, 200)
        warning_text = "\n".join(str(w.message) for w in caught)
        self.assertNotIn("DataFrame columns are not unique", warning_text)
        self.assertNotIn("incompatible dtype", warning_text)

    def test_regression_post_handles_small_dataset(self):
        dates = pd.date_range("2026-01-01", periods=8, freq="D")
        data = []
        for idx, date_value in enumerate(dates):
            data.append(
                {
                    "Date": date_value.strftime("%Y-%m-%dT00:00:00"),
                    "timezone": "GMT",
                    "Downloads": 100 + (idx * 7),
                    "Episodes Released": 1 if idx % 3 == 0 else 0,
                    "potential_missing_episode": False,
                }
            )
        self.store[self.podcast_id] = json.dumps(
            {"title": "Flow Podcast", "rss_url": "https://example.com/feed.xml", "data": data}
        )

        regression_req = FakeRequest(
            method="POST",
            route_params={"podcast_id": self.podcast_id},
            json_body={"target_col": "Downloads"},
        )
        regression_resp = regression_module.regression(regression_req)

        self.assertEqual(regression_resp.status_code, 200)
        body = json.loads(regression_resp.get_body().decode("utf-8"))
        self.assertIn("best_alpha", body["result"])
        self.assertGreaterEqual(body["result"]["n_train"], 1)
        self.assertGreaterEqual(body["result"]["n_test"], 1)


if __name__ == "__main__":
    unittest.main()
