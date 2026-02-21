import importlib
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd

from utils.csv_parser import parse_csv, validate_downloads_dataframe


os.environ["BLOB_CONNECTION_STRING"] = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=testaccount;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "EndpointSuffix=core.windows.net"
)

ingest_module = importlib.import_module("functions.v1.ingest")


class FakeRequest:
    def __init__(self, method="POST", route_params=None, json_body=None, headers=None):
        self.method = method
        self.route_params = route_params or {}
        self._json_body = json_body
        self.headers = headers or {}
        self.files = {}
        self.form = {}

    def get_json(self):
        if self._json_body is None:
            raise ValueError("Invalid JSON body")
        return self._json_body


class _FakeHttpResponse:
    def __init__(self, content=b"", status_code=200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class CsvValidationTests(unittest.TestCase):
    def test_parse_csv_maps_alias_headers_and_keeps_extra_columns(self):
        csv_text = (
            "download date,total_downloads,Campaign\n"
            "2026-01-01,100,Launch\n"
            "2026-01-02,125,Promo\n"
        )

        result = parse_csv(csv_text)

        self.assertIn("Date", result.columns)
        self.assertIn("Downloads", result.columns)
        self.assertIn("Campaign", result.columns)
        self.assertEqual(result["Downloads"].tolist(), [100, 125])
        self.assertTrue(isinstance(result["Date"].dtype, pd.DatetimeTZDtype))

    def test_validate_downloads_dataframe_rejects_monthly_cadence(self):
        rows = ["Date,Downloads"]
        monthly_dates = pd.date_range("2024-01-01", periods=15, freq="MS", tz="UTC")
        for idx, date_value in enumerate(monthly_dates):
            rows.append(f"{date_value.strftime('%Y-%m-%d')},{100 + idx}")
        csv_text = "\n".join(rows)

        parsed = parse_csv(csv_text)

        with self.assertRaisesRegex(ValueError, "daily or near-daily"):
            validate_downloads_dataframe(parsed)

    def test_validate_downloads_dataframe_resamples_monthly_when_requested(self):
        rows = ["Date,Downloads"]
        monthly_dates = pd.date_range("2024-01-01", periods=15, freq="MS", tz="UTC")
        for idx, date_value in enumerate(monthly_dates):
            rows.append(f"{date_value.strftime('%Y-%m-%d')},{150 + idx}")
        csv_text = "\n".join(rows)

        parsed = parse_csv(csv_text)
        validated = validate_downloads_dataframe(parsed, frequency_mode="resample_daily")

        self.assertGreater(len(validated), len(parsed))
        self.assertIn("input_frequency_warning", validated.attrs)
        gaps = validated["Date"].dt.normalize().diff().dropna().dt.days
        self.assertEqual(int(gaps.median()), 1)

    @patch("functions.v1.ingest.parse_rss_feed")
    @patch("functions.v1.ingest.save_podcast_blob")
    @patch("functions.v1.ingest.requests.get")
    @patch("functions.v1.ingest.load_podcast_blob")
    def test_ingest_returns_400_for_monthly_data(
        self,
        mock_load_podcast_blob,
        mock_requests_get,
        mock_save_podcast_blob,
        mock_parse_rss_feed,
    ):
        rows = ["Date,Downloads"]
        monthly_dates = pd.date_range("2024-01-01", periods=15, freq="MS", tz="UTC")
        for idx, date_value in enumerate(monthly_dates):
            rows.append(f"{date_value.strftime('%Y-%m-%d')},{200 + idx}")
        csv_text = "\n".join(rows)

        mock_load_podcast_blob.return_value = json.dumps(
            {"title": "Podcast", "rss_url": "https://example.com/feed.xml"}
        )
        mock_requests_get.return_value = _FakeHttpResponse(
            content=csv_text.encode("utf-8"),
            status_code=200,
        )
        mock_save_podcast_blob.return_value = "pod-1"
        mock_parse_rss_feed.return_value = pd.DataFrame(
            {"Date": pd.to_datetime(["2024-01-01"], utc=True), "Title": ["Episode 1"]}
        )

        req = FakeRequest(
            method="POST",
            route_params={"podcast_id": "pod-1"},
            json_body={"csv_url": "https://example.com/downloads.csv"},
            headers={"Content-Type": "application/json"},
        )

        response = ingest_module.ingest(req)
        body = json.loads(response.get_body().decode("utf-8"))

        self.assertEqual(response.status_code, 400)
        self.assertIn("daily or near-daily", body["message"])
        mock_parse_rss_feed.assert_not_called()

    @patch("functions.v1.ingest.add_seasonality_predictors")
    @patch("functions.v1.ingest.mark_potential_missing_episodes")
    @patch("functions.v1.ingest.perform_spike_clustering")
    @patch("functions.v1.ingest.add_episode_counts_and_titles")
    @patch("functions.v1.ingest.parse_rss_feed")
    @patch("functions.v1.ingest.save_podcast_blob")
    @patch("functions.v1.ingest.requests.get")
    @patch("functions.v1.ingest.load_podcast_blob")
    def test_ingest_resample_daily_accepts_monthly_data_with_warning(
        self,
        mock_load_podcast_blob,
        mock_requests_get,
        mock_save_podcast_blob,
        mock_parse_rss_feed,
        mock_add_episode_counts_and_titles,
        mock_perform_spike_clustering,
        mock_mark_potential_missing_episodes,
        mock_add_seasonality_predictors,
    ):
        rows = ["Date,Downloads"]
        monthly_dates = pd.date_range("2024-01-01", periods=15, freq="MS", tz="UTC")
        for idx, date_value in enumerate(monthly_dates):
            rows.append(f"{date_value.strftime('%Y-%m-%d')},{300 + idx}")
        csv_text = "\n".join(rows)

        mock_load_podcast_blob.return_value = json.dumps(
            {"title": "Podcast", "rss_url": "https://example.com/feed.xml"}
        )
        mock_requests_get.return_value = _FakeHttpResponse(
            content=csv_text.encode("utf-8"),
            status_code=200,
        )
        mock_save_podcast_blob.return_value = "pod-1"
        mock_parse_rss_feed.return_value = pd.DataFrame(
            {"Date": pd.to_datetime(["2024-01-01"], utc=True), "Title": ["Episode 1"]}
        )

        def fake_episode_counts(downloads_df, _episode_df):
            df = downloads_df.copy()
            row_count = len(df)
            df["Episodes Released"] = 0
            df["Episode_Titles"] = [[] for _ in range(row_count)]
            df["Clustered_Episode_Titles"] = [[] for _ in range(row_count)]
            return df

        def fake_spike_clustering(downloads_df, max_clusters=10):
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

        def fake_add_seasonality(downloads_df, date_col="Date"):
            df = downloads_df.copy()
            df["day_of_week"] = 0
            df["month"] = 1
            df["day_of_week_sin"] = 0.0
            df["day_of_week_cos"] = 1.0
            df["month_sin"] = 0.0
            df["month_cos"] = 1.0
            return df

        mock_add_episode_counts_and_titles.side_effect = fake_episode_counts
        mock_perform_spike_clustering.side_effect = fake_spike_clustering
        mock_mark_potential_missing_episodes.side_effect = fake_mark_missing
        mock_add_seasonality_predictors.side_effect = fake_add_seasonality

        req = FakeRequest(
            method="POST",
            route_params={"podcast_id": "pod-1"},
            json_body={
                "csv_url": "https://example.com/downloads.csv",
                "frequency_mode": "resample_daily",
            },
            headers={"Content-Type": "application/json"},
        )

        response = ingest_module.ingest(req)
        body = json.loads(response.get_body().decode("utf-8"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("warnings", body["result"])
        self.assertIn("resampled to daily", body["result"]["warnings"][0])
        self.assertGreater(len(body["result"]["data"]), len(monthly_dates))

    @patch("functions.v1.ingest.add_seasonality_predictors")
    @patch("functions.v1.ingest.mark_potential_missing_episodes")
    @patch("functions.v1.ingest.perform_spike_clustering")
    @patch("functions.v1.ingest.add_episode_counts_and_titles")
    @patch("functions.v1.ingest.parse_rss_feed")
    @patch("functions.v1.ingest.save_podcast_blob")
    @patch("functions.v1.ingest.requests.get")
    @patch("functions.v1.ingest.load_podcast_blob")
    def test_ingest_uses_fresh_rss_cache_without_fetch(
        self,
        mock_load_podcast_blob,
        mock_requests_get,
        mock_save_podcast_blob,
        mock_parse_rss_feed,
        mock_add_episode_counts_and_titles,
        mock_perform_spike_clustering,
        mock_mark_potential_missing_episodes,
        mock_add_seasonality_predictors,
    ):
        csv_text = "\n".join(
            [
                "Date,Downloads",
                "2026-01-01,100",
                "2026-01-02,110",
                "2026-01-03,120",
                "2026-01-04,130",
                "2026-01-05,140",
                "2026-01-06,150",
                "2026-01-07,160",
                "2026-01-08,170",
                "2026-01-09,180",
                "2026-01-10,190",
                "2026-01-11,200",
                "2026-01-12,210",
                "2026-01-13,220",
                "2026-01-14,230",
            ]
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        cached_episodes = [
            {"Date": "2026-01-01T00:00:00+00:00", "Title": "Episode 1"},
            {"Date": "2026-01-08T00:00:00+00:00", "Title": "Episode 2"},
        ]
        mock_load_podcast_blob.return_value = json.dumps(
            {
                "title": "Podcast",
                "rss_url": "https://example.com/feed.xml",
                "_rss_episode_cache": {"fetched_at": now_iso, "episodes": cached_episodes},
            }
        )
        mock_requests_get.return_value = _FakeHttpResponse(
            content=csv_text.encode("utf-8"),
            status_code=200,
        )
        mock_save_podcast_blob.return_value = "pod-1"

        def fake_episode_counts(downloads_df, _episode_df):
            df = downloads_df.copy()
            df["Episodes Released"] = 0
            df["Episode_Titles"] = [[] for _ in range(len(df))]
            df["Clustered_Episode_Titles"] = [[] for _ in range(len(df))]
            return df

        def fake_spike_clustering(downloads_df, max_clusters=10):
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

        def fake_add_seasonality(downloads_df, date_col="Date"):
            df = downloads_df.copy()
            df["day_of_week"] = 0
            df["month"] = 1
            df["day_of_week_sin"] = 0.0
            df["day_of_week_cos"] = 1.0
            df["month_sin"] = 0.0
            df["month_cos"] = 1.0
            return df

        mock_add_episode_counts_and_titles.side_effect = fake_episode_counts
        mock_perform_spike_clustering.side_effect = fake_spike_clustering
        mock_mark_potential_missing_episodes.side_effect = fake_mark_missing
        mock_add_seasonality_predictors.side_effect = fake_add_seasonality

        req = FakeRequest(
            method="POST",
            route_params={"podcast_id": "pod-1"},
            json_body={"csv_url": "https://example.com/downloads.csv"},
            headers={"Content-Type": "application/json"},
        )

        response = ingest_module.ingest(req)
        self.assertEqual(response.status_code, 200)
        mock_parse_rss_feed.assert_not_called()

    @patch("functions.v1.ingest.add_seasonality_predictors")
    @patch("functions.v1.ingest.mark_potential_missing_episodes")
    @patch("functions.v1.ingest.perform_spike_clustering")
    @patch("functions.v1.ingest.add_episode_counts_and_titles")
    @patch("functions.v1.ingest.parse_rss_feed")
    @patch("functions.v1.ingest.save_podcast_blob")
    @patch("functions.v1.ingest.requests.get")
    @patch("functions.v1.ingest.load_podcast_blob")
    def test_ingest_falls_back_to_stale_rss_cache_on_fetch_error(
        self,
        mock_load_podcast_blob,
        mock_requests_get,
        mock_save_podcast_blob,
        mock_parse_rss_feed,
        mock_add_episode_counts_and_titles,
        mock_perform_spike_clustering,
        mock_mark_potential_missing_episodes,
        mock_add_seasonality_predictors,
    ):
        csv_text = "\n".join(
            [
                "Date,Downloads",
                "2026-01-01,100",
                "2026-01-02,110",
                "2026-01-03,120",
                "2026-01-04,130",
                "2026-01-05,140",
                "2026-01-06,150",
                "2026-01-07,160",
                "2026-01-08,170",
                "2026-01-09,180",
                "2026-01-10,190",
                "2026-01-11,200",
                "2026-01-12,210",
                "2026-01-13,220",
                "2026-01-14,230",
            ]
        )

        stale_iso = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        cached_episodes = [
            {"Date": "2026-01-01T00:00:00+00:00", "Title": "Episode 1"},
            {"Date": "2026-01-08T00:00:00+00:00", "Title": "Episode 2"},
        ]
        mock_load_podcast_blob.return_value = json.dumps(
            {
                "title": "Podcast",
                "rss_url": "https://example.com/feed.xml",
                "_rss_episode_cache": {"fetched_at": stale_iso, "episodes": cached_episodes},
            }
        )
        mock_requests_get.return_value = _FakeHttpResponse(
            content=csv_text.encode("utf-8"),
            status_code=200,
        )
        mock_save_podcast_blob.return_value = "pod-1"
        mock_parse_rss_feed.side_effect = RuntimeError("rss unavailable")

        def fake_episode_counts(downloads_df, _episode_df):
            df = downloads_df.copy()
            df["Episodes Released"] = 0
            df["Episode_Titles"] = [[] for _ in range(len(df))]
            df["Clustered_Episode_Titles"] = [[] for _ in range(len(df))]
            return df

        def fake_spike_clustering(downloads_df, max_clusters=10):
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

        def fake_add_seasonality(downloads_df, date_col="Date"):
            df = downloads_df.copy()
            df["day_of_week"] = 0
            df["month"] = 1
            df["day_of_week_sin"] = 0.0
            df["day_of_week_cos"] = 1.0
            df["month_sin"] = 0.0
            df["month_cos"] = 1.0
            return df

        mock_add_episode_counts_and_titles.side_effect = fake_episode_counts
        mock_perform_spike_clustering.side_effect = fake_spike_clustering
        mock_mark_potential_missing_episodes.side_effect = fake_mark_missing
        mock_add_seasonality_predictors.side_effect = fake_add_seasonality

        req = FakeRequest(
            method="POST",
            route_params={"podcast_id": "pod-1"},
            json_body={"csv_url": "https://example.com/downloads.csv"},
            headers={"Content-Type": "application/json"},
        )

        response = ingest_module.ingest(req)
        body = json.loads(response.get_body().decode("utf-8"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("warnings", body["result"])
        self.assertIn("using cached episode metadata", body["result"]["warnings"][-1])
        mock_parse_rss_feed.assert_called_once()


if __name__ == "__main__":
    unittest.main()
