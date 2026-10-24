"""Tải PM2.5 từ kho S3 OpenAQ, làm sạch + resample về theo giờ, đưa về schema chung (README mục 5, 7.1).

Ví dụ:
    python data/download_openaq.py --locations 12345 23456 --days 365
Kết quả:
    data/processed/openaq_hn_clean.parquet   (station_id, param, value, event_time_ms)
    data/processed/stations.csv              (bảng tra hn_01 -> location_id gốc)
    data/processed/cleaning_report.json      (số dòng đã bỏ ở mỗi bước, dùng cho báo cáo)
"""
import argparse
import gzip
import io
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

BUCKET = "openaq-data-archive"
RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def make_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client("s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED, max_pool_connections=32))


def list_keys(s3, loc, start, end):
    """Liệt kê file ngày nằm trong [start, end] của một location."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for year in range(start.year, end.year + 1):
        prefix = f"records/csv.gz/locationid={loc}/year={year}/"
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for o in page.get("Contents", []):
                m = re.search(r"-(\d{8})\.csv\.gz$", o["Key"])
                if not m:
                    continue
                d = date(int(m[1][:4]), int(m[1][4:6]), int(m[1][6:]))
                if start <= d <= end:
                    keys.append(o["Key"])
    return sorted(keys)


def fetch(s3, key):
    """Tải 1 file (có cache trong data/raw để chạy lại không phải tải lại)."""
    path = RAW_DIR / key.replace("records/csv.gz/", "")
    if path.exists():
        return path.read_bytes()
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return body


def read_gz_csv(data):
    return pd.read_csv(io.BytesIO(data), compression="gzip")


def clean(raw, station_map):
    """raw: DataFrame gộp các file. station_map: {location_id: 'hn_01'}.
    Trả về (df_sạch, report)."""
    rep = {"rows_raw": int(len(raw))}
    df = raw[raw["parameter"] == "pm25"].copy()
    rep["rows_pm25"] = int(len(df))

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    n = len(df); df = df[df["value"].notna()]
    rep["dropped_null"] = int(n - len(df))

    n = len(df); df = df[df["value"] >= 0]
    rep["dropped_negative"] = int(n - len(df))

    dt = pd.to_datetime(df["datetime"], utc=True, format="ISO8601")
    # tính bằng Timedelta để không phụ thuộc độ phân giải thời gian của pandas
    df["event_time_ms"] = ((dt - EPOCH) // pd.Timedelta(milliseconds=1)).astype("int64")
    df["station_id"] = df["location_id"].map(station_map)
    df["param"] = "pm25"

    df = df.sort_values(["station_id", "event_time_ms"], kind="stable")
    n = len(df)
    df = df.drop_duplicates(subset=["station_id", "event_time_ms"], keep="first")
    rep["dropped_duplicate_timestamp"] = int(n - len(df))
    rep["rows_clean"] = int(len(df))

    # README 7.1: tần suất theo giờ (1 mẫu/giờ). Trạm có tần suất cao hơn (vd 5 phút) được
    # resample: floor event_time_ms về đầu giờ rồi lấy trung bình. Trạm vốn theo giờ giữ nguyên giá trị.
    df["event_time_ms"] = (df["event_time_ms"] // 3_600_000) * 3_600_000
    df = df.groupby(["station_id", "event_time_ms"], as_index=False).agg(value=("value", "mean"))
    df["param"] = "pm25"

    df = df.sort_values(["event_time_ms", "station_id"], kind="stable").reset_index(drop=True)
    out = df[["station_id", "param", "value", "event_time_ms"]].copy()
    out["value"] = out["value"].astype("float64")
    rep["resample"] = "1 sample/hour: floor to hour + mean"
    rep["rows_hourly"] = int(len(out))
    rep["per_station"] = {k: int(v) for k, v in out.groupby("station_id").size().items()}
    # khoảng cách mẫu trung vị (phút) để kiểm tra có đúng dữ liệu theo giờ không
    gaps = {}
    for st, g in out.groupby("station_id"):
        d = g["event_time_ms"].diff().dropna()
        gaps[st] = float(d.median() / 60000) if len(d) else None
    rep["median_gap_minutes"] = gaps
    return out, rep


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--locations", nargs="+", type=int, required=True, help="location ID OpenAQ, theo thứ tự -> hn_01, hn_02...")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    args = ap.parse_args()
    start = args.end - timedelta(days=args.days - 1)

    s3 = make_client()
    station_map = {loc: f"hn_{i + 1:02d}" for i, loc in enumerate(args.locations)}
    frames, meta = [], []
    for loc in args.locations:
        keys = list_keys(s3, loc, start, args.end)
        print(f"location {loc}: {len(keys)} file trong {start} .. {args.end}")
        with ThreadPoolExecutor(16) as ex:
            blobs = list(ex.map(lambda k: fetch(s3, k), keys))
        if blobs:
            df = pd.concat([read_gz_csv(b) for b in blobs], ignore_index=True)
            frames.append(df)
            first = df.iloc[0]
            meta.append({"station_id": station_map[loc], "location_id": loc, "name": first["location"],
                         "lat": first["lat"], "lon": first["lon"]})
    if not frames:
        raise SystemExit("Không tải được dữ liệu nào. Kiểm tra lại location ID.")

    out, rep = clean(pd.concat(frames, ignore_index=True), station_map)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_DIR / "openaq_hn_clean.parquet", index=False)
    pd.DataFrame(meta).to_csv(OUT_DIR / "stations.csv", index=False)
    rep["period"] = [str(start), str(args.end)]
    (OUT_DIR / "cleaning_report.json").write_text(json.dumps(rep, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(rep, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()