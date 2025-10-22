# lambda_function.py
import os, json, gzip, io
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3")
RAW_BUCKET = os.environ["RAW_BUCKET"]  # 例: awsloc-raw-xxxxx
LAMBDA = boto3.client("lambda")
STAYS_FN  = os.environ.get("STAYS_FUNCTION")   # 例: awsloc-stays
ENRICH_FN = os.environ.get("ENRICH_FUNCTION")  # 例: awsloc-enrich（任意）

def invoke_lambda_async(fn_name, bucket, key):
    """S3イベント風のペイロードで非同期起動（エラーは握りつぶす）"""
    if not fn_name: return
    event = {
        "Records":[{"s3":{"bucket":{"name":bucket},"object":{"key":key}}}]
    }
    try:
        LAMBDA.invoke(
            FunctionName=fn_name,
            InvocationType="Event",  # 非同期
            Payload=json.dumps(event).encode("utf-8")
        )
    except Exception as e:
        print(f"invoke {fn_name} failed: {e}")

# ===== 時刻ユーティリティ =====
def iso(dt):
    """ms/秒/ISO文字列を ISO8601Z に正規化"""
    if isinstance(dt, (int, float)):
        # ms を秒に
        if dt > 1e12:
            dt = dt / 1000.0
        return datetime.fromtimestamp(dt, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(dt, str):
        try:
            return datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc)\
                           .isoformat().replace("+00:00", "Z")
        except Exception:
            # 失敗時は原文返す（後段で弾かれる可能性あり）
            return dt
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")

def day_from_iso(ts_iso: str) -> str:
    """UTC基準 YYYY-MM-DD を返す"""
    return ts_iso[:10]

# ===== 正規化 =====
def normalize_payload(payload):
    """
    raw の2系統を吸収:
    ① 単発: {deviceId, timestamp(ms|sec|ISO), latitude, longitude}
    ② 日記: {deviceId, diaryCreatedAt, locations:[{lat|latitude, lon|longitude, timestamp}]}
    戻り値: (deviceId, [{"lat","lon","ts"}...])
    """
    device_id = payload.get("deviceId", "web-unknown")
    out = []

    if isinstance(payload.get("locations"), list):
        for p in payload["locations"]:
            lat = p.get("lat", p.get("latitude"))
            lon = p.get("lon", p.get("longitude"))
            ts  = p.get("timestamp")
            if lat is None or lon is None or ts is None:
                continue
            out.append({
                "deviceId": device_id,
                "lat": float(lat),
                "lon": float(lon),
                "ts": iso(ts)
            })
        return device_id, out

    # 単発
    lat = payload.get("latitude")
    lon = payload.get("longitude")
    ts  = payload.get("timestamp")
    if lat is not None and lon is not None and ts is not None:
        out.append({
            "deviceId": device_id,
            "lat": float(lat),
            "lon": float(lon),
            "ts": iso(ts)
        })
    return device_id, out

# ===== JSONL 読み書き（マージ追記） =====
def load_existing_jsonl(bucket, key):
    """既存 points.jsonl を配列で返す（無ければ空）"""
    try:
        obj = S3.get_object(Bucket=bucket, Key=key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        out=[]
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            try:
                r = json.loads(ln)
                # フォーマットの揺れにも耐える
                lat = r.get("lat", r.get("latitude"))
                lon = r.get("lon", r.get("longitude"))
                ts  = r.get("ts") or iso(r.get("timestamp"))
                if lat is None or lon is None or ts is None:
                    continue
                out.append({
                    "deviceId": r.get("deviceId", "web-unknown"),
                    "lat": float(lat),
                    "lon": float(lon),
                    "ts": ts
                })
            except Exception:
                pass
        return out
    except S3.exceptions.NoSuchKey:
        return []
    except Exception:
        return []

def write_merged_jsonl(bucket, key, new_records):
    """
    既存 points.jsonl を読み、new_records を結合して
    ts 昇順にソート、(ts,lat,lon) で重複除去してから書き戻す。
    """
    existing = load_existing_jsonl(bucket, key)
    merged = existing + new_records

    # 正規化＆重複除去（ts,lat,lon の3点一致でユニーク）
    def norm(p):
        return {
            "deviceId": p.get("deviceId", "web-unknown"),
            "lat": float(p.get("lat", p.get("latitude"))),
            "lon": float(p.get("lon", p.get("longitude"))),
            "ts":  p.get("ts") or iso(p.get("timestamp"))
        }

    seen=set(); uniq=[]
    for p in merged:
        try:
            q = norm(p)
            k = (q["ts"], round(q["lat"],6), round(q["lon"],6))
            if k in seen:
                continue
            seen.add(k); uniq.append(q)
        except Exception:
            continue

    # ts 昇順
    uniq.sort(key=lambda x: x["ts"])

    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in uniq) + "\n"
    print(f"merge: existing={len(existing)} new={len(new_records)} after={len(uniq)} key={key}")
    S3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"),
                  ContentType="application/jsonl")

# ===== S3 読み出し（.json / .json.gz 対応） =====
def read_raw_object(bucket, key):
    obj = S3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    if key.endswith(".gz"):
        data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
    return json.loads(data.decode("utf-8"))

# ===== メイン =====
def process_raw_object(bucket, key):
    """raw/ に置かれたオブジェクトを points.jsonl にマージ"""
    if bucket != RAW_BUCKET or not key.startswith("raw/"):
        return 0

    payload = read_raw_object(bucket, key)
    device_id, points = normalize_payload(payload)
    if not points:
        return 0

    # 日付ごとに分配して points.jsonl をマージ更新
    by_day = {}
    for p in points:
        d = day_from_iso(p["ts"])
        by_day.setdefault(d, []).append(p)

    total = 0
    for day, recs in by_day.items():
        out_prefix = f"processed/{device_id}/date={day}/"
        points_key = out_prefix + "points.jsonl"
        write_merged_jsonl(RAW_BUCKET, points_key, recs)
        invoke_lambda_async(STAYS_FN, RAW_BUCKET, points_key)

        # 可視化用スタブ（ダミー。後で日記生成に置き換える）
        stub_key = out_prefix + "diary_stub.txt"
        msg = f"Appended {len(recs)} points from {key}\n"
        S3.put_object(Bucket=RAW_BUCKET, Key=stub_key, Body=msg.encode("utf-8"),
                      ContentType="text/plain")
        total += len(recs)
    return total

def lambda_handler(event, context):
    count = 0
    for rec in event.get("Records", []):
        s3i = rec.get("s3", {})
        bucket = s3i.get("bucket", {}).get("name")
        key    = s3i.get("object", {}).get("key")
        if not bucket or not key:
            continue
        try:
            count += process_raw_object(bucket, key)
        except Exception as e:
            # 失敗しても他レコードは続行
            print(f"error processing {bucket}/{key}: {e}")
            continue
    return {"ok": True, "appended": count}
