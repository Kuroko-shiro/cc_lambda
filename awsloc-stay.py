# lambda_function.py
import os, json, boto3
from math import radians, sin, cos, asin, sqrt
from datetime import datetime, timezone

S3 = boto3.client("s3")

# ==== しきい値 ====
# stay
STAY_RADIUS_M = int(os.environ.get("STAY_RADIUS_M", "200"))      # 200m
STAY_MIN_SEC  = int(os.environ.get("STAY_MIN_SEC", "300"))       # 5分
# visits
VISIT_RADIUS_M = int(os.environ.get("VISIT_RADIUS_M", "120"))    # 120m
VISIT_MIN_SEC  = int(os.environ.get("VISIT_MIN_SEC", "30"))     # 30秒

def iso2dt(s):
    return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
def dt2iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def haversine_m(lat1, lon1, lat2, lon2):
    R=6371000.0
    dlat=radians(lat2-lat1); dlon=radians(lon2-lon1)
    a=sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2*R*asin(sqrt(a))

def read_points_jsonl(bucket, key):
    obj = S3.get_object(Bucket=bucket, Key=key)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    pts=[]
    for ln in lines:
        if not ln.strip(): continue
        rec=json.loads(ln)
        lat=float(rec.get("lat", rec.get("latitude")))
        lon=float(rec.get("lon", rec.get("longitude")))
        ts = rec.get("ts")
        if not ts:
            tms = rec.get("timestamp")
            if tms is not None:
                if tms > 1e12: tms = tms/1000.0
                ts = dt2iso(datetime.fromtimestamp(tms, tz=timezone.utc))
        if ts:
            pts.append({"lat":lat,"lon":lon,"ts":ts})
    pts.sort(key=lambda x: x["ts"])
    return pts

def detect_segments(points, radius_m, min_dur_sec):
    """
    半径 radius_m 内に min_dur_sec 以上とどまった連続区間を検出。
    単純スライディング（重心＋最大距離）で軽量に。
    """
    segs=[]
    n=len(points)
    if n==0: return segs
    start_idx=0

    def centroid(ws):
        return (sum(p["lat"] for p in ws)/len(ws), sum(p["lon"] for p in ws)/len(ws))

    i=1
    while i<=n:
        window = points[start_idx:i]
        latc, lonc = centroid(window)
        # ウィンドウ内の最大距離（重心から）
        max_r = 0.0
        for p in window:
            d = haversine_m(latc, lonc, p["lat"], p["lon"])
            if d > max_r: max_r = d
        dur = (iso2dt(window[-1]["ts"]) - iso2dt(window[0]["ts"])).total_seconds()

        if max_r > radius_m:
            if dur >= min_dur_sec and len(window) > 1:
                w2 = points[start_idx:i-1]
                latc2, lonc2 = centroid(w2)
                segs.append({
                    "center": {"lat": latc2, "lon": lonc2},
                    "start":  w2[0]["ts"],
                    "end":    w2[-1]["ts"]
                })
            start_idx = i-1
        i += 1
    if start_idx < n:
        window = points[start_idx:n]
        latc, lonc = centroid(window)
        dur = (iso2dt(window[-1]["ts"]) - iso2dt(window[0]["ts"])).total_seconds()
        if dur >= min_dur_sec:
            segs.append({
                "center": {"lat": latc, "lon": lonc},
                "start":  window[0]["ts"],
                "end":    window[-1]["ts"]
            })
    return segs

def write_json(bucket, key, obj):
    S3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, ensure_ascii=False, separators=(",",":")).encode("utf-8"),
        ContentType="application/json"
    )

def lambda_handler(event, context):
    # S3 イベント: processed/{deviceId}/date=YYYY-MM-DD/points.jsonl
    # 同じ場所に stays.json と visits.json を出力
    for rec in event.get("Records", []):
        bkt = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
        if not key.endswith("points.jsonl") or not key.startswith("processed/"):
            continue

        prefix = key.rsplit("/", 1)[0] + "/"
        points = read_points_jsonl(bkt, key)

        stays  = detect_segments(points, STAY_RADIUS_M,  STAY_MIN_SEC)
        visits = detect_segments(points, VISIT_RADIUS_M, VISIT_MIN_SEC)

        write_json(bkt, prefix + "stays.json",  stays)
        write_json(bkt, prefix + "visits.json", visits)
    return {"ok": True}
