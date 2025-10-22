# lambda_function.py (robust enrich: stays/visits)
import os, json, time, logging
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION      = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET   = os.environ["RAW_BUCKET"]          # processed/ も同じバケットを想定
PLACE_INDEX = os.environ["PLACE_INDEX"]         # Amazon Location Place index 名
MAX_RESULTS = int(os.environ.get("MAX_RESULTS", "1"))

s3  = boto3.client("s3", region_name=REGION)
loc = boto3.client("location", region_name=REGION)

def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def dump_siblings(bucket: str, key: str, limit: int = 50):
    prefix = key.rsplit("/", 1)[0] + "/"
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=limit)
        names = [c["Key"].split("/")[-1] for c in resp.get("Contents", [])]
        logger.info(f"[diag] list under {prefix}: {names}")
    except Exception as e:
        logger.info(f"[diag] list_objects_v2 failed: {e}")

def read_json(bucket: str, key: str, retries: int = 5, backoff: float = 0.3):
    dec_key = unquote_plus(key)
    last_err = None
    for i in range(retries):
        try:
            obj = s3.get_object(Bucket=bucket, Key=dec_key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError as e:
            code = e.response["Error"]["Code"]
            last_err = e
            if code in ("NoSuchKey", "404"):
                time.sleep(backoff * (2 ** i)); continue
            raise
    dump_siblings(bucket, dec_key)
    raise last_err

def write_json(bucket: str, key: str, obj):
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json"
    )

def reverse_geocode(lat: float, lon: float, retries: int = 3, backoff: float = 0.5):
    for i in range(retries):
        try:
            r = loc.search_place_index_for_position(
                IndexName=PLACE_INDEX,
                Position=[float(lon), float(lat)],  # [lon, lat]
                MaxResults=MAX_RESULTS,
                Language="ja"
            )
            results = r.get("Results", [])
            if not results:
                return None
            place = results[0].get("Place", {}) or {}
            return {
                "label": place.get("Label"),
                "country": place.get("Country"),
                "region": place.get("Region"),
                "subregion": place.get("SubRegion"),
                "municipality": place.get("Municipality"),
                "neighborhood": place.get("Neighborhood"),
                "postalCode": place.get("PostalCode"),
                "street": place.get("Street"),
                "name": place.get("Name")
            }
        except ClientError as e:
            logger.warning(f"reverse_geocode retry={i} code={e.response['Error']['Code']}")
            time.sleep(backoff * (2 ** i))
        except Exception:
            logger.exception("reverse_geocode error")
            time.sleep(backoff * (2 ** i))
    return None

def enrich_stays(stays: list):
    out = []
    for st in stays:
        c = st.get("center") or {}
        lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None:
            st["label"] = None
            out.append(st); continue
        info = reverse_geocode(lat, lon)
        if info:
            st["label"] = info.get("label")
            st["placeInfo"] = {k: v for k, v in info.items() if v and k != "label"}
        else:
            st["label"] = None
        out.append(st)
    return out

def enrich_visits(visits: list):
    out = []
    for v in visits:
        c = v.get("center") or v.get("point") or v.get("location") or v
        lat, lon = (c or {}).get("lat"), (c or {}).get("lon")
        if lat is None or lon is None:
            v["label"] = None
            out.append(v); continue
        info = reverse_geocode(lat, lon)
        if info:
            v["label"] = info.get("label")
            v["placeInfo"] = {k: v2 for k, v2 in info.items() if v2 and k != "label"}
        else:
            v["label"] = None
        out.append(v)
    return out

def lambda_handler(event, context):
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        raw_key = rec["s3"]["object"]["key"]
        key = unquote_plus(raw_key)
        logger.info(f"enrich target bucket={bucket} raw={raw_key} decoded={key} S3_BUCKET={S3_BUCKET}")

        if bucket != S3_BUCKET:
            logger.info("skip: bucket mismatch"); continue
        if not key.startswith("processed/"):
            logger.info("skip: prefix mismatch"); continue

        # stays.json
        if key.endswith("stays.json"):
            if not s3_key_exists(bucket, key):
                logger.error(f"[NoSuchKey] {key} not found. Dumping siblings.")
                dump_siblings(bucket, key)
                continue
            data = read_json(bucket, key)
            if not isinstance(data, list):
                logger.info("stays is not list; skip"); continue
            enriched = enrich_stays(data)
            out_key = key.rsplit("/", 1)[0] + "/stays_enriched.json"
            write_json(bucket, out_key, enriched)
            logger.info(f"wrote: s3://{bucket}/{out_key} (count={len(enriched)})")
            continue

        # visits.json
        if key.endswith("visits.json"):
            if not s3_key_exists(bucket, key):
                logger.error(f"[NoSuchKey] {key} not found. Dumping siblings.")
                dump_siblings(bucket, key)
                continue
            data = read_json(bucket, key)
            if not isinstance(data, list):
                logger.info("visits is not list; skip"); continue
            enriched = enrich_visits(data)
            out_key = key.rsplit("/", 1)[0] + "/visits_enriched.json"
            write_json(bucket, out_key, enriched)
            logger.info(f"wrote: s3://{bucket}/{out_key} (count={len(enriched)})")

    return {"ok": True}
