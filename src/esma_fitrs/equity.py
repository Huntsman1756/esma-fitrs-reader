"""Offline equity FITRS results, with explicit application periods (auth.044 v2/v3).

Discovery was checked against ESMA's esma_data_py. No upstream code is copied.
Native decimal text is preserved; a fresh file does not make an expired result current.
"""

from __future__ import annotations

import hashlib
import json
import re
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from defusedxml import ElementTree as ET

CATALOG = "https://registers.esma.europa.eu/solr/esma_registers_fitrs_files/select"
VERSION = "fitrs-equity/2"
METRICS = {
    "adt": "AvrgDalyTrnvr",
    "avt": "AvrgTxVal",
    "lis": "LrgInScale",
    "sms": "StdMktSz",
    "adnte": "AvrgDalyNbOfTxs",
}


def discover(as_of: date) -> list[dict]:
    params = urlencode(
        {
            "q": f"file_name:FULECR* AND creation_date:[* TO {as_of.isoformat()}T23:59:59Z]",
            "wt": "json",
            "rows": 100,
            "sort": "creation_date desc,file_name asc",
        }
    )
    with urlopen(CATALOG + "?" + params, timeout=45) as response:
        docs = json.load(response)["response"]["docs"]
    if not docs:
        raise ValueError("No equity FITRS full snapshot available")
    latest = docs[0]["creation_date"]
    selected = [row for row in docs if row["creation_date"] == latest]
    parts = {}
    for row in selected:
        match = re.fullmatch(r"FULECR_\d{8}_([A-Z])_(\d+)of(\d+)\.zip", row["file_name"])
        if (
            not match
            or urlparse(row["download_link"]).hostname != "fitrs.esma.europa.eu"
            or urlparse(row["download_link"]).scheme != "https"
        ):
            raise ValueError("Unexpected FITRS file")
        letter, part, total = match.groups()
        parts.setdefault(letter, []).append((int(part), int(total)))
    for group in parts.values():
        total = group[0][1]
        if any(n != total for _, n in group) or sorted(p for p, _ in group) != list(
            range(1, total + 1)
        ):
            raise ValueError("Incomplete FITRS full snapshot")
    return selected


def parse(path: Path, source: dict, sha: str) -> list[dict]:
    rows = []
    with zipfile.ZipFile(path) as archive:
        if sum(info.file_size for info in archive.infolist()) > 512 * 1024**2:
            raise ValueError("FITRS expanded payload exceeds limit")
        for member in archive.namelist():
            if not member.endswith(".xml"):
                continue
            with archive.open(member) as stream:
                for _, record in ET.iterparse(stream, events=["end"]):
                    if record.tag.rsplit("}", 1)[-1] != "EqtyTrnsprncyData":
                        continue

                    def text(path, record=record):
                        node = record.find("/".join("{*}" + p for p in path.split("/")))
                        return node.text.strip() if node is not None and node.text else None

                    row = {
                        "isin": text("Id"),
                        "classification": text("FinInstrmClssfctn"),
                        "methodology": text("Mthdlgy"),
                        "liquid": text("Lqdty"),
                        "report_from": text("RptgPrd/FrDtToDt/FrDt"),
                        "report_to": text("RptgPrd/FrDtToDt/ToDt"),
                        "application_from": text("ApplPrd/FrDtToDt/FrDt"),
                        "application_to": text("ApplPrd/FrDtToDt/ToDt"),
                        "reference_mic": text("RlvntMkt/Id"),
                        "reference_adnte": text("RlvntMkt/AvrgDalyNbOfTxs"),
                        "snapshot_date": source["creation_date"][:10],
                        "source_file": source["file_name"],
                        "source_url": source["download_link"],
                        "source_sha256": sha,
                        "locator": member + "#TechRcrdId=" + (text("TechRcrdId") or ""),
                    }
                    for key, native in METRICS.items():
                        row[key] = text("Sttstcs/" + native)
                        node = record.find("{*}Sttstcs/{*}" + native)
                        row[key + "_currency"] = node.get("Ccy") if node is not None else None
                    if row["isin"]:
                        rows.append(row)
                    record.clear()
    return rows


def discover_deltas(since: str, until: date) -> list[dict]:
    start = (date.fromisoformat(since) + timedelta(days=1)).isoformat()
    if start > until.isoformat():
        return []
    params = urlencode(
        {
            "q": "file_name:DLTECR* AND creation_date:["
            + start
            + "T00:00:00Z TO "
            + until.isoformat()
            + "T23:59:59Z]",
            "wt": "json",
            "rows": 1000,
            "sort": "creation_date asc,file_name asc",
        }
    )
    with urlopen(CATALOG + "?" + params, timeout=45) as response:
        body = json.load(response)["response"]
    if body["numFound"] > len(body["docs"]):
        raise ValueError("Delta catalog exceeds limit; prior snapshot retained")
    groups = {}
    for row in body["docs"]:
        match = re.fullmatch(r"DLTECR_(\d{8})_(\d+)of(\d+)\.zip", row["file_name"])
        url = urlparse(row["download_link"])
        if not match or url.scheme != "https" or url.hostname != "fitrs.esma.europa.eu":
            raise ValueError("Unexpected FITRS delta")
        day, part, total = match.groups()
        groups.setdefault(day, []).append((int(part), int(total)))
    for group in groups.values():
        total = group[0][1]
        if any(n != total for _, n in group) or sorted(p for p, _ in group) != list(
            range(1, total + 1)
        ):
            raise ValueError("Incomplete FITRS delta day; prior snapshot retained")
    return body["docs"]


def merge_delta(records: list[dict], updates: list[dict]) -> list[dict]:
    # A revised calculation replaces the same ISIN/methodology/reporting-period result.
    def key(row):
        return tuple(row.get(k) for k in ("isin", "methodology", "report_from", "report_to"))

    replaced = {key(row) for row in updates}
    return [row for row in records if key(row) not in replaced] + updates


def sync(root: Path, as_of: date | None = None) -> dict:
    as_of = as_of or date.today()
    full = discover(as_of)
    base_date = full[0]["creation_date"][:10]
    files = full + discover_deltas(base_date, as_of)
    raw = root / "transparency-raw"
    raw.mkdir(parents=True, exist_ok=True)
    records, sources = [], []
    for row in files:
        path = raw / row["file_name"]
        # Fetch again: ESMA can revise a publication under the same file name.
        with urlopen(row["download_link"], timeout=60) as response:
            final = urlparse(response.geturl())
            if final.scheme != "https" or final.hostname != "fitrs.esma.europa.eu":
                raise ValueError("Unexpected FITRS download destination")
            payload = response.read(32 * 1024**2 + 1)
        if len(payload) > 32 * 1024**2:
            raise ValueError("FITRS archive exceeds limit")
        sha = hashlib.sha256(payload).hexdigest()
        path = raw / (sha + ".zip")
        path.write_bytes(payload)
        parsed = parse(path, row, sha)
        if not parsed:
            raise ValueError("Empty FITRS file; prior snapshot retained")
        if row["file_name"].startswith("DLTECR"):
            records = merge_delta(records, parsed)
        else:
            records.extend(parsed)
        sources.append({**row, "sha256": sha, "records": len(parsed)})
    if not records:
        raise ValueError("No FITRS equity records parsed; prior snapshot retained")
    fingerprint = hashlib.sha256(
        json.dumps([VERSION, sources, as_of.isoformat()], sort_keys=True).encode()
    ).hexdigest()
    directory = root / "transparency" / fingerprint
    pointer_path = root / "transparency" / "CURRENT"
    if (
        (directory / "meta.json").exists()
        and (directory / "equity.parquet").exists()
        and pointer_path.exists()
        and pointer_path.read_text().strip() == fingerprint
    ):
        return json.loads((directory / "meta.json").read_bytes())
    directory.mkdir(parents=True, exist_ok=True)
    columns = list(records[0])
    table = pa.Table.from_pylist(records, schema=pa.schema([(key, pa.string()) for key in columns]))
    pq.write_table(
        table.sort_by([("isin", "ascending")]),
        directory / "equity.parquet",
        compression="zstd",
        row_group_size=2048,
    )
    meta = {
        "state": "LOADED",
        "snapshot_date": max(row["creation_date"][:10] for row in sources),
        "full_snapshot_date": base_date,
        "catalog_checked_through": as_of.isoformat(),
        "captured_at": datetime.now(UTC).isoformat(),
        "records": len(records),
        "sources": sources,
        "parser": VERSION,
        "scope": "FITRS equity full snapshot plus published daily deltas through catalog check",
    }
    (directory / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    pointer = root / "transparency" / "CURRENT.pending"
    pointer.write_text(fingerprint, encoding="ascii")
    pointer.replace(root / "transparency" / "CURRENT")
    return meta


class Transparency:
    def __init__(self, root: Path):
        self.root = root

    def current(self) -> Path | None:
        pointer = self.root / "transparency" / "CURRENT"
        if not pointer.exists():
            return None
        value = pointer.read_text().strip()
        if not re.fullmatch("[a-f0-9]{64}", value):
            raise ValueError("Invalid FITRS snapshot pointer")
        return pointer.parent / value

    def status(self) -> dict:
        current = self.current()
        return (
            json.loads((current / "meta.json").read_bytes()) if current else {"state": "NOT_LOADED"}
        )

    def instrument(self, isin: str, as_of: date) -> dict:
        current = self.current()
        if current is None:
            return {"state": "NOT_LOADED", "records": [], "applicable_record": None}
        with duckdb.connect(config={"threads": 2}) as con:
            cursor = con.execute(
                "SELECT * FROM read_parquet(?) WHERE isin=? "
                "ORDER BY application_from DESC NULLS LAST, report_to DESC NULLS LAST",
                [str(current / "equity.parquet"), isin],
            )
            names = [col[0] for col in cursor.description]
            records = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in records:
            begin, end = row["application_from"], row["application_to"]
            row["application_state"] = (
                "UNKNOWN"
                if not begin
                else "FUTURE"
                if begin > as_of.isoformat()
                else "EXPIRED"
                if end and end < as_of.isoformat()
                else "IN_PERIOD"
            )
        applicable = [
            row
            for row in records
            if row["application_state"] == "IN_PERIOD"
            and row["methodology"] in {"YEAR", "FFWK", "ESTM"}
        ]
        unique = {}
        for row in applicable:
            key = tuple(
                (k, v)
                for k, v in row.items()
                if k not in {"source_file", "source_url", "source_sha256", "locator"}
            )
            unique.setdefault(key, row)
        return {
            "state": "IN_APPLICATION_PERIOD"
            if len(unique) == 1
            else "CONFLICT"
            if unique
            else "NO_APPLICABLE_RESULT"
            if records
            else "NOT_IN_SNAPSHOT",
            "records": records,
            "applicable_record": next(iter(unique.values())) if len(unique) == 1 else None,
            "snapshot": self.status(),
            "as_of": as_of.isoformat(),
        }
