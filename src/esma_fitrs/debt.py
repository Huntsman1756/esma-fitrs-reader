"""FITRS non-equity results for debt CFIs: source observations, not trading permission.

ISIN and subclass records remain separate. No application date is inferred.
ESMA still publishes legacy SSTI values; their presence does not restore the waiver.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from defusedxml import ElementTree as ET

from esma_fitrs.equity import CATALOG

FIELDS = (
    "isin",
    "classification",
    "subclass_description",
    "subclass_criteria",
    "report_from",
    "report_to",
    "application_from",
    "application_to",
    "liquid",
    "lis_pre",
    "lis_pre_currency",
    "lis_post",
    "lis_post_currency",
    "ssti_pre",
    "ssti_pre_currency",
    "ssti_post",
    "ssti_post_currency",
    "source_file",
    "source_url",
    "source_sha256",
    "snapshot_date",
    "locator",
)
SCHEMA = pa.schema([(field, pa.string()) for field in FIELDS])
NOTICE = (
    "Resultados publicados por ESMA para renta fija; no prueban por sí solos vigencia "
    "ni autorización de ejecución. SSTI es un dato histórico/técnico: "
    "no habilita su antiguo waiver."
)
RULES_URL = "https://www.esma.europa.eu/annual-transparency-calculations-non-equity-instruments"


def validate_download_url(url: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "fitrs.esma.europa.eu"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in (None, 443)
    ):
        raise ValueError("Unexpected debt FITRS download destination")


def catalogue(prefix: str, as_of: date, since: str | None = None) -> list[dict]:
    q = f"file_name:{prefix}_*_D_* AND creation_date:[* TO {as_of.isoformat()}T23:59:59Z]"
    if since:
        q += f" AND creation_date:{{{since}T23:59:59Z TO *}}"
    params = urlencode(
        {
            "q": q,
            "wt": "json",
            "rows": 1 if prefix == "FULNCR" else 1000,
            "sort": "creation_date desc,file_name asc",
        }
    )
    with urlopen(CATALOG + "?" + params, timeout=60) as response:
        body = json.load(response)["response"]
    rows = body["docs"]
    if prefix == "FULNCR" and rows:
        # Bound the second request to the latest publication, not all historical full files.
        q += ' AND creation_date:"' + rows[0]["creation_date"] + '"'
        params = urlencode(
            {"q": q, "wt": "json", "rows": 1000, "sort": "creation_date desc,file_name asc"}
        )
        with urlopen(CATALOG + "?" + params, timeout=60) as response:
            body = json.load(response)["response"]
        rows = body["docs"]
    if body["numFound"] > len(rows):
        raise ValueError("Non-equity FITRS catalogue was truncated")
    if prefix == "FULNCR":
        if not rows:
            raise ValueError("No debt FITRS full snapshot available")
        rows = [row for row in rows if row["creation_date"] == rows[0]["creation_date"]]
    groups = {}
    for row in rows:
        match = re.fullmatch(prefix + r"_(\d{8})_D_(\d+)of(\d+)\.zip", row["file_name"])
        validate_download_url(row["download_link"])
        if not match:
            raise ValueError("Unexpected debt FITRS file")
        stamp, part, total = match.groups()
        if stamp != row["creation_date"][:10].replace("-", ""):
            raise ValueError("Debt FITRS publication dates disagree")
        groups.setdefault(stamp, []).append((int(part), int(total)))
    for group in groups.values():
        total = group[0][1]
        if any(n != total for _, n in group) or sorted(p for p, _ in group) != list(
            range(1, total + 1)
        ):
            raise ValueError("Incomplete debt FITRS publication")
    return sorted(rows, key=lambda row: (row["creation_date"], row["file_name"]))


def records(path: Path, source: dict, sha: str):
    with zipfile.ZipFile(path) as archive:
        # The official debt publication contains up to 500,000 verbose XML records per part.
        # Parsing is streamed; the declared expanded-size cap still rejects oversized archives.
        if sum(info.file_size for info in archive.infolist()) > 2 * 1024**3:
            raise ValueError("Debt FITRS expanded payload exceeds limit")
        for member in archive.namelist():
            if not member.endswith(".xml"):
                continue
            with archive.open(member) as stream:
                stack = []
                for event, record in ET.iterparse(stream, events=("start", "end")):
                    if event == "start":
                        stack.append(record)
                        continue
                    stack.pop()
                    if record.tag.rsplit("}", 1)[-1] != "NonEqtyTrnsprncyData":
                        continue

                    def text(path, record=record):
                        node = record.find("/".join("{*}" + p for p in path.split("/")))
                        return node.text.strip() if node is not None and node.text else None

                    row = {field: None for field in FIELDS}
                    row.update(
                        {
                            "isin": text("Id/ISINAndSubClss/ISIN"),
                            "classification": text("Id/ISINAndSubClss/FinInstrmClssfctn"),
                            "subclass_description": text("Id/ISINAndSubClss/DerivSubClss/Desc")
                            or text("Id/DerivSubClss/Desc"),
                            "report_from": text("RptgPrd/FrDtToDt/FrDt"),
                            "report_to": text("RptgPrd/FrDtToDt/ToDt"),
                            "application_from": text("ApplPrd/FrDtToDt/FrDt"),
                            "application_to": text("ApplPrd/FrDtToDt/ToDt"),
                            "liquid": text("Lqdty"),
                            "source_file": source["file_name"],
                            "source_url": source["download_link"],
                            "source_sha256": sha,
                            "snapshot_date": source["creation_date"][:10],
                            "locator": member + "#TechRcrdId=" + (text("TechRcrdId") or ""),
                        }
                    )
                    criteria = []
                    for criterion in record.findall(".//{*}SgmttnCrit"):
                        criteria.append({c.tag.rsplit("}", 1)[-1]: c.text for c in criterion})
                    row["subclass_criteria"] = json.dumps(criteria, sort_keys=True)
                    for field, native in (
                        ("lis_pre", "PreTradLrgInScaleThrshld"),
                        ("lis_post", "PstTradLrgInScaleThrshld"),
                        ("ssti_pre", "PreTradInstrmSzSpcfcThrshld"),
                        ("ssti_post", "PstTradInstrmSzSpcfcThrshld"),
                    ):
                        node = record.find("{*}" + native + "/{*}Amt")
                        if node is not None:
                            row[field] = node.text
                            row[field + "_currency"] = node.get("Ccy")
                    yield row
                    parent = stack[-1] if stack else None
                    record.clear()
                    if parent is not None:
                        parent.remove(record)


def sync(root: Path, as_of: date | None = None) -> dict:
    as_of = as_of or date.today()
    full = catalogue("FULNCR", as_of)
    sources = full + catalogue("DLTNCR", as_of, full[0]["creation_date"][:10])
    raw = root / "bond-transparency-raw"
    raw.mkdir(parents=True, exist_ok=True)
    target = root / "bond-transparency"
    target.mkdir(exist_ok=True)
    staging = target / ("pending-" + uuid.uuid4().hex)
    staging.mkdir()
    manifests = []
    for index, source in enumerate(sources):
        validate_download_url(source["download_link"])
        path = raw / source["file_name"]
        if not path.exists():
            with urlopen(source["download_link"], timeout=120) as response:
                validate_download_url(response.geturl())
                payload = response.read(100_000_001)
            if len(payload) > 100_000_000:
                raise ValueError("Debt FITRS compressed payload exceeds limit")
            temporary = path.with_suffix(".part")
            temporary.write_bytes(payload)
            temporary.replace(path)
        with path.open("rb") as handle:
            sha = hashlib.file_digest(handle, "sha256").hexdigest()
        count, batch = 0, []
        with pq.ParquetWriter(
            staging / f"part-{index}.parquet", SCHEMA, compression="zstd"
        ) as writer:
            for row in records(path, source, sha):
                batch.append(row)
                count += 1
                if len(batch) >= 10000:
                    writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
                    batch = []
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
        if count == 0:
            raise ValueError("Non-equity file contains no recognised transparency records")
        manifests.append({**source, "sha256": sha, "records": count})
    fingerprint = hashlib.sha256(
        json.dumps([manifests, as_of.isoformat()], sort_keys=True).encode()
    ).hexdigest()
    with duckdb.connect(config={"memory_limit": "512MB", "threads": 2}) as con:
        con.sql(
            "SELECT * EXCLUDE (n) FROM (SELECT *, row_number() OVER ("
            "PARTITION BY isin, classification, subclass_description, subclass_criteria, "
            "report_from, report_to ORDER BY snapshot_date DESC, source_file DESC) n "
            "FROM read_parquet(?)) WHERE n=1 ORDER BY isin",
            params=[str(staging / "part-*.parquet")],
        ).write_parquet(str(staging / "results.parquet"), compression="zstd", row_group_size=50000)
        count = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(staging / "results.parquet")]
        ).fetchone()[0]
    for part in staging.glob("part-*.parquet"):
        part.unlink()
    info = {
        "state": "LOADED",
        "records": count,
        "sources": manifests,
        "snapshot_date": sources[-1]["creation_date"][:10],
        "catalog_checked_through": as_of.isoformat(),
        "notice": NOTICE,
        "regime_reference": RULES_URL,
        "parser": "fitrs-debt/1",
        "captured_at": datetime.now(UTC).isoformat(),
    }
    (staging / "meta.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    destination = target / fingerprint
    if not destination.exists():
        staging.rename(destination)
    else:
        # The new check date is metadata, not a different source snapshot.
        for file in staging.iterdir():
            file.unlink()
        staging.rmdir()
    pointer = target / "CURRENT.pending"
    pointer.write_text(fingerprint, encoding="ascii")
    pointer.replace(target / "CURRENT")
    return info


def instrument(root: Path, isin: str, as_of: date) -> dict:
    pointer = root / "bond-transparency/CURRENT"
    if not pointer.exists():
        return {"state": "NOT_LOADED", "records": [], "applicable_record": None}
    name = pointer.read_text().strip()
    if not re.fullmatch("[a-f0-9]{64}", name):
        raise ValueError("Invalid debt FITRS snapshot pointer")
    directory = pointer.parent / name
    with duckdb.connect(config={"threads": 2}) as con:
        cursor = con.execute(
            "SELECT * FROM read_parquet(?) WHERE isin=? ORDER BY report_to DESC",
            [str(directory / "results.parquet"), isin],
        )
        names = [col[0] for col in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    for row in rows:
        row.update({"methodology": "NON_EQUITY", "application_state": "NOT_VERIFIED"})
    return {
        "state": "APPLICABILITY_NOT_VERIFIED" if rows else "NOT_IN_SNAPSHOT",
        "asset_scope": "NON_EQUITY",
        "records": rows,
        "applicable_record": None,
        "snapshot": json.loads((directory / "meta.json").read_bytes()),
        "notice": NOTICE,
        "regime_reference": RULES_URL,
        "as_of": as_of.isoformat(),
    }
