# Decisions log

Running record of non-obvious choices and why. Append, never rewrite.
Format: `## YYYY-MM-DD — decision` / **Context** / **Decision** / **Why** / **Revisit if**

---

## 2026-09-01 — Spark 3.5, not Spark 4.x

**Context:** Spark 4.x is available.
**Decision:** Pin Spark 3.5.x.
**Why:** `iceberg-spark-runtime-3.5_2.12` is the most documented, most tested Iceberg
integration. A course project should not spend its budget debugging a bleeding-edge runtime.
**Revisit if:** an experiment needs a Spark 4-only feature.

---

## 2026-09-01 — Three differentiators chosen: E1, E2, E3

**Context:** Five candidate depth-differentiators were on the table (benchmarks, COW-vs-MOR,
failure injection, manifest internals, Iceberg v3 spec).
**Decision:** Build E1 (planning cost), E2 (COW vs MOR), E3 (exactly-once under chaos).
Manifest internals and v3 spec are stretch goals only.
**Why:** E1 and E2 share one benchmark harness, so the third costs little marginal effort.
E3 carries the highest interview value and is independent of the other two, so it fails
independently. v3 spec work depends on runtime support that is not yet verified on this
machine — it cannot be load-bearing.
**Revisit if:** Phase 3 finishes ahead of schedule.

---

## 2026-09-01 — DECIMAL, not DOUBLE, for `amount`

**Decision:** `DECIMAL(12,2)`.
**Why:** Money never uses binary floating point. Repeated MERGE rounds in E2 would accumulate
representation error and contaminate the E3 value-equality check.

---

## TEMPLATE — Phase 0 catalog decision (fill in when resolved)

**Context:** Iceberg needs a catalog. Hive Metastore 3.1.3 backed by Derby is already
installed, but Derby is single-writer and currently used embedded.
**Decision:** _HiveCatalog via Thrift on 9083_ / _HadoopCatalog on HDFS_ — record which, with
the error output if the fallback was taken.
**Why:**
**Revisit if:**
