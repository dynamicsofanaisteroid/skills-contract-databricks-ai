# Author: Farah Zamir – AI Engineer, Virgin Atlantic
# Created: March 2026
# Context: Authored independently during AI Evaluation Harness project
# Purpose: To evaluate and enforce governance for skill-based agentic systems in Databricks
# Databricks notebook source
# MAGIC %md
# MAGIC # SKILLS CONTRACT TEMPLATE
# MAGIC ### Governance Checklist for User-Created Databricks Skills
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Version:** 2.0 (Generalized Template)
# MAGIC **Date:** March 2026
# MAGIC **Author:** Farah Zamir, AI Engineer
# MAGIC **Purpose:** Pre-execution governance lens for Assistant-executed skills
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Is
# MAGIC
# MAGIC A **Skills Contract** is a structured governance checklist that the Assistant reads before executing a user-created skill. It makes safety constraints visible at the point of use — not buried in policy documents.
# MAGIC
# MAGIC **This is a lens, not a blocker.** Assess each risk category proportionally. If a guardrail isn't relevant to the current execution, note it as N/A with a brief reason. The goal is proportionate governance, not blanket restriction.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MANDATORY USAGE: When to Read This Contract
# MAGIC
# MAGIC **This contract MUST be read before executing ANY skill:**
# MAGIC
# MAGIC * **User-created skills** — Before the Assistant generates or executes any data pipeline, analysis workflow, or table operation
# MAGIC * **GitHub repository skills** — Before executing skills from `ai-dev-kit` or other external sources
# MAGIC * **Modified existing skills** — Before running any skill that has been edited or extended from its original scope
# MAGIC
# MAGIC **Why this is required:**
# MAGIC
# MAGIC Skills in Databricks inherit the workspace token and can execute ANY operation the token permits — including writes, deletes, and schema modifications on production tables. The platform has **no technical controls** that restrict skills to read-only operations or bounded table scope.
# MAGIC
# MAGIC **Without this contract review:**
# MAGIC * A skill could overwrite production tables without backup
# MAGIC * PII fields could be extracted without classification or redaction
# MAGIC * Token excess permissions could be exploited unknowingly
# MAGIC * No audit trail would exist of what was accessed or modified
# MAGIC * Data quality issues could propagate silently to downstream consumers
# MAGIC
# MAGIC **The contract makes risks visible BEFORE execution** — when they can still be prevented. Once a skill runs with write permissions, the damage is done.
# MAGIC
# MAGIC **Enforcement pattern:**
# MAGIC 1. User requests skill creation or execution
# MAGIC 2. Assistant reads this contract FIRST
# MAGIC 3. Assistant assesses skill code against Sections 1-5
# MAGIC 4. Assistant completes Section 6 sign-off
# MAGIC 5. If CRITICAL items pass → proceed with execution
# MAGIC 6. If CRITICAL items fail → flag to user for remediation
# MAGIC 7. Log sign-off as MLflow artifact for audit trail
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Current Application: F01 Pipeline Skill
# MAGIC
# MAGIC **Skill Name:** `f01-pipeline-skill`
# MAGIC **Scope:** Single Python script (`F01_Pipeline_Full.py`) containing 9 steps (not 5 separate notebooks)
# MAGIC **Purpose:** Data contract creation for AI Evaluation Harness Features 02-07
# MAGIC **Execution Model:** PySpark DataFrames (read-only)
# MAGIC **Outputs:** In-memory DataFrames (7M rows, 17 features), schema dictionary, DQ validation, sample extract (30 sessions)
# MAGIC **Governance:** MLflow logging (artifacts, metrics, execution summary)
# MAGIC **Runtime:** ~25-30 minutes on 4-core cluster
# MAGIC **Data Source:** `ai_observability_p.telemetry.concierge_traces` (telemetry sandbox, read-only)
# MAGIC **Phase Boundary:** Phase 1 exploratory analysis — no production system connection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Risk Categories (Sections 1-5)
# MAGIC
# MAGIC Each section below addresses a specific governance risk:
# MAGIC
# MAGIC 1. **SQL & Data Access** — Prevents unintended write operations
# MAGIC 2. **PII & Sensitive Fields** — Ensures field classification and redaction
# MAGIC 3. **Audit Logging** — Requires MLflow evidence trail
# MAGIC 4. **Permission & Token Scope** — Documents what the token can access
# MAGIC 5. **Context Window Safety** — Ensures contract persistence across sessions
# MAGIC
# MAGIC **Sign-off required (Section 6)** before skill execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Customization Guide
# MAGIC
# MAGIC To adapt this contract for a new skill:
# MAGIC
# MAGIC 1. **Update "Current Application" section** with skill-specific details (name, scope, execution model, data sources)
# MAGIC 2. **Review Sections 1-5** and mark each checklist item as:
# MAGIC    * `[x] PASS` — Compliant
# MAGIC    * `[ ] FAIL` — Requires remediation
# MAGIC    * `[ ] N/A` — Not applicable (with justification)
# MAGIC 3. **Complete Section 6 sign-off** before execution
# MAGIC 4. **Save completed contract** alongside skill code for traceability
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — DATA ACCESS GUARDRAILS
# MAGIC
# MAGIC **Risk:** Skills can read or write any accessible table. For data profiling/analysis skills, write operations are scope violations.
# MAGIC
# MAGIC **Instruction:** Before executing code, confirm:
# MAGIC * **Read-only operations** — No writes, deletes, schema changes
# MAGIC * **Bounded table scope** — Only tables defined in skill scope
# MAGIC * **Sampling governance** — Use defined sampling strategy, not full table dumps

# COMMAND ----------

# Section 1 Checklist: Data Access Guardrails

data_access_checklist = {
    "1.1 — Read-only operations confirmed": {
        "requirement": "All data operations must be read-only (spark.table, SELECT queries, DataFrame transforms). No writes, deletes, or schema modifications.",
        "f01_status": "[x] PASS — F01 Pipeline uses spark.table() and DataFrame transforms only. No saveAsTable, insertInto, or DDL operations.",
        "flag_if": "Code contains: saveAsTable, insertInto, createOrReplaceTempView (persisted), INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, CREATE, MERGE"
    },
    "1.2 — Table scope bounded": {
        "requirement": "Data access limited to tables explicitly listed in skill scope. No queries outside defined sandbox.",
        "f01_status": "[x] PASS — Hardcoded to ai_observability_p.telemetry.concierge_traces only. No dynamic table construction.",
        "flag_if": "Code queries tables not listed in skill scope or uses dynamic table name construction"
    },
    "1.3 — No schema modification": {
        "requirement": "No DDL statements. Do not create, alter, or drop tables, views, or schemas.",
        "f01_status": "[x] PASS — No DDL operations. All outputs in-memory (DataFrames).",
        "flag_if": "Code contains: CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE VIEW, CREATE SCHEMA"
    },
    "1.4 — Sampling governance": {
        "requirement": "Use defined sampling strategy. No full table extracts without justification.",
        "f01_status": "[x] PASS — Stratified random sampling (30 sessions from 4.09M, 2-5000 message filter).",
        "flag_if": "Extraction returns full table volume or sampling logic is not applied"
    }
}

for check, detail in data_access_checklist.items():
    print(f"{check}")
    print(f"  Requirement: {detail['requirement']}")
    print(f"  F01 Status: {detail['f01_status']}")
    print(f"  Flag if: {detail['flag_if']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — PII & SENSITIVE FIELD GUARDRAILS
# MAGIC
# MAGIC **Risk:** Skills that profile data will identify PII fields. Those fields must be redacted before sample extraction — not flagged for later handling.
# MAGIC
# MAGIC **Instruction:** Classify fields by sensitivity tier, apply redaction strategy, document decisions in MLflow.

# COMMAND ----------

# Section 2 Checklist: PII & Sensitive Field Guardrails

pii_guardrail_checklist = {
    "2.1 — Field classification applied before extraction": {
        "requirement": "All fields classified as SAFE, REVIEW, or RESTRICTED before sample generation. Only SAFE fields in sample by default.",
        "f01_status": "[x] PASS — Step 3 classifies 11 fields by semantic role (identifier, timestamp, text_payload). Step 4 redacts 4 RESTRICTED fields (trace_id, span_id, session_id, name).",
        "flag_if": "Sample generated before classification complete, or includes RESTRICTED fields"
    },
    "2.2 — Identifier redaction confirmed": {
        "requirement": "User/session/device identifiers hashed or excluded in sample extract.",
        "f01_status": "[x] PASS — 4 identifier fields excluded from safe_df. Synthetic MD5 hashes generated in normalized_df for session_id and message_id.",
        "flag_if": "Raw identifier values appear in output datasets"
    },
    "2.3 — Residual PII in text fields assessed": {
        "requirement": "Free-text fields assessed for residual PII (names, emails, phone, account refs) before inclusion.",
        "f01_status": "[x] PASS — Source data already PII-masked by observability service. user_input and assistant_response included as SAFE. Residual risk documented as LOW in execution summary.",
        "flag_if": "Free-text fields included without residual PII assessment"
    },
    "2.4 — PII Assessment documented for audit": {
        "requirement": "PII classification, redaction decisions, and rationale logged to MLflow as versioned artifact.",
        "f01_status": "[x] PASS — schema_dictionary.csv logged to MLflow (11 fields, is_sensitive flag). execution_summary.json includes pii_assessment section.",
        "flag_if": "PII assessment completes without logged artifact"
    }
}

for check, detail in pii_guardrail_checklist.items():
    print(f"{check}")
    print(f"  Requirement: {detail['requirement']}")
    print(f"  F01 Status: {detail['f01_status']}")
    print(f"  Flag if: {detail['flag_if']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — AUDIT LOGGING & EVIDENCE TRAIL
# MAGIC
# MAGIC **Risk:** Without audit artifacts, skill execution is opaque. Retroactive logging is incomplete and unacceptable.
# MAGIC
# MAGIC **Instruction:** MLflow experiment must be active before execution. Log schema dictionary, DQ results, and execution summary as artifacts.

# COMMAND ----------

# Section 3 Checklist: Audit Logging & Evidence Trail

audit_guardrail_checklist = {
    "3.1 — MLflow experiment initialized before execution": {
        "requirement": "MLflow experiment active and confirmed before skill execution begins. Run ID logged at start.",
        "f01_status": "[x] PASS — Step 0 initializes /Users/farah.zamir@fly.virgin.com/F01_Pipeline_Skill experiment. Run metadata includes mlflow_run_id.",
        "flag_if": "Skill execution begins without confirmed active MLflow run"
    },
    "3.2 — Schema Dictionary logged as artifact": {
        "requirement": "Schema Dictionary logged as versioned MLflow artifact (field classifications, data types, null rates).",
        "f01_status": "[x] PASS — Step 3 logs schema_dictionary.csv to MLflow (11 fields: field_name, data_type, semantic_role, is_sensitive, null_pct).",
        "flag_if": "Schema profiling completes without logged artifact"
    },
    "3.3 — DQ validation results logged": {
        "requirement": "All DQ check results (PASS/WARN/FAIL) logged as MLflow metrics and structured artifact.",
        "f01_status": "[x] PASS — Step 7.5 runs 7 DQ checks, logs each as metric (1=PASS, 0=FAIL), logs dq_validation.json with full details.",
        "flag_if": "DQ checks execute without logging outcomes to MLflow"
    },
    "3.4 — Execution summary produced on completion": {
        "requirement": "Structured execution summary logged at pipeline completion (run metadata, data contract, PII assessment, DQ results, permissions).",
        "f01_status": "[x] PASS — Step 9 generates execution_summary.json with: run_metadata, data_contract, pii_assessment, dq_validation, permissions_and_governance, reproducibility.",
        "flag_if": "Pipeline completes without producing structured execution summary"
    }
}

for check, detail in audit_guardrail_checklist.items():
    print(f"{check}")
    print(f"  Requirement: {detail['requirement']}")
    print(f"  F01 Status: {detail['f01_status']}")
    print(f"  Flag if: {detail['flag_if']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — PERMISSION & TOKEN SCOPE
# MAGIC
# MAGIC **Risk:** Skills inherit workspace token by default. No per-skill scoping exists. Excess permissions are latent risks.
# MAGIC
# MAGIC **Instruction:** Document token scope (what can it access/modify?). Confirm minimum necessary access. Enforce Phase 1/Phase 2 boundary.

# COMMAND ----------

# Section 4 Checklist: Permission & Token Scope

permission_guardrail_checklist = {
    "4.1 — Token scope documented": {
        "requirement": "Document token permissions: write access? Schema modification? Access to tables outside sandbox?",
        "f01_status": "[x] DOCUMENTED — execution_summary.json includes permissions_and_governance: 'read access to ai_observability_p.telemetry.concierge_traces only'.",
        "flag_if": "Token scope unknown or cannot be determined"
    },
    "4.2 — Minimum necessary access confirmed": {
        "requirement": "Confirm token does NOT have write access to production catalogs, financial tables, or Concierge AI system data.",
        "f01_status": "[x] PASS — Token scope: telemetry sandbox read-only. No write operations in code. execution_summary documents 'table_scope: bounded to telemetry sandbox only'.",
        "flag_if": "Token has write access to tables outside defined sandbox"
    },
    "4.3 — Phase 1 / Phase 2 boundary enforced": {
        "requirement": "Skill must NOT connect to, trigger, or feed data into live Concierge AI production system. Phase 1 boundary is hard constraint.",
        "f01_status": "[x] PASS — execution_summary documents 'phase_boundary: Phase 1 - exploratory analysis, no production deployment'. No production system integration.",
        "flag_if": "Skill output configured to feed into live production pipeline"
    }
}

for check, detail in permission_guardrail_checklist.items():
    print(f"{check}")
    print(f"  Requirement: {detail['requirement']}")
    print(f"  F01 Status: {detail['f01_status']}")
    print(f"  Flag if: {detail['flag_if']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — CONTEXT WINDOW SAFETY & CONSTRAINT PERSISTENCE
# MAGIC
# MAGIC **Risk:** In long sessions, context compression can silently remove safety constraints from earlier conversation.
# MAGIC
# MAGIC **Instruction:** Load contract fresh at session start. For long executions (>30 min), confirm constraints mid-session. Flag deviations, don't silently resolve.

# COMMAND ----------

# Section 5 Checklist: Context Window Safety

context_guardrail_checklist = {
    "5.1 — Contract loaded fresh at session start": {
        "requirement": "This contract must be read in full at start of every execution session. Cannot skip based on previous session.",
        "f01_status": "[x] CONFIRMED — Contract read before F01 Pipeline execution.",
        "flag_if": "Execution proceeds without reading contract in current session"
    },
    "5.2 — Mid-session constraint check": {
        "requirement": "For executions >30 minutes or >3 steps, re-confirm key constraints (read-only, PII redaction) at midpoint.",
        "f01_status": "[x] PASS — F01 Pipeline is single script (9 steps, ~25-30 min). DQ validation at Step 7.5 acts as mid-execution check (verifies row counts, null rates, schema completeness).",
        "flag_if": "Long execution reaches data extraction without constraint confirmation"
    },
    "5.3 — Deviations logged, not silently resolved": {
        "requirement": "If any situation conflicts with contract constraints or is ambiguous, stop and flag to user. Do not silently resolve.",
        "f01_status": "[x] CONFIRMED — Deviation protocol understood. execution_summary.json includes 'exceptions' field for recording deviations.",
        "flag_if": "Ambiguity or unexpected situation resolved without user notification"
    }
}

for check, detail in context_guardrail_checklist.items():
    print(f"{check}")
    print(f"  Requirement: {detail['requirement']}")
    print(f"  F01 Status: {detail['f01_status']}")
    print(f"  Flag if: {detail['flag_if']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — PRE-EXECUTION SIGN-OFF
# MAGIC
# MAGIC **Instruction:** Complete sign-off before skill execution. For F01 Pipeline, all checks are [x] PASS. For new skills, update checklist with actual assessment.
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Run the cell below to generate sign-off artifact
# MAGIC 2. Review each checklist item against your skill implementation
# MAGIC 3. Mark `[x]` for CONFIRMED, `[ ]` for issues, or N/A with reason
# MAGIC 4. Log sign-off as MLflow artifact: `skill_contract_signoff.json`

# COMMAND ----------

import json
from datetime import datetime

# Section 6: Pre-Execution Sign-Off
# For F01 Pipeline: All checks PASS (governance-compliant implementation)
# For new skills: Update status fields with actual assessment

sign_off = {
    "execution_metadata": {
        "skill_name": "f01-pipeline-skill",
        "skill_scope": "Single Python script (F01_Pipeline_Full.py), 9 steps",
        "executed_by": "farah.zamir@fly.virgin.com",
        "session_start": datetime.now().isoformat(),
        "mlflow_experiment": "/Users/farah.zamir@fly.virgin.com/F01_Pipeline_Skill",
        "contract_version": "2.0"
    },
    "sign_off_checklist": {
        "CRITICAL — Data access restricted to read-only operations": "[x] CONFIRMED - F01 uses spark.table() and DataFrame transforms only",
        "CRITICAL — Table scope bounded to defined sandbox": "[x] CONFIRMED - Hardcoded to ai_observability_p.telemetry.concierge_traces",
        "CRITICAL — PII classification complete before sample extraction": "[x] CONFIRMED - Step 3 classifies 11 fields, Step 4 redacts 4 identifiers",
        "CRITICAL — MLflow experiment active and logging confirmed": "[x] CONFIRMED - Step 0 initializes experiment, Steps 3-9 log artifacts/metrics",
        "CRITICAL — Phase 1 / Phase 2 boundary enforced": "[x] CONFIRMED - No production system connection, documented in execution summary",
        "CRITICAL — Contract read fresh in this session": "[x] CONFIRMED - Contract loaded before F01 Pipeline execution",
        "RECOMMENDED — Token scope confirmed as minimum necessary": "[x] CONFIRMED - Read-only telemetry sandbox, documented in execution summary",
        "RECOMMENDED — Identifier redaction confirmed": "[x] CONFIRMED - 4 fields redacted, synthetic MD5 hashes, residual PII = LOW",
        "RECOMMENDED — Mid-session constraint check": "[x] CONFIRMED - DQ validation at Step 7.5 acts as mid-execution check"
    },
    "dq_validation_summary": {
        "checks_executed": 7,
        "checks_passed": 7,
        "checks_warned": 0,
        "checks_failed": 0,
        "overall_status": "PASS"
    },
    "exceptions": [],
    "sign_off_status": "COMPLETE - All CRITICAL and RECOMMENDED items confirmed",
    "notes": "F01 Pipeline Skill is governance-compliant. All guardrails implemented and verified."
}

print(json.dumps(sign_off, indent=2))
print("\n" + "="*70)
print("CONTRACT STATUS: COMPLETE (F01 Pipeline)")
print("All CRITICAL and RECOMMENDED items confirmed.")
print("\nFor new skills: Update checklist items before execution.")
print("Log as MLflow artifact: mlflow.log_dict(sign_off, 'skill_contract_signoff.json')")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## About This Contract Template
# MAGIC
# MAGIC This Skills Contract template was developed for Virgin Atlantic's AI Evaluation Harness project. It implements the principle that safety constraints should be **architectural rather than conversational** — loaded fresh from persistent source at every execution.
# MAGIC
# MAGIC **The contract is proportionate by design.** Each guardrail is tied to a specific, evidenced risk. Where a risk is not present, mark it N/A with brief reason — not a blocker.
# MAGIC
# MAGIC **Review and update this contract when:**
# MAGIC * Skill scope changes (new data sources, new steps, new outputs)
# MAGIC * Execution environment changes (new token, new cluster, new catalog)
# MAGIC * New risk category identified through operational experience
# MAGIC * Phase 1 / Phase 2 boundary is formally reviewed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Customization Checklist for New Skills
# MAGIC
# MAGIC 1. **Cell 1 (Overview):** Update "Current Application" section with skill name, scope, execution model, data sources, runtime
# MAGIC 2. **Sections 1-5 (Guardrails):** Review each checklist item:
# MAGIC    * Update `f01_status` field to reflect your skill's actual implementation
# MAGIC    * Mark `[x] PASS` if compliant, `[ ] FAIL` if remediation needed, `[ ] N/A: [reason]` if not applicable
# MAGIC 3. **Section 6 (Sign-off):** Complete sign-off checklist before execution:
# MAGIC    * Update `execution_metadata` with skill details and MLflow experiment path
# MAGIC    * Mark each checklist item as `[x] CONFIRMED`, `[ ] EXCEPTION: [reason]`, or `[ ] N/A: [reason]`
# MAGIC    * Add any exceptions or deviations to `exceptions` array
# MAGIC    * Run cell and log output as MLflow artifact
# MAGIC 4. **Save completed contract** alongside skill code in version control
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Farah Zamir — AI Engineer, Virgin Atlantic — March 2026*