# skills-contract-databricks-ai
Governance-first evaluation &amp; risk scaffolding for Databricks AI Skills (authored independently during Virgin Atlantic apprenticeship)
# Databricks AI Skills Governance – Risk Evaluation & Contract Framework

## Author: Farah Zamir – AI Engineer, Virgin Atlantic  
**Date:** March 2026  
**Context:** Authored during development of AI Evaluation Harness  
**Purpose:**  

This repository contains two core governance artifacts:

- `skills_contract_eval_harness.py`  
  A structured, pre-execution Skills Contract for Databricks agentic skills – enforcing constraints like:
  - Read-only scope
  - PII redaction
  - MLflow audit trail
  - Context window safety
  - Permissions and schema modification guardrails

- `risk_analysis_ai_dev_kit.py`  
  A full critical risk evaluation of the unmodified Databricks `ai-dev-kit`, highlighting destructive operations that bypass user safeguards.

---

These artifacts were created independently during internal AI safety analysis at Virgin Atlantic, with the intent of surfacing platform-level governance needs in enterprise LLM environments.

## License  
This repository is authored and maintained by Farah Zamir. Attribution required for any reuse or extension.
