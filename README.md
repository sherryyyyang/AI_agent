## Healthcare LLM Agent Pipeline (Simulated)

See demo (GitHub Pages): `https://<your-username>.github.io/<your-repo>/index.html`

**Important**: All data, statistics, and figures in this project are **fully simulated** and are **not representative of any real company, product, patient population, or commercial entity**. The goal is to showcase workflow and modeling patterns for healthcare analytics using LLM agents and LLM-based judges.

---

### 1. Query for Answer (LLM Agent)

This project includes a generic LLM-powered agent that turns free-text healthcare analytics questions into structured analysis outputs.

- **Core script**: `llm_agent_pipeline.py`  
- **High-level flow**:
  - Ingest a batch of questions from JSON (e.g., clinical trial, therapeutic outcome, or drug-pricing questions).
  - Normalize question structure and generate a clean case title / folder name via an LLM model.
  - Orchestrate a multi-step pipeline:
    - Query validation
    - Data / feature extraction
    - Statistical analysis
    - Visualization and results packaging
  - Persist structured JSON reports that downstream evaluators and dashboards can consume.

In a healthcare context, this pattern can be used to:

- Draft and refine **clinical trial protocols** and analysis plans.
- Summarize **therapeutic outcome** and real‑world evidence analyses.
- Explore scenarios for **drug pricing, market access, and market‑uptake modeling**.

---

### 2. LLM as a Judge (Healthcare Criteria)

The second part of the pipeline uses a separate LLM as a **judge**, guided by structured healthcare reporting and quantitative criteria, to evaluate the outputs from the first stage.

- **Healthcare reporting checklist judge** (`llm_judge_healthcare_criteria.py`):
  - Uses a detailed healthcare reporting checklist (covering study design, participants, outcomes, bias, interpretation, etc.).
  - For each generated finding, asks an LLM to assign 0/1 scores per checklist item plus textual justifications.
  - Produces machine-readable JSON scores that can be aggregated across questions and batches.

- **Statistical test type judge** (`llm_judge_stats_type.py`):
  - Reads JSON model outputs (coefficients, p‑values, hazard ratios, etc.).
  - Uses an LLM to infer which statistical tests are being used (e.g., regression vs. t‑tests vs. survival models).
  - Aggregates test types across many JSON files into a concise summary table.

- **Benchmark report generator** (`benchmark_report_generator.py`):
  - Aggregates pipeline and judge outputs across many runs.
  - Builds a Word report with:
    - Simulated success rate and runtime metrics.
    - Coverage and consistency relative to a generic **healthcare reporting criteria checklist**.

Together, these components demonstrate a pattern you can reuse in real healthcare settings: one LLM agent to generate answers and analytics, and a second LLM-based judge to audit, score, and explain the quality of those outputs against explicit healthcare-focused criteria. 