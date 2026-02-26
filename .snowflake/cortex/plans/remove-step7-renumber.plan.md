# Plan: Remove Step 7 and Renumber Steps

## Overview
Remove the redundant `07_cortex_search` folder (old fabricated workflow) and renumber all subsequent steps.

## Current Structure
```
setup/
├── 07_cortex_search/      ← REMOVE (redundant with step 11)
├── 08_data_masking/       → becomes 07_data_masking
├── 09_object_tagging/     → becomes 08_object_tagging
├── 10_test_governance/    → becomes 09_test_governance
├── 11_gong_analysis/      → becomes 10_gong_analysis
├── 12_web_search/         → becomes 11_web_search
├── 13_postgres_activity_logs/ → becomes 12_postgres_activity_logs
```

## Tasks

### Task 1: Remove setup/07_cortex_search/
Delete the entire folder containing:
- `01_create_transcript_content.sql`
- `02_create_transcript_search.sql`

### Tasks 2-7: Rename Folders
Use `mv` commands to rename each folder sequentially.

### Task 8: Update MASTER_README.md
- Update folder structure diagram
- Renumber all step references (Step 7-13 → Step 7-12)
- Update table of contents
- Ensure all step descriptions match new numbers

## Final Structure
```
setup/
├── 01_database_setup/
├── 02_raw_tables/
├── 03_sfdc_data/
├── 04_refined_data/
├── 05_pdf_contracts/
├── 06_dynamic_tables/
├── 07_data_masking/
├── 08_object_tagging/
├── 09_test_governance/
├── 10_gong_analysis/
├── 11_web_search/
├── 12_postgres_activity_logs/
```
