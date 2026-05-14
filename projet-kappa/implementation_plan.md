# Implementation Plan: Complete the Kappa Architecture Project

## Context

After reading the course materials (Pr. Abakouy, ENSA Al Hoceima — Gestion de Projets 2025-2026), the professor's guide (`gp.docx`), and the existing code, here is what I understand:

- This is a **Project Management (GP) course**, not purely a coding course
- The professor provided a **step-by-step guide** (gp.docx) that covers 7 "Blocs" for the presentation + technical steps
- The technical implementation is a **vehicle to demonstrate GP concepts** (MOA/MOE, Scrum sprints, Gantt, risk management, etc.)
- The teammate followed the professor's guide up to **Phase 1 Step 3** (simulateur.py + docker-compose)

## What the Professor Expects (from gp.docx — 7 Blocs)

| Bloc | Content | Status |
|------|---------|--------|
| **Bloc 1** | Intro to Kappa vs Lambda | ❌ Not documented |
| **Bloc 2** | Actors & Roles (MOA=Marketing, MOE=Dev, Chef de Projet) | ❌ Not documented |
| **Bloc 3** | Planning (Gantt, WBS, estimation) | ✅ Gantt exists |
| **Bloc 4** | Implementation & Use Case (Kafka ingestion, Flink topology, real-time actions) | ⚠️ Partial (only ingestion) |
| **Bloc 5** | Methodology & Lifecycle (Agile/Scrum, Sprints, Validation/Recette) | ⚠️ Taiga exists but not documented |
| **Bloc 6** | Risk Management (data loss, delays, Planning Poker) | ❌ Not documented |
| **Bloc 7** | Conclusion & Perspectives (ML integration, bilan) | ❌ Not documented |

> [!IMPORTANT]
> The professor explicitly asks for:
> 1. A **filtering topology** that removes robot clicks
> 2. An **aggregation topology** that calculates real-time cart totals
> 3. A **cart abandonment detection** that triggers alerts to marketing
> 4. **Kafka retention** configuration (e.g., 7 days)
> 5. **Replay** capability (`--from-beginning`)
> 6. A new topic `alertes_marketing` for processed results

## Proposed Changes

### 1. Stream Processing Scripts (the missing Kappa core)

Since PyFlink setup in Docker is complex and the professor's guide says "in a simplified simulation, you can verify..." — I'll create **Python-based stream processors** using `kafka-python` that simulate what Flink would do. This is pragmatic and demonstrable.

#### [NEW] `stream_processing/filtrage.py`
- **Topology 1: Filtering** — Reads from `clics_ecommerce`, filters out robot clicks (bot user agents), writes clean events to `clics_filtres`
- Demonstrates: Kappa's single processing pipeline concept

#### [NEW] `stream_processing/agregation.py`
- **Topology 2: Aggregation** — Reads clean events, computes real-time stats per product (views, cart adds, purchases, abandons) in sliding windows
- Demonstrates: Continuous aggregation, windowing

#### [NEW] `stream_processing/detection_abandon.py`
- **Topology 3: Cart Abandonment Detection** — Detects users who added to cart but didn't purchase within a time window, writes alerts to `alertes_marketing`
- Demonstrates: Pattern matching, real-time business rules, actionable events

#### [NEW] `stream_processing/__init__.py`
- Package init

---

### 2. Kafka Retention & Replay Configuration

#### [NEW] `config/setup_topics.sh`
- Creates all topics with proper retention policies (`retention.ms=604800000` = 7 days)
- Topics: `clics_ecommerce`, `clics_filtres`, `alertes_marketing`, `stats_produits`
- Demonstrates: Event retention (key Kappa concept)

#### [NEW] `config/test_replay.py`
- Script that demonstrates replay: reads all events from beginning, recomputes stats, compares with real-time results
- Demonstrates: Kappa's killer feature — reprocessing by replaying the log

---

### 3. Improved Simulator

#### [MODIFY] `simulateur.py`
- Add bot/robot events (to demonstrate filtering topology)
- Add session tracking (to make abandonment detection meaningful)
- Add price data (to make aggregation more realistic)

---

### 4. Docker Compose Enhancement

#### [MODIFY] `docker-compose.yml`
- Add an init container or script to auto-create topics on startup
- Keep Flink for show (the professor expects it in the architecture)

---

### 5. Documentation for Each Component

#### [NEW] `docs/01_architecture_kappa.md`
- Bloc 1: Kappa vs Lambda comparison, why Kappa was chosen
- Architecture diagram of the full system

#### [NEW] `docs/02_acteurs_et_roles.md`
- Bloc 2: MOA (Marketing), MOE (Dev team), Chef de Projet, mapping to the team

#### [NEW] `docs/03_implementation_technique.md`
- Bloc 4: Detailed explanation of each component (simulateur, Kafka, topologies, alertes)
- Code walkthrough with explanations

#### [NEW] `docs/04_methodologie_agile.md`
- Bloc 5: Sprint descriptions, Kanban board explanation, validation process

#### [NEW] `docs/05_gestion_des_risques.md`
- Bloc 6: Risk matrix, mitigation strategies, Planning Poker reference

#### [NEW] `docs/06_retention_et_replay.md`
- Deep dive on event retention and replay — the heart of Kappa

#### [NEW] `docs/07_guide_execution.md`
- Step-by-step guide to run the entire demo from scratch

---

### 6. Final Report Structure

#### [NEW] `rapport/rapport_final.md`
- Complete project report following the professor's 7 Blocs structure
- Integrates all documentation, screenshots, and technical details
- Ready to be converted to PDF/Word for submission

---

### 7. Project README

#### [NEW] `README.md`
- Project overview, how to run, architecture diagram

---

## Verification Plan

### Automated Tests
- Start Docker containers
- Run simulateur.py → verify events appear in Kafka
- Run filtrage.py → verify bot events are filtered
- Run detection_abandon.py → verify alerts are generated
- Run test_replay.py → verify replay produces consistent results

### Manual Verification
- Review all documentation for completeness against the 7 Blocs
- Verify the report covers all course concepts (MOA/MOE, Gantt, Scrum, risks, etc.)

## Open Questions

> [!IMPORTANT]
> 1. **Team members' names?** I need them for the report cover page. I'll use placeholders for now.
> 2. **Presentation date?** The Gantt shows 16/04/2026 but that's already passed. Should I update it?
> 3. **Should the report be in French?** The course materials and existing code are in French, so I'll write everything in French.
