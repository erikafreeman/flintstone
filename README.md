---
title: Feuerstein
emoji: "\U0001FAA8"
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
license: mit
---

# Feuerstein — IGB Publication Intelligence Tool

Semantic search across 6,700+ IGB publications, collaborator discovery, study site maps, external partner network, and Open Practices Score.

Built as a companion to [FRED](https://fred.igb-berlin.de) (hence the name — Fred Feuerstein).

## Local Setup

```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```
