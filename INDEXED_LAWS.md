# Indexed Laws Tracker

This table tracks laws currently embedded into the Qdrant collection (`rada_legislation_mxbai`).

| Law ID | English Title | Sections | Chunks Indexed | Source URL |
|---|---|---:|---:|---|
| `2341-14` | Criminal Code of Ukraine | 567 | 3468 | https://zakon.rada.gov.ua/laws/show/2341-14 |
| `435-15` | Civil Code of Ukraine | 1340 | 3988 | https://zakon.rada.gov.ua/laws/show/435-15 |
| `80731-10` | Commercial Code of Ukraine (Economic Code) | 713 | 6682 | https://zakon.rada.gov.ua/laws/show/80731-10 |
| `254%D0%BA/96-%D0%B2%D1%80` | Constitution of Ukraine | 144 | 953 | https://zakon.rada.gov.ua/laws/show/254%D0%BA/96-%D0%B2%D1%80 |

## Notes

- Indexed total: **15091** chunks/vectors.
- Chunk counts are based on the current chunking config (`CHUNK_SIZE=400`, `CHUNK_OVERLAP=80`).
- If chunking settings or extraction logic changes, regenerate this table after re-indexing.
