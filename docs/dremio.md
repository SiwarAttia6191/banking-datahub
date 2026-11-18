# Dremio on the lake

Dremio gives business users sql straight on the curated layer - no data copy, no
ingestion step. The lake stays the source of truth; dremio reads the parquet folders
in place.

## First run

1. start the service (it is part of docker-compose):
   `docker compose up -d dremio`
2. open http://localhost:9047 - the first launch asks you to create the admin user
3. add the lake as a source:
   - new source → s3
   - endpoint url: `http://s3:8333` (the compose network name of the seaweedfs service)
   - access key / secret: `dev-access-key` / `dev-secret-key`
   - bucket: `lake-curated`
   - enable "path-style access" in the advanced options
4. dremio now shows the curated folders - promote a folder (e.g.
   `curated/transactions`) to a dataset and query it

## Why this matters

- the lake is the source of truth: files on object storage, replayable, immutable
- dremio queries it **in place**: no data copy, no ingestion step for the bi layer
- in production this is the same pattern against the cloud object store (cos/s3)
- the postgres serving layer keeps a small row-level copy for the fastest dashboards;
  dremio covers everything else

this is the bi tool named in the job description - learned by running it on this lake.
