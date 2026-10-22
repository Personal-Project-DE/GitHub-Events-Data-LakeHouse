# Landing fixture

`2024-01-01-0.json.gz` is a tiny, synthetic GH Archive-compatible landing object. It represents the file downloaded for source hour `2024-01-01-0` and is deliberately gzip-compressed NDJSON: one complete GitHub event JSON object per line.

It contains four event types used by the project: `WatchEvent`, `ForkEvent`, `PullRequestEvent`, and `PushEvent`. The fixture is not a real GH Archive download and must only be used for local development and tests.

The production collector preserves the downloaded GH Archive object unchanged at:

```text
s3a://lakehouse/landing/gharchive/year=2024/month=01/day=01/2024-01-01-0.json.gz
```
