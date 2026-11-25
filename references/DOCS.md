# Official docs (fetched, not bundled)

The curated references in this skill (`functions.md`, `connections.md`, `generator-config.md`, `modifiers.md`, `operations.md`) work standalone.

For full upstream detail and examples, fetch ShadowTraffic's official docs into `references/docs/`:

```bash
python3 scripts/fetch_docs.py
```

That downloads the LLM-friendly bundle from [ShadowTraffic's AI context repo](https://github.com/ShadowTraffic/shadowtraffic-ai-context), splits it into one file per page under `references/docs/`, and writes a provenance README. The directory is git-ignored: the public repo does **not** redistribute ShadowTraffic's documentation.

- Source: https://docs.shadowtraffic.io
- Bundle: `markdown-docs/docs.md` (auto-synced by ShadowTraffic)
- Default ref: `master`; use `--ref <tag>` to pin a version
- Offline mirror: `--url file:///path/to/docs.md`

If a link such as `references/docs/functions/uuid.md` is missing, run the fetch script once. ShadowTraffic's documentation remains the property of ShadowTraffic; it is downloaded locally for reference only.
