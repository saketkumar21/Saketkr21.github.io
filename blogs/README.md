# Blogs

Source-of-truth Markdown for posts published to [saket.whiz.pub](https://saket.whiz.pub).

The whiz.pub blog subdomain is the *rendered* output; this folder is where the drafts live and get version-controlled alongside the portfolio.

---

## File convention

- Filename: `YYYY-MM-DD-slug.md` (chronological, easy to sort).
- YAML frontmatter at the top with `title`, `slug`, `summary`, `tags`, `status`.
- Markdown body below.
- **The `slug` in frontmatter becomes the URL** on whiz.pub — `https://saket.whiz.pub/<slug>`.

Example:

```yaml
---
title: A dbt project. Snowflake to Spark. Zero rewrites.
slug: dbt-polyglot-launch
summary: How I built dbt-polyglot — a compile-time SQL-dialect transpiler…
tags: [dbt, snowflake, spark, iceberg, data-engineering, open-source]
status: published
---

# A dbt project. Snowflake to Spark. Zero rewrites.

...
```

---

## Publishing to whiz.pub

Two ways: CLI (recommended, `git`-friendly) or the web dashboard.

### One-time setup — CLI

```bash
# 1. Install the whiz CLI (single-file binary, no Node needed)
curl -sL https://whiz.pub/install | sh

# 2. Log in (opens browser or prompts for email + OTP code)
whiz login
```

Once logged in, your API key is stored locally in `~/.config/whiz/`.

### Publish a new post

```bash
# From the repo root
whiz publish blogs/2026-07-05-dbt-polyglot-launch.md
```

The CLI reads the frontmatter, sends the markdown to `https://api.whiz.pub/v1/posts`, and returns the live URL. `status: published` in the frontmatter goes live immediately; `status: draft` uploads it but keeps it unlisted.

### Update an existing post

Same command, same file. **`whiz publish` is upsert-by-slug** — running it again for the same `slug` updates the post in place. Edit the markdown, re-run the command, refresh the browser.

```bash
whiz publish blogs/2026-07-05-dbt-polyglot-launch.md
```

### Preview locally (optional)

```bash
whiz preview blogs/2026-07-05-dbt-polyglot-launch.md
```

Renders the post's markdown as it will appear on whiz.pub, without publishing.

---

## Alternate — REST API

If you want to script it (CI, GitHub Actions), the API is a simple `POST`:

```bash
curl -X POST https://api.whiz.pub/v1/posts \
  -H "Authorization: Bearer $WHIZ_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "A dbt project. Snowflake to Spark. Zero rewrites.",
    "slug": "dbt-polyglot-launch",
    "content": "# ... markdown body ...",
    "tags": ["dbt", "snowflake", "spark"],
    "summary": "How I built dbt-polyglot…",
    "status": "published"
  }'
```

Get `WHIZ_API_KEY` from <https://app.whiz.pub/settings>.

---

## Workflow suggestion

1. Draft in `blogs/YYYY-MM-DD-slug.md`.
2. `git commit` and push — post is version-controlled and appears in the portfolio repo.
3. `whiz publish blogs/…md` — goes live at `https://saket.whiz.pub/<slug>`.
4. Portfolio homepage fetches the whiz.pub RSS feed at build-time and shows the latest posts inline (see `src/components/BlogPreview.astro`).

That's it — one source of truth for the writing, two surfaces (whiz.pub + portfolio homepage), zero manual sync.

---

## Post index

| Date | Slug | Title | Status |
|---|---|---|---|
| 2026-07-05 | `dbt-polyglot-launch` | A dbt project. Snowflake to Spark. Zero rewrites. | Published |
