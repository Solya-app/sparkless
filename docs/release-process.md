# Release Process

Sparkless uses [semantic-release](https://semantic-release.gitbook.io/) for automated versioning and publishing.

## How It Works

1. **Push to `main`** with conventional commit messages
2. **Semantic-release** analyzes commits since the last tag, determines the version bump, creates a GitHub release + tag
3. **Publish job** builds the wheel and uploads to Azure DevOps Artifacts

## Conventional Commits

Semantic-release uses commit messages to determine the version bump:

| Prefix | Bump | Example |
|--------|------|---------|
| `fix:` | Patch (5.0.0 → 5.0.1) | `fix: resolve CaseWhen comparison bug` |
| `feat:` | Minor (5.0.0 → 5.1.0) | `feat: add dropTempView support` |
| `feat!:` or `BREAKING CHANGE:` | Major (5.0.0 → 6.0.0) | `feat!: remove Python 3.8 support` |
| `chore:`, `docs:`, `style:`, `ci:` | No release | `chore: clean up TODO files` |

## Workflow

### For development (local testing)

```bash
# In sparkless repo — make changes, test
python3 -m pytest tests/unit/ tests/parity/ -q -o "addopts="

# In data-platform repo — test with local sparkless
pip install -e /path/to/sparkless
# or
uv add --dev --editable /path/to/sparkless
```

### For release (automated)

```bash
# Just push with conventional commit messages
git push origin main

# Semantic-release will:
# 1. Analyze commits since last tag
# 2. Bump version in pyproject.toml and _version.py
# 3. Update CHANGELOG.md
# 4. Create git tag (e.g., v5.2.0)
# 5. Create GitHub release
# 6. Build and publish to Azure Artifacts
```

### Manual publish (emergency)

If you need to publish without semantic-release:

```bash
# Bump version manually
sed -i 's/version = "X.Y.Z"/version = "X.Y.W"/' pyproject.toml sparkless/_version.py

# Build
python -m build

# Publish to Azure Artifacts
twine upload \
  --repository-url "https://pkgs.dev.azure.com/solya-azure-devops/sparkless/_packaging/sparkless/pypi/upload/" \
  --username _ \
  --password "$AZURE_DEVOPS_PAT" \
  dist/sparkless-*.whl
```

## Configuration

### `.releaserc.json`

Defines semantic-release behavior:
- Branches: `main` only
- Tag format: `vX.Y.Z`
- Plugins: commit analyzer, release notes, changelog, version bumper, git committer, GitHub release

### GitHub Secrets

- `GITHUB_TOKEN` — auto-provided, used by semantic-release for tags and releases
- `AZURE_DEVOPS_PAT` — Azure DevOps Personal Access Token for Artifacts feed

### Azure Artifacts Feed

- **Organization**: `solya-azure-devops`
- **Project**: `sparkless`
- **Feed**: `sparkless`
- **Upload URL**: `https://pkgs.dev.azure.com/solya-azure-devops/sparkless/_packaging/sparkless/pypi/upload/`
- **Install URL**: `https://pkgs.dev.azure.com/solya-azure-devops/sparkless/_packaging/sparkless/pypi/simple/`

## Installing from Azure Artifacts

In `pyproject.toml`:

```toml
[[tool.uv.index]]
name = "solya-sparkless"
url = "https://pkgs.dev.azure.com/solya-azure-devops/sparkless/_packaging/sparkless/pypi/simple/"
explicit = true

[tool.uv.sources]
sparkless = { index = "solya-sparkless" }
```

Authentication via environment variables:
```bash
UV_INDEX_SOLYA_SPARKLESS_USERNAME=_
UV_INDEX_SOLYA_SPARKLESS_PASSWORD=<AZURE_DEVOPS_PAT>
```

## Version History

| Version | Description |
|---------|-------------|
| v5.0.0 | Solya fork — removed Rust/Robin, pure Python engine |
| v5.x.x | Bug fixes and feature additions for data-platform compatibility |
