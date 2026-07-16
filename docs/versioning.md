# Versioning and Release Policy

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/).

Given a version number MAJOR.MINOR.PATCH:
- **MAJOR**: incompatible API changes (wire protocol changes, interface breaking changes)
- **MINOR**: new backwards-compatible functionality
- **PATCH**: backwards-compatible bug fixes

## Breaking Change Policy

### What constitutes a breaking change

- Changes to the Raft wire protocol (AppendEntries, RequestVote, InstallSnapshot message formats)
- Changes to WAL or snapshot file formats that require migration
- Removal or signature changes of exported Go interfaces (FSM, Transport, LogStore, etc.)
- Changes to the YAML configuration schema that remove or rename required fields
- Admin API endpoint removals or non-backwards-compatible response format changes

### What is NOT a breaking change

- Adding new optional configuration fields (with documented defaults)
- Adding new admin API endpoints
- Improving error messages
- Performance improvements that don't affect correctness
- Adding new exported functions/types (only removals/changes are breaking)

## Release Process

1. Update `CHANGELOG.md` with all changes since last release
2. Create a version tag: `git tag -a v1.2.3 -m "Release v1.2.3"`
3. Push tag: `git push origin v1.2.3`
4. GitHub Actions release workflow builds and publishes binaries automatically
5. Update Docker image tag in Helm chart `values.yaml`

## Support Policy

- Latest MAJOR version: full support (bug fixes, security patches)
- Previous MAJOR version: security patches only for 6 months after new MAJOR release
- Older versions: community support only
