## Description

<!-- What does this PR do and why? Link related issues (e.g. "Fixes #123"). -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that changes existing behavior)
- [ ] Documentation / CI / tooling only

## Checklist

- [ ] My commits are signed off (`git commit -s`) per the [DCO](../CONTRIBUTING.md#developer-certificate-of-origin-dco)
- [ ] `make test-race` passes locally
- [ ] `make lint` passes locally (golangci-lint + staticcheck)
- [ ] `make vuln` passes locally (govulncheck)
- [ ] I added or updated tests for my changes
- [ ] I updated `CHANGELOG.md` and relevant docs
- [ ] If I changed `proto/raft.proto`, I regenerated stubs with `make proto`

## Testing

<!-- Describe how you tested this change (unit, race, integration, chaos, manual). -->

## Additional notes

<!-- Anything reviewers should pay special attention to. -->
