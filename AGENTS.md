# Project Working Rules

## Git branches and releases

- Create every new development branch from a three-segment version number: `release/X.Y.Z`.
- Do not create branches whose name is only letters or only a milestone name (for example, `m6` or `feature`).
- Create the matching annotated tag `vX.Y.Z` only when that version is complete and verified.
- Base the next version branch on the latest completed release branch, unless an explicit integration branch is required.

## Repository hygiene

- Do not add `etcd-dbsize-analyzer-codex-development-guide.md` to Git.
- Do not add anything under `docs/superpowers/` to Git.
