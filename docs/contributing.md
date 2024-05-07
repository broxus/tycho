## Pull Requests

All contributions to `tycho` are made via Pull Requests (PRs). Please follow
these steps when creating a PR:

- **Fork the Repository:** Fork the repository or create a new branch in the
  repository if you have write access.
- **Commit Structure:** We review each commit separately, and we do not use the
  squash-merge strategy. Please manually combine any fixup commits before
  sending them for review. Each commit should ideally focus on a single task.
  For example, if you need to refactor a function to add a new feature cleanly,
  place the refactoring in one commit and the new feature in another. If the
  refactoring itself consists of many parts, separate these into individual
  commits. Include tests and documentation in the same commit as the code they
  test and document. The commit message should describe the changes in the
  commit; the PR description can be brief or even empty, but feel free to
  include a personal message.
- **Testing:** Ensure that the changes are thoroughly tested. If the PR
  introduces a new feature, it should also contain tests for that feature.
- **Draft PRs:** Feel free to submit draft PRs to get early feedback and to
  ensure you are on the right track.
- **Commit Naming Convention:** Each commit should follow [Conventional
  Commits](https://www.conventionalcommits.org/en/v1.0.0/). This helps us
  automatically generate changelogs and version bumps. (todo: implement this)
- **Documentation of Changes:** If your PR introduces a user-observable change (
  e.g., a new protocol feature, new configuration option, new Prometheus metric,
  etc.), please document it in [CHANGELOG.md](../CHANGELOG.md) in
  the `[unreleased]` section.
- **No Merge Policy:** The tycho repository uses
  a [no merge policy](./git.md#no-merge-policy). Please rebase your branch on
  top of the `master` branch before creating a PR.

