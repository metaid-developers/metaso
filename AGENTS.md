# Agent Instructions

- Commit once for every round of modifications.
- Prefer small, frequent commits. Commit each independent, verifiable unit of work as soon as it is complete.
- For every modification or newly added feature, create one commit.
- Before committing, make sure the relevant local verification steps pass for your changes. Prefer the smallest meaningful verification set instead of defaulting to the full suite.
- When merging completed work into `main`, use `git merge --no-ff` to preserve the feature merge point.
- All documentation and code comments must be written in English.
- When spawning review or test subagents, default to model `gpt-5.5`.

## Local Verification Policy

- When creating a new branch or worktree, do not run the full test suite as a baseline by default. Use lightweight baseline checks such as `git status`, dependency availability, and only the task-relevant smoke/build command if the next change needs it.
- For focused documentation, prompt, UI copy, or narrowly scoped code changes, targeted tests plus the smallest relevant static or build check are sufficient before committing.
- Run the full test suite locally only when the change touches shared runtime behavior, chain writes, persistence formats, release artifacts, package/build plumbing, broad cross-module output, or when the user explicitly asks for full verification.
- After merging a completed branch into `main`, do not automatically run the full test suite for every low-risk branch. Re-run the same targeted checks on the merged result unless the merge combines high-risk areas, resolves conflicts, or changes release/build artifacts.
- Treat CI as the default full-suite gate for ordinary development merges. Local full-suite runs are still required before releases and when CI is unavailable or unsuitable for the risk involved.
