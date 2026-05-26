# Agent Instructions

## Code Style

- Keep code straightforward and beginner-readable.
- Write all source code, code comments, doc comments, test names, commit messages, and public documentation in English.
- Do not use fancy syntax sugar when a clear ordinary form is easier to read.
- Do not compress conditional logic into one-line `if` / `else` expressions.
- Write `if` / `else` branches on separate lines with explicit blocks.
- Add a short line comment above each meaningful logic block explaining what the block does.
- Add doc comments for every public method, function, class, and important internal helper.
- Prefer descriptive names over abbreviations.
- Avoid clever abstractions unless they remove real duplication or make the system easier to reason about.

## Testing Expectations

- Every milestone or PR that changes behavior must include tests.
- Do not validate a feature with only one happy-path case; include at least two meaningful test cases for each core behavior when the behavior is large enough to justify it.
- Use edge conditions, invalid inputs, failure paths, empty states, boundary values, dependency errors, or conflict scenarios as guidance for meaningful coverage when they are relevant to the behavior.
- Tests should prove the behavior introduced by the milestone, not only that the application starts.
- Test names and assertions should make the scenario and expected behavior clear to a reviewer.

## Change Management

- Do not do implementation work directly on `main`.
- Work on a feature, milestone, or fix branch. Reusing the current milestone branch is fine; creating a new branch for every tiny step is not required.
- Commit by logical step on the feature or milestone branch, not by arbitrary time slices.
- Do not manually squash local branch commits before opening a PR unless the user explicitly asks.
- Preserve useful logical commits on the branch for review; `main` stays clean through GitHub squash merge.
- Create PRs by milestone.
- Prefer one Codex goal per milestone when using goal-based execution.
- Milestones should have a clear done state and should avoid unnecessary decision branches.
- If a milestone requires a decision, document the recommended default and ask the user only when the choice materially changes scope, cost, or architecture.
- Agents may create commits and open PRs when a milestone or coherent logical step is complete.
- Agents must not merge PRs by themselves.
- When refactoring or deleting meaningful code, make a dedicated commit when the change is substantial enough to review independently.

## Benchmarkable Baselines

- For mechanisms that prove an engineering design choice, keep a runnable baseline mode and an improved mode in the current codebase.
- Do not rely on old commits as the only way to compare behavior or performance.
- Baseline and improved modes should be switchable through configuration, strategy selection, command flags, benchmark parameters, or eval arms.
- Benchmark and eval scripts should remain runnable after later milestones.
- Do not add A/B modes for every feature; use them only when the comparison creates a useful metric, debugging story, or design explanation.
- Do not remove a baseline mode unless it is moved to `Archive/` and the reason is documented.

## File Deletion Policy

- Do not permanently delete files with `rm`.
- Move files that would otherwise be deleted into `Archive/`.
- Keep `Archive/` ignored by git.
- If a file is moved to `Archive/`, mention it in the final summary.
