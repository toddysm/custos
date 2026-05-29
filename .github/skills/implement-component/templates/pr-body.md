Closes #<task-issue#>

Part of [#<tracker#>](../issues/<tracker#>) — `<PREFIX>-000-<COMPONENT>` (`<component>` implementation tracker).

## Summary

`<one-paragraph description matching the commit-message "what + why">`

## What changed

### `<area 1, e.g. New module>`

- `<file>` — `<what>`.
- `<file>` — `<what>`.

### `<area 2, e.g. Tests>`

- `<file>` — `<what>`.

### `<area 3, e.g. Docs>`

- `<file>` — `<what>`.

## Quality

- `ruff check` clean, `ruff format` clean.
- `mypy --strict` clean.
- Full suite: **`<N>` passed, `<P>` % coverage**.

## Tracker status

After merge, the `<PREFIX>-000-<COMPONENT>` tracker line for this task will be
ticked automatically by the `implement-component` skill. When the final task
merges, the tracker is auto-closed.
