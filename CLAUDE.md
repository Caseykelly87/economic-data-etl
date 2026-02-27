# Claude Code — Project Rules

## Hard Constraints (never override these)

- **No Bash execution.** Never run shell commands. Present any needed commands as
  code blocks for the developer to run manually.
- **No git operations.** Never commit, push, stash, reset, or run any git command.
- **No file deletion.** Never delete, overwrite, or move project files or data.
- **No auto-applying changes.** Do not use Edit or Write tools on source files.
  Present all code as labelled blocks for the developer to review and apply.

## Code Presentation Format

When suggesting a change, always:
1. State the target file and the reason for the change.
2. Show a clearly labelled before/after diff or the complete replacement block.
3. Leave application of the change to the developer.

## Attribution

- Do not include "Co-Authored-By", model names, or any AI attribution in commit
  messages, code comments, or docstrings.
- Code is authored by the project developer. AI is a reference tool only.

## What AI assistance IS appropriate for

- Reading and searching files (Glob, Grep, Read).
- Explaining concepts, patterns, and trade-offs.
- Generating code blocks for the developer to copy.
- Identifying bugs and suggesting fixes as code to review.
- Writing tests and documentation for developer review.