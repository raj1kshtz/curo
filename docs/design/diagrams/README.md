# Architecture Diagram Sources

The Mermaid files in this directory are the source of truth for Curo's
architecture diagrams. The SVG files are generated artifacts committed so
reviewers see the exact layout that was inspected.

## Render

The diagrams use Mermaid CLI `12.0.0`.

From the repository root:

```sh
npx --yes @mermaid-js/mermaid-cli@12.0.0 \
  -i docs/design/diagrams/system-context.mmd \
  -o docs/design/diagrams/system-context.svg \
  -b transparent
```

Repeat the command for each `.mmd` source. A pull request that changes a source
must update its SVG in the same commit.

## Visual Rules

- Use landscape, left-to-right flow unless independent horizontal lanes are
  clearer.
- Split a diagram rather than accept crossed connectors.
- Do not route a connector through a node or component boundary.
- Use one-way result terminals instead of overlapping return arrows.
- Keep each view focused on one architectural question.
- Use labels and line styles as well as color.

## Color Semantics

| Category | Fill | Border |
| -------- | ---- | ------ |
| Host or external | `#F1F5F9` | `#475569` |
| Public API | `#DBEAFE` | `#2563EB` |
| Safety boundary | `#FFE4E6` | `#E11D48` |
| Analysis | `#EDE9FE` | `#7C3AED` |
| Mitigation | `#FEF3C7` | `#D97706` |
| Bounded state | `#CCFBF1` | `#0F766E` |
| Observability | `#DCFCE7` | `#15803D` |

Solid connectors carry requests or results. Dashed connectors carry control
or configuration. Dotted connectors carry telemetry or time signals.
