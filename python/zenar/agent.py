"""Tool definitions that can render themselves as prompts.

`Collection.tool_spec(name)` returns a `ToolSpec`. It is a plain dict, so it can be passed to a
tool-calling API as is, and it also has `create_prompt()` for the system prompt.
"""

from __future__ import annotations

_CALLING_RULES = (
    "Call this tool with `query`: the search text, in natural language. "
    "Put filters in `where`, using only the field names listed above. "
    'A plain value means equality, for example {"tenant": "A"}. '
    'An object gives operators, for example {"price": {"gte": 10, "lt": 50}}. '
    'Combine conditions with "and", "or" and "not". '
    "Use the values listed above exactly as written."
)


class ToolSpec(dict):
    """A tool definition: a dict with the usual `name`, `description`, `parameters` and
    `input_schema` keys, plus `create_prompt()`."""

    def create_prompt(self, instructions: str | None = None) -> str:
        """Returns a system-prompt section that explains the tool and its fields.

        `instructions` is optional text added after the generated part, such as the role of the
        assistant or the rules for answering.
        """
        parts = [self["description"], _CALLING_RULES]
        if instructions:
            parts.append(instructions.strip())
        return "\n\n".join(parts)
