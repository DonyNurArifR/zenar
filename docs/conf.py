"""Sphinx configuration for the Zenar documentation site."""

project = "Zenar"
copyright = "2026, Zenar contributors"
author = "Zenar contributors"

extensions = ["myst_parser"]
source_suffix = {".md": "markdown"}
exclude_patterns = ["_build", "requirements.txt"]

myst_enable_extensions = ["colon_fence"]
# Heading anchors, so links like STORAGE.md#crash-tests resolve.
myst_heading_anchors = 3

html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "github_url": "https://github.com/DonyNurArifR/zenar",
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "show_prev_next": True,
}
