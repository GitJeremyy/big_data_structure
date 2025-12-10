1. Download the extension : LaTeX Workshop
2. Type in the search bar at the top : >Preferences: Open User Settings(JSON)
3. Put this in it to have all of the build files in a build folder (that's in a git ignore) and the built pdf in your root:
```json
{
  "github.copilot.nextEditSuggestions.enabled": true,
  "editor.unicodeHighlight.ambiguousCharacters": false,
  "workbench.editorAssociations": {
    "*.copilotmd": "vscode.markdown.preview.editor",
    "*.pdf": "latex-workshop-pdf-hook"
  },
  "latex-workshop.latex.outDir": "./build",
  "latex-workshop.latex.tools": [
    {
      "name": "latexmk",
      "command": "latexmk",
      "args": [
        "-pdf",
        "-interaction=nonstopmode",
        "-synctex=1",
        "-outdir=build",
        "%DOC%"
      ]
    },
    {
      "name": "move-pdf",
      "command": "cmd",
      "args": [
        "/c",
        "copy build\\%DOCFILE%.pdf ."
      ]
    }
  ],
  "latex-workshop.latex.recipes": [
    {
      "name": "latexmk -> move-pdf",
      "tools": ["latexmk", "move-pdf"]
    }
  ],
  "latex-workshop.latex.clean.fileTypes": [
    "*.aux",
    "*.bbl",
    "*.blg",
    "*.fls",
    "*.log",
    "*.fdb_latexmk",
    "*.synctex.gz"
  ]
}
```