package main

import "embed"

//go:embed tmpl/*.html
var tmplFS embed.FS
