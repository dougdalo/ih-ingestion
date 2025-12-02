package generator

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"text/template"
)

func RenderToFile(t *template.Template, data any, path string) error {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return err
	}

	log.Printf("arquivo gerado: %s", path)
	return nil
}
