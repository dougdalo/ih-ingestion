package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

	"ih-ingestion/internal/config"
)

type pageData struct {
	EnvContent           string
	EnvStatus            string
	EnvStatusClass       string
	EnvPath              string
	IngestionContent     string
	IngestionStatus      string
	IngestionStatusClass string
	IngestionPath        string
}

type server struct {
	tmpl          *template.Template
	envPath       string
	ingestionPath string
	mutex         sync.Mutex
}

func main() {
	var (
		addr          string
		envPath       string
		ingestionPath string
	)

	flag.StringVar(&addr, "addr", ":8080", "endereço para escutar (ex.: :8080)")
	flag.StringVar(&envPath, "env", ".env", "caminho do arquivo .env a ser editado")
	flag.StringVar(&ingestionPath, "config", "config/ingestion.yaml", "caminho do ingestion.yaml")
	flag.Parse()

	tmpl := template.Must(template.New("page").Parse(indexHTML))

	srv := &server{
		tmpl:          tmpl,
		envPath:       envPath,
		ingestionPath: ingestionPath,
	}

	http.HandleFunc("/", srv.handleIndex)
	http.HandleFunc("/save-env", srv.handleSaveEnv)
	http.HandleFunc("/save-ingestion", srv.handleSaveIngestion)

	log.Printf("config-ui disponível em http://localhost%s (env: %s, ingestion: %s)", addr, envPath, ingestionPath)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	envContent, envStatus, envStatusClass := s.readFileStatus(s.envPath)
	ingestionContent, ingestionStatus, ingestionStatusClass := s.readFileStatus(s.ingestionPath)

	data := pageData{
		EnvContent:           envContent,
		EnvStatus:            envStatus,
		EnvStatusClass:       envStatusClass,
		EnvPath:              s.envPath,
		IngestionContent:     ingestionContent,
		IngestionStatus:      ingestionStatus,
		IngestionStatusClass: ingestionStatusClass,
		IngestionPath:        s.ingestionPath,
	}

	if err := s.tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleSaveEnv(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "form inválido", http.StatusBadRequest)
		return
	}

	content := r.FormValue("env_content")

	if err := s.writeFile(s.envPath, []byte(content)); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		s.renderEnvCard(w, pageData{
			EnvStatus:      fmt.Sprintf("erro ao salvar .env: %v", err),
			EnvStatusClass: "status err",
			EnvContent:     content,
		}, true)
		return
	}

	envData := pageData{
		EnvPath:        s.envPath,
		EnvStatus:      fmt.Sprintf(".env salvo em %s", s.envPath),
		EnvStatusClass: "status ok",
	}

	s.renderEnvCard(w, envData, false)
}

func (s *server) handleSaveIngestion(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "form inválido", http.StatusBadRequest)
		return
	}

	content := r.FormValue("ingestion_content")
	if strings.TrimSpace(content) == "" {
		w.WriteHeader(http.StatusBadRequest)
		s.renderIngestionCard(w, pageData{
			IngestionStatus:      "ingestion.yaml não pode ficar vazio",
			IngestionStatusClass: "status err",
			IngestionContent:     content,
		}, true)
		return
	}

	var cfg config.IngestionConfig
	if err := yaml.Unmarshal([]byte(content), &cfg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.renderIngestionCard(w, pageData{
			IngestionStatus:      fmt.Sprintf("ingestion.yaml inválido: %v", err),
			IngestionStatusClass: "status err",
			IngestionContent:     content,
		}, true)
		return
	}

	if err := config.ValidateIngestionConfig(&cfg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.renderIngestionCard(w, pageData{
			IngestionStatus:      fmt.Sprintf("ingestion.yaml inválido: %v", err),
			IngestionStatusClass: "status err",
			IngestionContent:     content,
		}, true)
		return
	}

	if err := s.writeFile(s.ingestionPath, []byte(content)); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		s.renderIngestionCard(w, pageData{
			IngestionStatus:      fmt.Sprintf("erro ao salvar ingestion.yaml: %v", err),
			IngestionStatusClass: "status err",
			IngestionContent:     content,
		}, true)
		return
	}

	ingestionData := pageData{
		IngestionPath:        s.ingestionPath,
		IngestionStatus:      fmt.Sprintf("ingestion.yaml salvo em %s (%d sqlservers)", s.ingestionPath, len(cfg.SqlServers)),
		IngestionStatusClass: "status ok",
	}

	s.renderIngestionCard(w, ingestionData, false)
}

func (s *server) renderEnvCard(w http.ResponseWriter, data pageData, useProvidedContent bool) {
	content, status, statusClass := s.readFileStatus(s.envPath)

	if data.EnvStatus == "" {
		data.EnvStatus = status
		data.EnvStatusClass = statusClass
	}

	if !useProvidedContent {
		data.EnvContent = content
	}
	data.EnvPath = s.envPath

	if err := s.tmpl.ExecuteTemplate(w, "envCard", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) renderIngestionCard(w http.ResponseWriter, data pageData, useProvidedContent bool) {
	content, status, statusClass := s.readFileStatus(s.ingestionPath)

	if data.IngestionStatus == "" {
		data.IngestionStatus = status
		data.IngestionStatusClass = statusClass
	}

	if !useProvidedContent {
		data.IngestionContent = content
	}
	data.IngestionPath = s.ingestionPath

	if err := s.tmpl.ExecuteTemplate(w, "ingestionCard", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) readFileStatus(path string) (string, string, string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Sprintf("arquivo ainda não existe; será criado em %s", path), "status"
	}
	return string(data), fmt.Sprintf("lendo %s", path), "status"
}

func (s *server) writeFile(path string, data []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644)
}

const indexHTML = `{{define "page"}}
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Config UI</title>
  <script src="https://unpkg.com/htmx.org@1.9.12" integrity="sha384-+oqoEcJ7+9P+Dg8M0Zy07lzeppoea4T1aI6+RaeMn7nSMeKMKCXmqJazM3QCwFS9" crossorigin="anonymous"></script>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; margin: 32px; background: #0f172a; color: #e2e8f0; }
    h1 { margin-bottom: 0; }
    p.small { color: #94a3b8; margin-top: 4px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 24px; margin-top: 24px; }
    .card { background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.3); }
    textarea { width: 100%; min-height: 360px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; background: #0b1224; color: #e2e8f0; border: 1px solid #1f2937; border-radius: 8px; padding: 12px; box-sizing: border-box; }
    button { background: linear-gradient(90deg, #22d3ee, #3b82f6); color: #0b1224; border: none; border-radius: 8px; padding: 10px 16px; font-weight: 700; cursor: pointer; }
    button:hover { filter: brightness(1.05); }
    .status { margin-top: 10px; padding: 8px 10px; border-radius: 8px; font-size: 14px; display: inline-block; }
    .status.ok { background: rgba(34, 197, 94, 0.15); border: 1px solid rgba(34, 197, 94, 0.5); color: #bbf7d0; }
    .status.err { background: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.5); color: #fecdd3; }
    code { background: #0b1224; padding: 3px 5px; border-radius: 6px; }
  </style>
</head>
<body>
  <h1>Config UI</h1>
  <p class="small">Edite os arquivos de configuração sem sair do navegador. Os caminhos atuais são <code>{{.EnvPath}}</code> e <code>{{.IngestionPath}}</code>.</p>

  <div class="grid">
    {{template "envCard" .}}
    {{template "ingestionCard" .}}
  </div>
</body>
</html>
{{end}}

{{define "envCard"}}
  <div class="card">
    <h2>.env</h2>
    <p class="small">Conteúdo salvo diretamente em <code>{{.EnvPath}}</code>.</p>
    <form hx-post="/save-env" hx-target="closest .card" hx-swap="outerHTML">
      <textarea id="env-content" name="env_content" spellcheck="false">{{.EnvContent}}</textarea>
      <div style="display:flex; justify-content: space-between; align-items: center; margin-top: 12px;">
        <span id="env-status" class="{{.EnvStatusClass}}">{{.EnvStatus}}</span>
        <button type="submit">Salvar .env</button>
      </div>
    </form>
  </div>
{{end}}

{{define "ingestionCard"}}
  <div class="card">
    <h2>ingestion.yaml</h2>
    <p class="small">Valida a estrutura antes de salvar em <code>{{.IngestionPath}}</code>.</p>
    <form hx-post="/save-ingestion" hx-target="closest .card" hx-swap="outerHTML">
      <textarea id="ingestion-content" name="ingestion_content" spellcheck="false">{{.IngestionContent}}</textarea>
      <div style="display:flex; justify-content: space-between; align-items: center; margin-top: 12px;">
        <span id="ingestion-status" class="{{.IngestionStatusClass}}">{{.IngestionStatus}}</span>
        <button type="submit">Salvar ingestion.yaml</button>
      </div>
    </form>
  </div>
{{end}}
`
