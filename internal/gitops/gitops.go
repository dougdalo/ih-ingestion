package gitops

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Config representa as configurações para operar o GitOps.
type Config struct {
	RepoURL      string
	BaseBranch   string
	BranchPrefix string
	LocalPath    string
	UserName     string
	UserEmail    string
}

// LoadConfigFromEnv lê as configurações de GitOps das variáveis de ambiente.
// Se GIT_REPO_URL não estiver definido, retorna (nil, nil) e o GitOps fica desabilitado.
func LoadConfigFromEnv() (*Config, error) {
	repo := os.Getenv("GIT_REPO_URL")
	if strings.TrimSpace(repo) == "" {
		return nil, nil
	}

	baseBranch := os.Getenv("GIT_BASE_BRANCH")
	if strings.TrimSpace(baseBranch) == "" {
		baseBranch = "main"
	}

	prefix := os.Getenv("GIT_TARGET_BRANCH_PREFIX")
	if strings.TrimSpace(prefix) == "" {
		prefix = "ingestion-"
	}

	localPath := os.Getenv("GIT_LOCAL_PATH") // pode ser relativo ao diretório do binário
	userName := os.Getenv("GIT_USER_NAME")
	userEmail := os.Getenv("GIT_USER_EMAIL")

	return &Config{
		RepoURL:      repo,
		BaseBranch:   baseBranch,
		BranchPrefix: prefix,
		LocalPath:    localPath,
		UserName:     userName,
		UserEmail:    userEmail,
	}, nil
}

// PrepareRepo garante que o repositório local exista, esteja atualizado
// e faz checkout de uma branch de trabalho baseada em BaseBranch.
// branchSuffix é usado para compor o nome final da branch (prefix+suffix).
// Retorna (localPathResolvido, branchName, erro).
func PrepareRepo(cfg *Config, execDir, branchSuffix string) (string, string, error) {
	if cfg == nil {
		return "", "", fmt.Errorf("config GitOps é nil")
	}

	// Resolve caminho local do repositório (absoluto)
	localPath := cfg.LocalPath
	if strings.TrimSpace(localPath) == "" {
		localPath = filepath.Join(execDir, "argocd-repo")
	} else if !filepath.IsAbs(localPath) {
		localPath = filepath.Join(execDir, localPath)
	}

	// Se .git não existe, clona o repositório
	gitDir := filepath.Join(localPath, ".git")
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		if err := cloneRepo(cfg, localPath); err != nil {
			return "", "", fmt.Errorf("falha ao clonar repositório: %w", err)
		}
	} else if err == nil {
		// Já é um repositório git -> atualiza
		if err := runGit(localPath, "fetch", "--all"); err != nil {
			return "", "", fmt.Errorf("git fetch: %w", err)
		}
		if err := runGit(localPath, "checkout", cfg.BaseBranch); err != nil {
			return "", "", fmt.Errorf("git checkout %s: %w", cfg.BaseBranch, err)
		}
		if err := runGit(localPath, "pull", "--ff-only"); err != nil {
			return "", "", fmt.Errorf("git pull: %w", err)
		}
	} else {
		// Qualquer outro erro de Stat
		return "", "", fmt.Errorf("erro verificando .git em %s: %w", localPath, err)
	}

	// Configura user.name / user.email se fornecidos
	if strings.TrimSpace(cfg.UserName) != "" {
		_ = runGit(localPath, "config", "user.name", cfg.UserName)
	}
	if strings.TrimSpace(cfg.UserEmail) != "" {
		_ = runGit(localPath, "config", "user.email", cfg.UserEmail)
	}

	// Monta o nome da branch
	suffix := strings.TrimSpace(branchSuffix)
	if suffix == "" {
		suffix = time.Now().Format("20060102-150405")
	} else {
		suffix = strings.ReplaceAll(suffix, " ", "-")
	}
	branchName := cfg.BranchPrefix + suffix

	// Garante que estamos na baseBranch e cria/substitui a branch de trabalho
	if err := runGit(localPath, "checkout", cfg.BaseBranch); err != nil {
		return "", "", fmt.Errorf("git checkout %s: %w", cfg.BaseBranch, err)
	}
	if err := runGit(localPath, "checkout", "-B", branchName); err != nil {
		return "", "", fmt.Errorf("git checkout -B %s: %w", branchName, err)
	}

	return localPath, branchName, nil
}

// CommitAndPush adiciona todas as mudanças, faz commit (se houver) e dá push.
func CommitAndPush(localPath, branchName, message string) error {
	changed, err := hasPendingChanges(localPath)
	if err != nil {
		return fmt.Errorf("checando mudanças no git: %w", err)
	}
	if !changed {
		fmt.Printf("GitOps: nenhum arquivo modificado em %s, nada para commitar.\n", localPath)
		return nil
	}

	if err := runGit(localPath, "add", "."); err != nil {
		return fmt.Errorf("git add: %w", err)
	}

	if err := runGit(localPath, "commit", "-m", message); err != nil {
		return fmt.Errorf("git commit: %w", err)
	}

	if err := runGit(localPath, "push", "-u", "origin", branchName); err != nil {
		return fmt.Errorf("git push: %w", err)
	}

	return nil
}

// cloneRepo faz o git clone --branch BaseBranch RepoURL localPath.
func cloneRepo(cfg *Config, localPath string) error {
	parent := filepath.Dir(localPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("criando diretório pai %s: %w", parent, err)
	}

	// Se diretório existir e não estiver vazio, melhor falhar do que sobrescrever algo inesperado
	if fi, err := os.Stat(localPath); err == nil && fi.IsDir() {
		if entries, err := os.ReadDir(localPath); err == nil && len(entries) > 0 {
			return fmt.Errorf("diretório %s já existe e não está vazio (não é seguro clonar aqui)", localPath)
		}
	}

	cmd := exec.Command("git", "clone", "--branch", cfg.BaseBranch, cfg.RepoURL, localPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runGit executa um comando git no diretório localPath.
func runGit(localPath string, args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = localPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// hasPendingChanges verifica se há mudanças não commitadas no repositório.
func hasPendingChanges(localPath string) (bool, error) {
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = localPath

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return false, err
	}

	return strings.TrimSpace(out.String()) != "", nil
}
