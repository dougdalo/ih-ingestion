package kustomize

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Kustomization struct {
	APIVersion string   `yaml:"apiVersion,omitempty"`
	Kind       string   `yaml:"kind,omitempty"`
	Namespace  string   `yaml:"namespace,omitempty"`
	Resources  []string `yaml:"resources,omitempty"`
}

// UpdateKustomization garante que o kustomization.yaml da pasta `dir` exista
// e contenha todos os arquivos informados em `newFiles` (apenas nomes de arquivo, não caminhos absolutos).
// namespace: se != "" e o arquivo ainda não tiver namespace, ele seta.
// se namespace == "", não mexe no campo Namespace existente.
func UpdateKustomization(dir string, newFiles []string, namespace string) error {
	if len(newFiles) == 0 {
		return nil
	}

	// dedupe dos novos arquivos
	uniqNew := make([]string, 0, len(newFiles))
	seen := map[string]struct{}{}

	for _, f := range newFiles {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		uniqNew = append(uniqNew, f)
	}
	if len(uniqNew) == 0 {
		return nil
	}

	kpath := filepath.Join(dir, "kustomization.yaml")
	var k Kustomization

	data, err := os.ReadFile(kpath)
	if err == nil {
		if err := yaml.Unmarshal(data, &k); err != nil {
			return fmt.Errorf("falha ao parsear %s: %w", kpath, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("erro lendo %s: %w", kpath, err)
	}

	if k.APIVersion == "" {
		k.APIVersion = "kustomize.config.k8s.io/v1beta1"
	}
	if k.Kind == "" {
		k.Kind = "Kustomization"
	}
	if namespace != "" && k.Namespace == "" {
		k.Namespace = namespace
	}

	existing := map[string]struct{}{}
	for _, r := range k.Resources {
		existing[strings.TrimSpace(r)] = struct{}{}
	}

	for _, f := range uniqNew {
		if _, ok := existing[f]; !ok {
			k.Resources = append(k.Resources, f)
		}
	}

	out, err := yaml.Marshal(&k)
	if err != nil {
		return fmt.Errorf("falha ao serializar kustomization: %w", err)
	}

	if err := os.WriteFile(kpath, out, 0o644); err != nil {
		return fmt.Errorf("falha ao escrever %s: %w", kpath, err)
	}

	return nil
}
