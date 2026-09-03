.PHONY: infra setup pipeline api dashboard all

# 1. Subir infraestrutura (postgres, pgadmin, ollama)
infra:
	docker compose up -d postgres pgadmin ollama

# 2. Buildar as imagens dos serviços de aplicação (pipeline, api, dashboard)
build:
	docker compose build

# 4. Pipeline completo (coleta + gold + modelagem)
pipeline:
	docker compose up --build pipeline

# 7. API (documentação em http://localhost:8000/docs)
api:
	docker compose up --build -d api

# 8. Dashboard
dashboard:
	docker compose up --build -d dashboard

# Subir tudo de uma vez
all:
	docker compose up -d postgres pgadmin ollama
	docker compose up --build pipeline
	docker compose up --build -d api dashboard

# Ambiente virtual e dependências (opcional, para rodar local)
setup:
	python -m venv venv
	@echo "Ative o venv: source venv/bin/activate (Linux/macOS) ou venv\\Scripts\\activate (Windows)"
	@pip install -r requirements.txt
