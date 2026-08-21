# api/routers/classificacao.py
from fastapi import APIRouter, HTTPException, Query

from api.schemas import ClassificacaoResponse
from api.services import classificacao_service

router = APIRouter(prefix="/classificacao", tags=["Classificação"])


@router.get(
    "/criminalidade-letal",
    response_model=ClassificacaoResponse,
    summary="Classifica cada RA/ano como alta ou baixa criminalidade letal (Regressão Logística)",
    description=(
        "Por padrão, serve a classificação a partir do pipeline de Regressão "
        "Logística mais recente já persistido em `models/` "
        "(`fonte_modelo='artefato'`, resposta rápida). Se ainda não existir "
        "nenhum artefato nesse formato, treina o modelo sob demanda "
        "(`fonte_modelo='retreino'`). Para forçar um novo treino mesmo com um "
        "artefato disponível, use `POST /classificacao/retrain`."
    ),
)
def classificar_criminalidade_letal(
    usar_cache: bool = Query(True, description="Reaproveita classificação recente em cache (30 min)"),
):
    try:
        return classificacao_service.classificar_criminalidade(usar_cache=usar_cache)
    except classificacao_service.DadosInsuficientesError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - erro inesperado de treinamento/infra
        raise HTTPException(
            status_code=500, detail=f"Falha ao gerar classificação: {exc}"
        ) from exc


@router.post(
    "/retrain",
    response_model=ClassificacaoResponse,
    summary="Força o re-treino da Regressão Logística e persiste o novo pipeline em models/",
    description=(
        "Ignora qualquer artefato existente, treina uma nova Regressão "
        "Logística a partir das tabelas gold mais recentes, salva o pipeline "
        "em `models/` (`fonte_modelo='retreino'`) e retorna a classificação "
        "resultante. As próximas chamadas a GET /classificacao/"
        "criminalidade-letal passam a servir a partir deste novo artefato."
    ),
)
def retreinar_classificacao():
    try:
        return classificacao_service.classificar_criminalidade(
            usar_cache=False,
            forcar_retreino=True,
            persistir_modelo=True,
        )
    except classificacao_service.DadosInsuficientesError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - erro inesperado de treinamento/infra
        raise HTTPException(
            status_code=500, detail=f"Falha ao re-treinar modelo: {exc}"
        ) from exc
