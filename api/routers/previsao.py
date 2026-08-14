# api/routers/previsao.py
from fastapi import APIRouter, HTTPException, Query

from api.schemas import ModelosTreinadosResponse, PrevisaoResponse
from api.services import forecast_service

router = APIRouter(prefix="/previsao", tags=["Previsão"])


@router.get(
    "/crimes-contra-mulher",
    response_model=PrevisaoResponse,
    summary="Previsão de crimes contra a mulher (Prophet + XGBoost)",
    description=(
        "Por padrão, serve a previsão a partir do bundle Prophet+XGBoost mais "
        "recente já persistido em `models/` (`fonte_modelo='artefato'`, resposta "
        "rápida). Se ainda não existir nenhum artefato nesse formato, treina o "
        "par sob demanda (`fonte_modelo='retreino'`). Para forçar um novo "
        "treino mesmo com um artefato disponível, use `POST /previsao/retrain`."
    ),
)
def previsao_crimes_contra_mulher(
    horizonte_anos: int = Query(5, ge=1, le=10, description="Quantos anos à frente prever"),
    usar_cache: bool = Query(True, description="Reaproveita previsão recente em cache (30 min)"),
    persistir_modelo: bool = Query(
        False,
        description=(
            "Só tem efeito se, nesta chamada, não houver artefato disponível e o "
            "par Prophet+XGBoost precisar ser treinado: se true, o resultado do "
            "treino é salvo em models/ (bundle), para que as próximas chamadas já "
            "sirvam a partir dele. Para forçar um retreino mesmo com artefato "
            "existente, use POST /previsao/retrain."
        ),
    ),
):
    try:
        return forecast_service.gerar_previsao(
            horizonte_anos=horizonte_anos,
            usar_cache=usar_cache,
            persistir_modelo=persistir_modelo,
        )
    except forecast_service.DadosInsuficientesError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - erro inesperado de treinamento/infra
        raise HTTPException(
            status_code=500, detail=f"Falha ao gerar previsão: {exc}"
        ) from exc


@router.post(
    "/retrain",
    response_model=PrevisaoResponse,
    summary="Força o re-treino do par Prophet+XGBoost e persiste o novo bundle em models/",
    description=(
        "Ignora qualquer artefato existente, treina um novo par Prophet+XGBoost "
        "a partir da tabela gold mais recente, salva o bundle em `models/` "
        "(`fonte_modelo='retreino'`) e retorna a previsão resultante. As "
        "próximas chamadas a GET /previsao/crimes-contra-mulher passam a servir "
        "a partir deste novo artefato."
    ),
)
def retreinar_previsao(
    horizonte_anos: int = Query(5, ge=1, le=10, description="Quantos anos à frente prever"),
):
    try:
        return forecast_service.gerar_previsao(
            horizonte_anos=horizonte_anos,
            usar_cache=False,
            forcar_retreino=True,
            persistir_modelo=True,
        )
    except forecast_service.DadosInsuficientesError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - erro inesperado de treinamento/infra
        raise HTTPException(
            status_code=500, detail=f"Falha ao re-treinar modelo: {exc}"
        ) from exc


@router.get(
    "/modelos",
    response_model=ModelosTreinadosResponse,
    summary="Lista os modelos já treinados e persistidos em models/",
)
def modelos_treinados():
    return forecast_service.listar_modelos_treinados()
