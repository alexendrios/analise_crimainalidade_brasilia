# api/routers/previsao.py
from fastapi import APIRouter, HTTPException, Query

from api.schemas import ModelosTreinadosResponse, PrevisaoResponse
from api.services import forecast_service

router = APIRouter(prefix="/previsao", tags=["Previsão"])


@router.get(
    "/crimes-contra-mulher",
    response_model=PrevisaoResponse,
    summary="Previsão de crimes contra a mulher (Prophet + XGBoost)",
)
def previsao_crimes_contra_mulher(
    horizonte_anos: int = Query(5, ge=1, le=10, description="Quantos anos à frente prever"),
    usar_cache: bool = Query(True, description="Reaproveita previsão recente em cache (30 min)"),
    persistir_modelo: bool = Query(
        False, description="Se true, salva o modelo re-treinado em models/ (como o pipeline batch)"
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


@router.get(
    "/modelos",
    response_model=ModelosTreinadosResponse,
    summary="Lista os modelos já treinados e persistidos em models/",
)
def modelos_treinados():
    return forecast_service.listar_modelos_treinados()
