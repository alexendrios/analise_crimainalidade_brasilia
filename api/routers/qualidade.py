# api/routers/qualidade.py
from fastapi import APIRouter, HTTPException

from api.schemas import QualidadeResponse
from api.services import qualidade_service

router = APIRouter(prefix="/qualidade", tags=["Qualidade dos Dados"])


@router.get(
    "/dados",
    response_model=QualidadeResponse,
    summary="Data Quality Score (0-100): nota geral e por tabela gold, com dimensões",
)
def dados_qualidade():
    try:
        return qualidade_service.obter_qualidade_dados()
    except qualidade_service.DadosIndisponiveisError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc