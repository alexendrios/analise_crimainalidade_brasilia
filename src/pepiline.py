from src.busca import coletar_dados_
from util.extrator_zip import arquivos_zip_execucao
from util.leitor_excel import (
    listar_arquivos_populacao,
    consolidar_historico,
    salvar_historico_csv,
)


if __name__ == "__main__":  # pragma: no cover
    # 1️⃣ Coleta dos dados (download)
    coletar_dados_()

    # 2️⃣ Extração segura dos ZIPs
    arquivos_zip_execucao()

    # 3️⃣ Leitura automática dos arquivos Excel
    arquivos = listar_arquivos_populacao("./data/planilha")

    # 4️⃣ Consolidação histórica DF
    df_historico = consolidar_historico(arquivos)

    # 5️⃣ Exportação para CSV
    salvar_historico_csv(
        df_historico,
        "./data/output/populacao_df_historico.csv",
    )
