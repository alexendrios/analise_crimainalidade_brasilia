"""
conftest.py

Configuração global da suíte de testes do projeto
SSPDF Análise Predição.

Recursos:
    - pytest
    - pytest-html
    - pytest-cov
    - requests
    - pandas
    - tqdm

Objetivos:
    - Configurar o relatório HTML;
    - Criar Summary executivo;
    - Apresentar indicadores reais da execução;
    - Disponibilizar fixtures reutilizáveis;
    - Facilitar testes HTTP;
    - Facilitar testes CSV;
    - Facilitar testes com pandas;
    - Facilitar testes com tqdm;
    - Facilitar testes de logging;
    - Manter a cobertura sob responsabilidade do pytest-cov.

Compatibilidade:
    Python 3.13+
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, mock_open, patch
import time

import pandas as pd
import pytest
import requests


# ============================================================
# IDENTIDADE DO PROJETO
# ============================================================

PROJECT_NAME = "Criminalidade Brasília - DF"

REPORT_TITLE = "Relatório de Qualidade e Testes Unitários Automatizados"

REPORT_DESCRIPTION = (
    "Relatório consolidado da execução automatizada da suíte de "
    "testes unitários, validação dos componentes da aplicação e "
    "análise da cobertura de código."
)

PYTHON_VERSION = "Python 3.13"

REPORT_TOOL = "pytest-html"

COVERAGE_TOOL = "pytest-cov"

COVERAGE_MINIMUM = 95.0


# ============================================================
# ESTATÍSTICAS DA EXECUÇÃO
# ============================================================
#
# Coletadas via terminalreporter.stats (compatível com xdist)
# e via pytest_runtest_makereport para o caso sem xdist.
# ============================================================

_EXECUTION_STATS = {
    "passed": 0,
    "failed": 0,
    "skipped": 0,
    "errors": 0,
    "xfailed": 0,
    "xpassed": 0,
    "total": 0,
    "duration": 0.0,
    "status": "NÃO EXECUTADO",
}


# ============================================================
# CONFIGURAÇÃO DO PYTEST
# ============================================================


def pytest_configure(config):
    """
    Configura os metadados exibidos pelo pytest-html.
    """

    metadata = getattr(
        config,
        "_metadata",
        None,
    )

    if metadata is None:
        return

    metadata["Projeto"] = PROJECT_NAME
    metadata["Relatório"] = REPORT_TITLE
    metadata["Descrição"] = REPORT_DESCRIPTION
    metadata["Python"] = PYTHON_VERSION
    metadata["Framework"] = "pytest"
    metadata["Relatório HTML"] = REPORT_TOOL
    metadata["Cobertura"] = COVERAGE_TOOL
    metadata["Meta de cobertura"] = f"{COVERAGE_MINIMUM:.0f}%"
    metadata["Data/Hora"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")


# ============================================================
# INÍCIO DA EXECUÇÃO
# ============================================================

_SESSION_REF = None


def pytest_sessionstart(session):
    """
    Registra o início da execução para cálculo da duração
    e armazena referência da session para uso nos hooks do HTML.
    """

    global _SESSION_REF

    _SESSION_REF = session

    session.config._sspdf_start_time = time.perf_counter()


# ============================================================
# CAPTURA DOS RESULTADOS DOS TESTES
# ============================================================


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Captura o resultado individual dos testes.

    Compatível com xdist:
        - Em modo single-process: popula _EXECUTION_STATS
          diretamente, garantindo dados antes do HTML summary;
        - Em modo xdist: worker popula sua cópia local;
          o controller usa terminalreporter.stats como fallback.
    """

    outcome = yield

    report = outcome.get_result()

    # --------------------------------------------------------
    # Consideramos apenas a fase principal do teste.
    # --------------------------------------------------------

    if report.when != "call":
        return

    was_xfail = getattr(
        report,
        "wasxfail",
        False,
    )

    # --------------------------------------------------------
    # APROVADO / XPASS
    # --------------------------------------------------------

    if report.passed:
        if was_xfail:
            _EXECUTION_STATS["xpassed"] += 1
        else:
            _EXECUTION_STATS["passed"] += 1

    # --------------------------------------------------------
    # FALHA / XFAIL
    # --------------------------------------------------------

    elif report.failed:
        if was_xfail:
            _EXECUTION_STATS["xfailed"] += 1
        else:
            _EXECUTION_STATS["failed"] += 1

    # --------------------------------------------------------
    # SKIPPED
    # --------------------------------------------------------

    elif report.skipped:
        _EXECUTION_STATS["skipped"] += 1


# ============================================================
# FINAL DA EXECUÇÃO
# ============================================================


def pytest_sessionfinish(session, exitstatus):
    """
    Finaliza a coleta dos indicadores da execução.

    Compatibilidade com pytest-xdist:
        - Em modo single-process: lê de _EXECUTION_STATS;
        - Em modo xdist: lê de terminalreporter.stats
          que é consolidado pelo controller.
    """

    global _EXECUTION_STATS

    tr = session.config.pluginmanager.get_plugin(
        "terminalreporter",
    )

    if tr is not None and getattr(tr, "stats", {}):
        stats = tr.stats

        passed = len(stats.get("passed", []))
        failed = len(stats.get("failed", []))
        skipped = len(stats.get("skipped", []))
        xfailed = len(stats.get("xfailed", []))
        xpassed = len(stats.get("xpassed", []))

    else:
        passed = _EXECUTION_STATS["passed"]
        failed = _EXECUTION_STATS["failed"]
        skipped = _EXECUTION_STATS["skipped"]
        xfailed = _EXECUTION_STATS["xfailed"]
        xpassed = _EXECUTION_STATS["xpassed"]

    # --------------------------------------------------------
    # TOTAL
    # --------------------------------------------------------

    total = passed + failed + skipped + xfailed + xpassed

    # --------------------------------------------------------
    # DURAÇÃO
    # --------------------------------------------------------

    start_time = getattr(
        session.config,
        "_sspdf_start_time",
        None,
    )

    if start_time is not None:
        duration = time.perf_counter() - start_time

    else:
        duration = 0.0

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if exitstatus == pytest.ExitCode.OK:
        status = "APROVADO"

    elif exitstatus == pytest.ExitCode.TESTS_FAILED:
        status = "ATENÇÃO"

    else:
        status = "ERRO DE EXECUÇÃO"

    # --------------------------------------------------------
    # ATUALIZA ESTATÍSTICAS
    # --------------------------------------------------------

    _EXECUTION_STATS.update(
        {
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "errors": 0,
            "xfailed": xfailed,
            "xpassed": xpassed,
            "total": total,
            "duration": duration,
            "status": status,
        }
    )


# ============================================================
# TÍTULO DO RELATÓRIO HTML
# ============================================================


def pytest_html_report_title(report):
    """
    Define o título exibido pelo pytest-html.
    """

    report.title = f"{PROJECT_NAME} - {REPORT_TITLE}"


# ============================================================
# SUMMARY EXECUTIVO
# ============================================================


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_summary(
    prefix,
    summary,
    postfix,
):
    """
    Cria um Summary executivo e profissional
    para o relatório HTML.

    Compatibilidade com pytest-xdist:
        - Em modo xdist, _EXECUTION_STATS pode estar zerado
          pois pytest_sessionfinish ainda não rodou;
        - Usamos terminalreporter.stats via _SESSION_REF como
          fonte primária (dados consolidados dos workers);
        - Fallback para _EXECUTION_STATS (modo single-process
          sem xdist, onde pytest_runtest_makereport populou).
    """

    # --------------------------------------------------------
    # Fonte primária: terminalreporter.stats
    # (funciona com e sem xdist — dados já disponíveis)
    # --------------------------------------------------------

    tr = None

    if _SESSION_REF is not None:
        tr = _SESSION_REF.config.pluginmanager.get_plugin(
            "terminalreporter",
        )

    if tr is not None and getattr(tr, "stats", {}):
        tr_stats = tr.stats

        passed = len(tr_stats.get("passed", []))
        failed = len(tr_stats.get("failed", []))
        skipped = len(tr_stats.get("skipped", []))
        xfailed = len(tr_stats.get("xfailed", []))
        xpassed = len(tr_stats.get("xpassed", []))
        errors = 0

    else:
        passed = _EXECUTION_STATS["passed"]
        failed = _EXECUTION_STATS["failed"]
        skipped = _EXECUTION_STATS["skipped"]
        errors = _EXECUTION_STATS["errors"]
        xfailed = _EXECUTION_STATS["xfailed"]
        xpassed = _EXECUTION_STATS["xpassed"]

    total = passed + failed + skipped + xfailed + xpassed

    # --------------------------------------------------------
    # DURAÇÃO
    # --------------------------------------------------------

    if _SESSION_REF is not None:
        start_time = getattr(
            _SESSION_REF.config,
            "_sspdf_start_time",
            None,
        )

        if start_time is not None:
            duration = time.perf_counter() - start_time

        else:
            duration = 0.0

    else:
        duration = _EXECUTION_STATS["duration"]

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if total > 0 and failed == 0:
        status = "APROVADO"

    elif total > 0:
        status = "ATENÇÃO"

    else:
        status = "NÃO EXECUTADO"

    # --------------------------------------------------------
    # TAXA DE APROVAÇÃO
    # --------------------------------------------------------

    if total > 0:
        approval_rate = (passed / total) * 100
    else:
        approval_rate = 0.0

    # --------------------------------------------------------
    # TEXTO DA DURAÇÃO
    # --------------------------------------------------------

    if duration >= 60:
        minutes = int(duration // 60)

        seconds = duration - (minutes * 60)

        duration_text = f"{minutes} min {seconds:.2f} s"

    else:
        duration_text = f"{duration:.2f} segundos"

    # --------------------------------------------------------
    # STATUS HTML
    # --------------------------------------------------------

    if status == "APROVADO":
        status_html = """
        <div class="sspdf-status sspdf-success">
            ✓ SUÍTE DE TESTES APROVADA
        </div>
        """

    elif status == "ATENÇÃO":
        status_html = """
        <div class="sspdf-status sspdf-warning">
            ⚠ ATENÇÃO: EXISTEM TESTES COM PROBLEMAS
        </div>
        """

    else:
        status_html = """
        <div class="sspdf-status sspdf-error">
            ✕ ERRO DURANTE A EXECUÇÃO
        </div>
        """

    # ========================================================
    # CSS
    # ========================================================

    css = """
    <style>

        .sspdf-header {
            padding: 26px;
            margin: 10px 0 25px 0;
            border-radius: 10px;
            background: #f8fafc;
            border-left: 6px solid #1e3a5f;
        }

        .sspdf-header h2 {
            margin: 0 0 8px 0;
            font-size: 28px;
            color: #1e3a5f;
        }

        .sspdf-header p {
            margin: 6px 0;
            color: #475569;
            line-height: 1.5;
        }

        .sspdf-subtitle {
            font-size: 18px;
            font-weight: 600;
            color: #334155;
        }

        .sspdf-status {
            padding: 16px 20px;
            margin: 20px 0;
            border-radius: 8px;
            font-size: 19px;
            font-weight: bold;
            text-align: center;
        }

        .sspdf-success {
            background: #ecfdf5;
            color: #166534;
            border: 1px solid #86efac;
        }

        .sspdf-warning {
            background: #fffbeb;
            color: #92400e;
            border: 1px solid #fcd34d;
        }

        .sspdf-error {
            background: #fef2f2;
            color: #991b1b;
            border: 1px solid #fca5a5;
        }

        .sspdf-section {
            margin-top: 28px;
        }

        .sspdf-section h3 {
            color: #1e3a5f;
            border-bottom: 2px solid #e2e8f0;
            padding-bottom: 8px;
        }

        .sspdf-metrics {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(145px, 1fr));
            gap: 12px;
            margin: 15px 0;
        }

        .sspdf-card {
            padding: 18px;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #ffffff;
            text-align: center;
        }

        .sspdf-card-value {
            font-size: 29px;
            font-weight: bold;
            color: #1e3a5f;
        }

        .sspdf-card-label {
            margin-top: 6px;
            color: #64748b;
            font-size: 13px;
        }

        .sspdf-special {
            width: 100%;
            border-collapse: collapse;
        }

        .sspdf-special td {
            padding: 10px 12px;
            border-bottom: 1px solid #e2e8f0;
        }

        .sspdf-special td:first-child {
            font-weight: bold;
            width: 65%;
            color: #334155;
        }

        .sspdf-info {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }

        .sspdf-info td {
            padding: 10px 12px;
            border-bottom: 1px solid #e2e8f0;
        }

        .sspdf-info td:first-child {
            font-weight: bold;
            width: 35%;
            color: #334155;
        }

        .sspdf-highlight {
            padding: 18px;
            margin-top: 15px;
            background: #f8fafc;
            border-left: 4px solid #1e3a5f;
            border-radius: 5px;
            line-height: 1.6;
            color: #475569;
        }

        .sspdf-footer {
            margin-top: 30px;
            padding: 15px;
            background: #f8fafc;
            border-radius: 6px;
            color: #64748b;
            font-size: 12px;
            text-align: center;
        }

    </style>
    """

    # ========================================================
    # CABEÇALHO
    # ========================================================

    header = f"""
    <div class="sspdf-header">

        <h2>{PROJECT_NAME}</h2>

        <p class="sspdf-subtitle">
            {REPORT_TITLE}
        </p>

        <p>
            {REPORT_DESCRIPTION}
        </p>

    </div>
    """

    # ========================================================
    # INDICADORES
    # ========================================================

    metrics = f"""
    <div class="sspdf-section">

        <h3>Indicadores da Execução</h3>

        <div class="sspdf-metrics">

            <div class="sspdf-card">
                <div class="sspdf-card-value">
                    {total}
                </div>
                <div class="sspdf-card-label">
                    Testes Executados
                </div>
            </div>

            <div class="sspdf-card">
                <div class="sspdf-card-value">
                    {passed}
                </div>
                <div class="sspdf-card-label">
                    Aprovados
                </div>
            </div>

            <div class="sspdf-card">
                <div class="sspdf-card-value">
                    {failed}
                </div>
                <div class="sspdf-card-label">
                    Falhas
                </div>
            </div>

            <div class="sspdf-card">
                <div class="sspdf-card-value">
                    {skipped}
                </div>
                <div class="sspdf-card-label">
                    Ignorados
                </div>
            </div>

            <div class="sspdf-card">
                <div class="sspdf-card-value">
                    {errors}
                </div>
                <div class="sspdf-card-label">
                    Erros
                </div>
            </div>

        </div>

        <div class="sspdf-highlight">

            <strong>Taxa de aprovação:</strong>
            {approval_rate:.2f}%

            <br>

            <strong>Tempo total:</strong>
            {duration_text}

        </div>

    </div>
    """

    # ========================================================
    # RESULTADOS ESPECIAIS
    # ========================================================

    special_results = f"""
    <div class="sspdf-section">

        <h3>Resultados Especiais</h3>

        <table class="sspdf-special">

            <tr>
                <td>Expected Failures (XFAIL)</td>
                <td>{xfailed}</td>
            </tr>

            <tr>
                <td>Unexpected Passes (XPASS)</td>
                <td>{xpassed}</td>
            </tr>

        </table>

    </div>
    """

    # ========================================================
    # AMBIENTE
    # ========================================================

    environment = f"""
    <div class="sspdf-section">

        <h3>Ambiente de Testes</h3>

        <table class="sspdf-info">

            <tr>
                <td>Projeto</td>
                <td>{PROJECT_NAME}</td>
            </tr>

            <tr>
                <td>Python</td>
                <td>{PYTHON_VERSION}</td>
            </tr>

            <tr>
                <td>Framework</td>
                <td>pytest</td>
            </tr>

            <tr>
                <td>Relatório</td>
                <td>{REPORT_TOOL}</td>
            </tr>

            <tr>
                <td>Cobertura</td>
                <td>{COVERAGE_TOOL}</td>
            </tr>

            <tr>
                <td>Meta de cobertura</td>
                <td>
                    {COVERAGE_MINIMUM:.0f}%
                </td>
            </tr>

            <tr>
                <td>Data/Hora</td>
                <td>
                    {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
                </td>
            </tr>

        </table>

    </div>
    """

    # ========================================================
    # OBJETIVO
    # ========================================================

    objective = """
    <div class="sspdf-section">

        <h3>Objetivo da Suíte</h3>

        <div class="sspdf-highlight">

            Garantir a confiabilidade dos componentes
            da aplicação por meio de testes automatizados,
            verificando comportamentos esperados,
            tratamento de erros, processamento de dados,
            integração entre componentes e geração dos
            resultados da aplicação.

        </div>

    </div>
    """

    # ========================================================
    # RODAPÉ
    # ========================================================

    footer = """
    <div class="sspdf-footer">

        Relatório gerado automaticamente pelo pipeline
        de testes do projeto
        <strong>Criminalidade Brasília - DF</strong>.

    </div>
    """

    # ========================================================
    # INSERE NO PYTEST-HTML
    # ========================================================

    prefix.extend(
        [
            css,
            header,
            status_html,
            metrics,
            special_results,
            environment,
            objective,
            footer,
        ]
    )


# ============================================================
# FIXTURES - REQUESTS
# ============================================================


@pytest.fixture
def mock_requests_get():
    """
    Mock para requests.get.
    """

    with patch("requests.get") as mock:
        yield mock


@pytest.fixture
def mock_requests_post():
    """
    Mock para requests.post.
    """

    with patch("requests.post") as mock:
        yield mock


@pytest.fixture
def mock_requests_session():
    """
    Mock para requests.Session.
    """

    with patch("requests.Session") as mock:
        yield mock


@pytest.fixture
def mock_session_factory():
    """
    Factory para simular requests.Session()
    como context manager.

    Compatível com:

        with requests.Session() as session:
            response = session.get(url)
    """

    def _factory(
        response=None,
        get_side_effect=None,
    ):

        mock_session = MagicMock()

        if get_side_effect is not None:
            mock_session.get.side_effect = get_side_effect

        else:
            mock_session.get.return_value = response

        mock_session_cls = MagicMock()

        (mock_session_cls.return_value.__enter__.return_value) = mock_session

        (mock_session_cls.return_value.__exit__.return_value) = False

        return mock_session_cls

    return _factory


# ============================================================
# FIXTURES - RESPONSE HTTP
# ============================================================


@pytest.fixture
def mock_response():
    """
    Factory para criação de respostas HTTP simuladas.
    """

    def _mock(
        html="",
        status_code=200,
    ):

        response = Mock(spec=requests.Response)

        response.status_code = status_code

        response.text = html

        response.content = html.encode("utf-8")

        response.raise_for_status.return_value = None

        return response

    return _mock


@pytest.fixture
def mock_response_ok(mock_response):
    """
    Resposta HTTP 200.
    """

    return mock_response(
        html="<html>OK</html>",
        status_code=200,
    )


@pytest.fixture
def mock_response_created(mock_response):
    """
    Resposta HTTP 201.
    """

    return mock_response(
        html="<html>Created</html>",
        status_code=201,
    )


@pytest.fixture
def mock_response_no_content(mock_response):
    """
    Resposta HTTP 204.
    """

    return mock_response(
        html="",
        status_code=204,
    )


@pytest.fixture
def mock_response_not_found(mock_response):
    """
    Resposta HTTP 404.
    """

    response = mock_response(
        html="<html>Not Found</html>",
        status_code=404,
    )

    response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

    return response


@pytest.fixture
def mock_response_error(mock_response):
    """
    Resposta HTTP 500.
    """

    response = mock_response(
        html="<html>Erro interno</html>",
        status_code=500,
    )

    response.raise_for_status.side_effect = requests.HTTPError(
        "500 Internal Server Error"
    )

    return response


@pytest.fixture
def mock_json_response(mock_response):
    """
    Factory para respostas HTTP JSON.
    """

    def _factory(
        data,
        status_code=200,
    ):

        response = mock_response(
            html="",
            status_code=status_code,
        )

        response.json.return_value = data

        return response

    return _factory


# ============================================================
# FIXTURES - TQDM
# ============================================================


@pytest.fixture
def mock_tqdm():
    """
    Mock da biblioteca tqdm.
    """

    with patch("tqdm.tqdm") as mock:
        progress = mock.return_value.__enter__.return_value

        progress.update = MagicMock()

        yield mock


# ============================================================
# FIXTURES - DATAFRAMES
# ============================================================


@pytest.fixture
def df_sem_header():
    """
    DataFrame sem cabeçalho esperado.
    """

    return pd.DataFrame(
        [
            ["Região A", "10", "20"],
            ["Região B", "30", "40"],
        ]
    )


@pytest.fixture
def df_vazio():
    """
    DataFrame vazio.
    """

    return pd.DataFrame()


@pytest.fixture
def df_exemplo():
    """
    DataFrame genérico para testes.
    """

    return pd.DataFrame(
        {
            "Região": [
                "Região A",
                "Região B",
                "Região C",
            ],
            "Ocorrências": [
                10,
                30,
                20,
            ],
        }
    )


@pytest.fixture
def df_crimes():
    """
    DataFrame simplificado para testes
    relacionados à criminalidade.
    """

    return pd.DataFrame(
        {
            "Região": [
                "Região A",
                "Região A",
                "Região B",
                "Região B",
                "Região C",
            ],
            "Crime": [
                "Furto",
                "Roubo",
                "Furto",
                "Homicídio",
                "Roubo",
            ],
            "Quantidade": [
                10,
                5,
                20,
                2,
                8,
            ],
        }
    )


# ============================================================
# FIXTURES - CSV
# ============================================================


@pytest.fixture
def mock_csv():
    """
    Factory para mockar builtins.open
    e simular arquivos CSV.
    """

    def _mock_csv(linhas):

        conteudo = "\n".join(linhas)

        mocked_open = mock_open(read_data=conteudo)

        return patch(
            "builtins.open",
            mocked_open,
        )

    return _mock_csv


@pytest.fixture
def mock_open_file():
    """
    Factory genérica para mockar open().
    """

    def _mock_open_file(
        conteudo="",
    ):

        mocked_open = mock_open(read_data=conteudo)

        return patch(
            "builtins.open",
            mocked_open,
        )

    return _mock_open_file


# ============================================================
# FIXTURES - ARQUIVOS TEMPORÁRIOS
# ============================================================


@pytest.fixture
def arquivo_csv_tmp(tmp_path):
    """
    Cria um arquivo CSV temporário.
    """

    def _create_file(linhas):

        arquivo = tmp_path / "entrada.csv"

        arquivo.write_text(
            "\n".join(linhas),
            encoding="utf-8",
        )

        return arquivo

    return _create_file


@pytest.fixture
def arquivo_txt_tmp(tmp_path):
    """
    Cria um arquivo TXT temporário.
    """

    def _create_file(
        conteudo="",
        nome="arquivo.txt",
    ):

        arquivo = tmp_path / nome

        arquivo.write_text(
            conteudo,
            encoding="utf-8",
        )

        return arquivo

    return _create_file


# ============================================================
# FIXTURES - DIRETÓRIOS
# ============================================================


@pytest.fixture
def diretorio_saida(tmp_path):
    """
    Cria diretório temporário de saída.
    """

    diretorio = tmp_path / "saida"

    diretorio.mkdir()

    return diretorio


@pytest.fixture
def diretorio_entrada(tmp_path):
    """
    Cria diretório temporário de entrada.
    """

    diretorio = tmp_path / "entrada"

    diretorio.mkdir()

    return diretorio


# ============================================================
# FIXTURES - LOGGING
# ============================================================


@pytest.fixture
def mock_logger():
    """
    Mock completo do logger de tratamento_crimes.
    """

    logger = MagicMock()

    with patch(
        "src.tratamento_crimes.logger",
        logger,
    ):
        yield logger


@pytest.fixture
def mock_logger_error():
    """
    Mock específico de logger.error().
    """

    with patch("src.tratamento_crimes.logger.error") as mock:
        yield mock


# ============================================================
# HELPER - HEADER NÃO ENCONTRADO
# ============================================================


def assert_header_nao_encontrado(
    func,
    entrada,
    saida=None,
):
    """
    Valida funções que devem lançar ValueError
    quando o cabeçalho esperado não é encontrado.
    """

    with patch("src.tratamento_crimes.logger.error") as mock_logger_error:
        with pytest.raises(
            ValueError,
            match="Header não encontrado no CSV",
        ):
            if saida is not None:
                func(
                    entrada,
                    saida,
                )

            else:
                func(entrada)

        mock_logger_error.assert_called_once_with(
            "Header não encontrado no arquivo %s",
            entrada,
        )


# ============================================================
# FIXTURES - DATETIME
# ============================================================


@pytest.fixture
def data_hora_teste():
    """
    Data/hora fixa para testes determinísticos.
    """

    return datetime(
        2026,
        1,
        1,
        12,
        0,
        0,
    )


# ============================================================
# FIXTURES - DOWNLOAD
# ============================================================


@pytest.fixture
def mock_download_response(
    mock_response,
):
    """
    Resposta padrão para testes de download.
    """

    return mock_response(
        html="conteudo de teste",
        status_code=200,
    )


@pytest.fixture
def mock_download_error_response(
    mock_response,
):
    """
    Resposta de erro para testes de download.
    """

    response = mock_response(
        html="erro de download",
        status_code=500,
    )

    response.raise_for_status.side_effect = requests.HTTPError(
        "Erro durante o download"
    )

    return response


# ============================================================
# FIXTURES - VARIÁVEIS DE AMBIENTE
# ============================================================


@pytest.fixture
def mock_env(monkeypatch):
    """
    Configura variáveis de ambiente temporárias.
    """

    def _set_env(**variables):

        for key, value in variables.items():
            monkeypatch.setenv(
                key,
                str(value),
            )

    return _set_env


# ============================================================
# FIXTURES - CONFIGURAÇÃO
# ============================================================


@pytest.fixture
def configuracao_padrao():
    """
    Configuração padrão para testes.
    """

    return {
        "timeout": 30,
        "encoding": "utf-8",
    }
