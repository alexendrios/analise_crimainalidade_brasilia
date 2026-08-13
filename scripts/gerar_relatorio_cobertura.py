"""
gerar_relatorio_cobertura.py

Gera um relatório executivo de cobertura de código para o projeto
Criminalidade Brasília - DF.

Fonte oficial dos dados:
    .coverage

Relatórios gerados:
    tests_report/cobertura-executiva.html
    tests_report/coverage/index.html

O coverage.py continua sendo responsável pelos dados técnicos.
Este script apenas apresenta esses dados de forma executiva.

Compatibilidade:
    Python 3.13+
    coverage.py
"""

from __future__ import annotations

import html
import sys
from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIGURAÇÃO
# ============================================================

PROJECT_NAME = "Criminalidade Brasília - DF"

REPORT_TITLE = "Relatório Executivo de Cobertura de Código"

DESCRIPTION = (
    "Análise consolidada da cobertura de código da aplicação, "
    "com base nos resultados reais obtidos pelo coverage.py."
)

MIN_COVERAGE = 85.0

PROJECT_ROOT = Path(__file__).resolve().parent.parent

COVERAGE_FILE = PROJECT_ROOT / ".coverage"

REPORTS_DIR = PROJECT_ROOT / "test_report"

EXECUTIVE_REPORT = REPORTS_DIR / "cobertura-executiva.html"

TECHNICAL_REPORT = REPORTS_DIR / "coverage" / "index.html"


# ============================================================
# UTILITÁRIOS
# ============================================================


def format_number(value: int | float) -> str:
    """
    Formata números para apresentação no relatório.
    """

    if isinstance(value, float):
        return f"{value:.2f}"

    return f"{value:,}".replace(",", ".")


def coverage_class(value: float) -> str:
    """
    Define a classificação visual da cobertura.
    """

    if value >= 90:
        return "excellent"

    if value >= MIN_COVERAGE:
        return "approved"

    if value >= 70:
        return "warning"

    return "critical"


def coverage_status(value: float) -> tuple[str, str]:
    """
    Retorna status textual e classe CSS.
    """

    if value >= MIN_COVERAGE:
        return "APROVADO", "approved"

    return "ABAIXO DA META", "critical"


def percentage(value: float) -> str:
    """
    Formata percentual.
    """

    return f"{value:.2f}%"


# ============================================================
# LEITURA DO COVERAGE
# ============================================================


def load_coverage():
    """
    Carrega os dados do arquivo .coverage utilizando
    diretamente a API oficial do coverage.py.
    """

    try:
        from coverage import Coverage
    except ImportError:
        print(
            "ERRO: o pacote 'coverage' não está instalado.",
            file=sys.stderr,
        )
        print(
            "Instale com: pip install coverage",
            file=sys.stderr,
        )
        raise SystemExit(1)

    if not COVERAGE_FILE.exists():
        print(
            f"ERRO: arquivo de cobertura não encontrado:\n{COVERAGE_FILE}",
            file=sys.stderr,
        )

        print(
            "\nExecute primeiro:\npytest",
            file=sys.stderr,
        )

        raise SystemExit(1)

    coverage = Coverage(data_file=str(COVERAGE_FILE))

    coverage.load()

    return coverage


# ============================================================
# COLETA DOS INDICADORES
# ============================================================


def collect_metrics(coverage):
    """
    Calcula os indicadores reais da cobertura utilizando
    a API do coverage.py.

    Compatível com diferentes versões do coverage.py.
    """

    data = coverage.get_data()

    files = sorted(data.measured_files())

    total_statements = 0
    total_missing = 0

    total_branches = 0
    total_missing_branches = 0

    modules = []

    for filename in files:
        path = Path(filename)

        try:
            relative_path = path.relative_to(PROJECT_ROOT)
        except ValueError:
            relative_path = path

        try:
            analysis = coverage._analyze(filename)

            # ==================================================
            # LINHAS
            # ==================================================

            statements = len(analysis.statements)

            missing = len(analysis.missing)

            executed = statements - missing

            # ==================================================
            # BRANCHES
            # ==================================================

            branch_total = 0
            branch_missing = 0

            # IMPORTANTE:
            # has_arcs é propriedade booleana em versões
            # atuais do coverage.py.
            has_arcs = getattr(
                analysis,
                "has_arcs",
                False,
            )

            if has_arcs:
                try:
                    arcs = analysis.arc_possibilities()

                    branch_total = len(arcs)

                except (
                    AttributeError,
                    TypeError,
                ):
                    branch_total = 0

                try:
                    missing_arcs = analysis.missing_branch_arcs()

                    if missing_arcs:
                        branch_missing = sum(
                            len(arcs) for arcs in missing_arcs.values()
                        )

                except (
                    AttributeError,
                    TypeError,
                ):
                    branch_missing = 0

            # ==================================================
            # ACUMULADORES
            # ==================================================

            total_statements += statements
            total_missing += missing

            total_branches += branch_total
            total_missing_branches += branch_missing

            # ==================================================
            # COBERTURA DO ARQUIVO
            # ==================================================

            if statements > 0:
                line_coverage = (executed / statements) * 100

            else:
                line_coverage = 100.0

            if branch_total > 0:
                branch_coverage = ((branch_total - branch_missing) / branch_total) * 100

            else:
                branch_coverage = None

            modules.append(
                {
                    "file": str(relative_path),
                    "statements": statements,
                    "executed": executed,
                    "missing": missing,
                    "coverage": line_coverage,
                    "branch_total": branch_total,
                    "branch_missing": branch_missing,
                    "branch_coverage": branch_coverage,
                }
            )

        except Exception as exc:
            print(
                f"Aviso: não foi possível analisar {filename}: {exc}",
                file=sys.stderr,
            )

    # ========================================================
    # COBERTURA GLOBAL DE LINHAS
    # ========================================================

    if total_statements > 0:
        line_coverage = ((total_statements - total_missing) / total_statements) * 100

    else:
        line_coverage = 0.0

    # ========================================================
    # COBERTURA GLOBAL DE BRANCHES
    # ========================================================

    if total_branches > 0:
        branch_coverage = (
            (total_branches - total_missing_branches) / total_branches
        ) * 100

    else:
        branch_coverage = None

    # ========================================================
    # RESULTADO
    # ========================================================

    return {
        "files": files,
        "modules": modules,
        "total_statements": (total_statements),
        "executed": (total_statements - total_missing),
        "missing": total_missing,
        "line_coverage": (line_coverage),
        "total_branches": (total_branches),
        "missing_branches": (total_missing_branches),
        "branch_coverage": (branch_coverage),
    }


# ============================================================
# HTML
# ============================================================


def build_html(metrics: dict) -> str:
    """
    Constrói o relatório HTML executivo.
    """

    line_coverage = metrics["line_coverage"]

    branch_coverage = metrics["branch_coverage"]

    status, status_class = coverage_status(line_coverage)

    margin = line_coverage - MIN_COVERAGE

    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    module_rows = []

    sorted_modules = sorted(
        metrics["modules"],
        key=lambda item: (
            -item["coverage"],
            item["file"],
        ),
    )

    for module in sorted_modules:
        module_coverage = module["coverage"]

        module_class = coverage_class(module_coverage)

        branch_value = (
            percentage(module["branch_coverage"])
            if module["branch_coverage"] is not None
            else "N/A"
        )

        module_rows.append(
            f"""
            <tr>
                <td>
                    <span class="file-name">
                        {html.escape(module["file"])}
                    </span>
                </td>

                <td class="number">
                    {format_number(module["statements"])}
                </td>

                <td class="number">
                    {format_number(module["executed"])}
                </td>

                <td class="number">
                    {format_number(module["missing"])}
                </td>

                <td>
                    <div class="coverage-cell">

                        <div class="progress">
                            <div
                                class="progress-bar {module_class}"
                                style="width: {module_coverage:.2f}%"
                            ></div>
                        </div>

                        <span>
                            {percentage(module_coverage)}
                        </span>

                    </div>
                </td>

                <td class="number">
                    {branch_value}
                </td>
            </tr>
            """
        )

    modules_html = "\n".join(module_rows)

    branch_display = (
        percentage(branch_coverage) if branch_coverage is not None else "N/A"
    )

    technical_link = ""

    if TECHNICAL_REPORT.exists():
        technical_link = """
        <a
            class="button secondary"
            href="coverage/index.html"
        >
            Abrir relatório técnico do Coverage.py
        </a>
        """

    return f"""
<!DOCTYPE html>

<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>
    {html.escape(PROJECT_NAME)}
    -
    {html.escape(REPORT_TITLE)}
</title>

<style>

* {{
    box-sizing: border-box;
}}

body {{
    margin: 0;
    padding: 0;

    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;

    background: #f4f7fb;
    color: #1f2937;
}}

.container {{
    width: min(1400px, 94%);
    margin: 0 auto;
}}

.header {{
    margin-top: 30px;
    padding: 34px;

    background: #ffffff;

    border-radius: 14px;

    border-left: 7px solid #1e3a5f;

    box-shadow:
        0 4px 18px rgba(15, 23, 42, 0.08);
}}

.header h1 {{
    margin: 0;

    color: #1e3a5f;

    font-size: 30px;
}}

.header h2 {{
    margin: 8px 0 12px;

    font-size: 21px;

    font-weight: 600;

    color: #334155;
}}

.header p {{
    margin: 0;

    max-width: 900px;

    color: #64748b;

    line-height: 1.6;
}}

.status {{
    margin-top: 22px;

    padding: 18px;

    border-radius: 10px;

    text-align: center;

    font-size: 20px;

    font-weight: 700;
}}

.status.approved {{
    background: #ecfdf5;
    color: #166534;
    border: 1px solid #86efac;
}}

.status.critical {{
    background: #fef2f2;
    color: #991b1b;
    border: 1px solid #fca5a5;
}}

.hero {{
    margin-top: 22px;

    padding: 38px;

    background: #ffffff;

    border-radius: 14px;

    text-align: center;

    box-shadow:
        0 4px 18px rgba(15, 23, 42, 0.08);
}}

.hero-value {{
    font-size: 72px;

    font-weight: 800;

    color: #1e3a5f;

    line-height: 1;
}}

.hero-label {{
    margin-top: 12px;

    color: #64748b;

    font-size: 15px;

    text-transform: uppercase;

    letter-spacing: 1.5px;
}}

.cards {{
    display: grid;

    grid-template-columns:
        repeat(auto-fit, minmax(210px, 1fr));

    gap: 16px;

    margin-top: 22px;
}}

.card {{
    background: #ffffff;

    border-radius: 12px;

    padding: 24px;

    text-align: center;

    box-shadow:
        0 4px 18px rgba(15, 23, 42, 0.06);
}}

.card-value {{
    font-size: 30px;

    font-weight: 750;

    color: #1e3a5f;
}}

.card-label {{
    margin-top: 7px;

    font-size: 13px;

    color: #64748b;
}}

.section {{
    margin-top: 28px;

    padding: 28px;

    background: #ffffff;

    border-radius: 14px;

    box-shadow:
        0 4px 18px rgba(15, 23, 42, 0.06);
}}

.section h3 {{
    margin-top: 0;

    padding-bottom: 12px;

    border-bottom: 2px solid #e2e8f0;

    color: #1e3a5f;

    font-size: 20px;
}}

.table-wrapper {{
    overflow-x: auto;
}}

table {{
    width: 100%;

    border-collapse: collapse;

    min-width: 850px;
}}

th {{
    padding: 13px 12px;

    background: #f8fafc;

    color: #475569;

    text-align: left;

    font-size: 13px;

    border-bottom: 2px solid #e2e8f0;
}}

td {{
    padding: 13px 12px;

    border-bottom: 1px solid #e2e8f0;

    font-size: 13px;
}}

.number {{
    text-align: right;

    font-variant-numeric: tabular-nums;
}}

.file-name {{
    font-family:
        Consolas,
        "Courier New",
        monospace;

    color: #334155;
}}

.coverage-cell {{
    display: flex;

    align-items: center;

    gap: 10px;
}}

.progress {{
    width: 130px;

    height: 9px;

    background: #e2e8f0;

    border-radius: 20px;

    overflow: hidden;
}}

.progress-bar {{
    height: 100%;

    border-radius: 20px;
}}

.progress-bar.excellent {{
    background: #15803d;
}}

.progress-bar.approved {{
    background: #2563eb;
}}

.progress-bar.warning {{
    background: #d97706;
}}

.progress-bar.critical {{
    background: #dc2626;
}}

.info-table {{
    width: 100%;

    min-width: auto;
}}

.info-table td:first-child {{
    width: 35%;

    font-weight: 600;

    color: #475569;
}}

.info-table td:last-child {{
    color: #1f2937;
}}

.buttons {{
    display: flex;

    flex-wrap: wrap;

    gap: 12px;

    margin-top: 20px;
}}

.button {{
    display: inline-block;

    padding: 11px 18px;

    border-radius: 8px;

    text-decoration: none;

    font-size: 14px;

    font-weight: 600;
}}

.button.secondary {{
    background: #1e3a5f;

    color: #ffffff;
}}

.footer {{
    margin: 28px 0;

    padding: 20px;

    text-align: center;

    color: #64748b;

    font-size: 12px;
}}

@media (max-width: 700px) {{

    .header {{
        padding: 24px;
    }}

    .header h1 {{
        font-size: 24px;
    }}

    .hero-value {{
        font-size: 56px;
    }}

    .section {{
        padding: 20px;
    }}

}}

</style>

</head>

<body>

<div class="container">

    <header class="header">

        <h1>
            {html.escape(PROJECT_NAME)}
        </h1>

        <h2>
            {html.escape(REPORT_TITLE)}
        </h2>

        <p>
            {html.escape(DESCRIPTION)}
        </p>

    </header>


    <div class="status {status_class}">
        {"✓" if status == "APROVADO" else "✕"}
        {status}
    </div>


    <section class="hero">

        <div class="hero-value">
            {percentage(line_coverage)}
        </div>

        <div class="hero-label">
            Cobertura total de linhas
        </div>

    </section>


    <section class="cards">

        <div class="card">

            <div class="card-value">
                {percentage(line_coverage)}
            </div>

            <div class="card-label">
                Cobertura de linhas
            </div>

        </div>


        <div class="card">

            <div class="card-value">
                {percentage(MIN_COVERAGE)}
            </div>

            <div class="card-label">
                Meta mínima
            </div>

        </div>


        <div class="card">

            <div class="card-value">
                {margin:+.2f} p.p.
            </div>

            <div class="card-label">
                Margem sobre a meta
            </div>

        </div>


        <div class="card">

            <div class="card-value">
                {format_number(len(metrics["modules"]))}
            </div>

            <div class="card-label">
                Arquivos analisados
            </div>

        </div>


        <div class="card">

            <div class="card-value">
                {format_number(metrics["total_statements"])}
            </div>

            <div class="card-label">
                Linhas de código
            </div>

        </div>


        <div class="card">

            <div class="card-value">
                {format_number(metrics["missing"])}
            </div>

            <div class="card-label">
                Linhas não cobertas
            </div>

        </div>

    </section>


    <section class="section">

        <h3>
            Indicadores de Branch Coverage
        </h3>

        <table class="info-table">

            <tr>
                <td>
                    Branch coverage
                </td>

                <td>
                    {branch_display}
                </td>
            </tr>

            <tr>
                <td>
                    Branches analisados
                </td>

                <td>
                    {format_number(metrics["total_branches"])}
                </td>
            </tr>

            <tr>
                <td>
                    Branches não cobertos
                </td>

                <td>
                    {format_number(metrics["missing_branches"])}
                </td>
            </tr>

        </table>

    </section>


    <section class="section">

        <h3>
            Cobertura por Arquivo
        </h3>

        <div class="table-wrapper">

            <table>

                <thead>

                    <tr>

                        <th>
                            Arquivo
                        </th>

                        <th>
                            Linhas
                        </th>

                        <th>
                            Executadas
                        </th>

                        <th>
                            Não cobertas
                        </th>

                        <th>
                            Cobertura
                        </th>

                        <th>
                            Branch
                        </th>

                    </tr>

                </thead>

                <tbody>

                    {modules_html}

                </tbody>

            </table>

        </div>

    </section>


    <section class="section">

        <h3>
            Informações da Execução
        </h3>

        <table class="info-table">

            <tr>
                <td>
                    Projeto
                </td>

                <td>
                    {html.escape(PROJECT_NAME)}
                </td>
            </tr>

            <tr>
                <td>
                    Ferramenta
                </td>

                <td>
                    coverage.py / pytest-cov
                </td>
            </tr>

            <tr>
                <td>
                    Branch Coverage
                </td>

                <td>
                    {"Ativada" if metrics["total_branches"] else "Não disponível"}
                </td>
            </tr>

            <tr>
                <td>
                    Meta mínima
                </td>

                <td>
                    {percentage(MIN_COVERAGE)}
                </td>
            </tr>

            <tr>
                <td>
                    Cobertura obtida
                </td>

                <td>
                    {percentage(line_coverage)}
                </td>
            </tr>

            <tr>
                <td>
                    Status
                </td>

                <td>
                    {status}
                </td>
            </tr>

            <tr>
                <td>
                    Data/Hora
                </td>

                <td>
                    {generated_at}
                </td>
            </tr>

        </table>

        <div class="buttons">

            {technical_link}

        </div>

    </section>


    <footer class="footer">

        Relatório gerado automaticamente a partir dos dados
        oficiais do coverage.py.

        <br>

        Criminalidade Brasília - DF

    </footer>

</div>

</body>

</html>
"""


# ============================================================
# GERAÇÃO DO RELATÓRIO
# ============================================================


def generate_report() -> None:
    """
    Executa todo o processo de geração.
    """

    print("=" * 70)

    print("CRIMINALIDADE BRASÍLIA - DF")

    print("GERAÇÃO DO RELATÓRIO EXECUTIVO DE COBERTURA")

    print("=" * 70)

    coverage = load_coverage()

    print("\nLendo dados do coverage.py...")

    metrics = collect_metrics(coverage)

    REPORTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_html = build_html(metrics)

    EXECUTIVE_REPORT.write_text(
        report_html,
        encoding="utf-8",
    )

    print("\nRelatório gerado com sucesso:")

    print(f"  {EXECUTIVE_REPORT}")

    print("\nIndicadores:")

    print(f"  Cobertura de linhas : {percentage(metrics['line_coverage'])}")

    if metrics["branch_coverage"] is not None:
        print(f"  Cobertura de branches: {percentage(metrics['branch_coverage'])}")

    print(f"  Meta mínima         : {percentage(MIN_COVERAGE)}")

    print(f"  Arquivos analisados : {format_number(len(metrics['modules']))}")

    print(f"  Linhas analisadas   : {format_number(metrics['total_statements'])}")

    print(f"  Linhas executadas   : {format_number(metrics['executed'])}")

    print(f"  Linhas não cobertas : {format_number(metrics['missing'])}")

    status, _ = coverage_status(metrics["line_coverage"])

    print(f"  Status              : {status}")

    print("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    generate_report()
