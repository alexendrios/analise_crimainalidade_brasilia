const fs = require('fs');
const path = require('path');
const allure = require('allure-js-commons');

function localizarArquivoMaisRecente(diretorio, extensao) {

    if (!fs.existsSync(diretorio)) {
        return null;
    }

    const arquivos = [];

    function percorrer(dir) {

        for (const entry of fs.readdirSync(dir, {
            withFileTypes: true
        })) {

            const fullPath = path.join(dir, entry.name);

            if (entry.isDirectory()) {

                percorrer(fullPath);

                continue;
            }

            if (
                entry.isFile() &&
                entry.name.toLowerCase().endsWith(extensao)
            ) {

                arquivos.push(fullPath);
            }
        }
    }

    percorrer(diretorio);

    if (arquivos.length === 0) {
        return null;
    }

    arquivos.sort((a, b) => {

        return (
            fs.statSync(b).mtimeMs -
            fs.statSync(a).mtimeMs
        );
    });

    return arquivos[0];
}


async function anexarArquivo(
    nome,
    arquivo,
    contentType,
    fileExtension
) {

    if (!arquivo) {

        console.log(
            `[ALLURE] ${nome}: arquivo não encontrado.`
        );

        return;
    }

    if (!fs.existsSync(arquivo)) {

        console.log(
            `[ALLURE] ${nome}: arquivo não existe: ${arquivo}`
        );

        return;
    }

    try {

        await allure.attachmentPath(
            nome,
            arquivo,
            {
                contentType,
                fileExtension
            }
        );

        console.log(
            `[ALLURE] ✓ Anexado: ${nome}`
        );

    } catch (error) {

        console.error(
            `[ALLURE] ✗ Erro ao anexar ${nome}:`,
            error.message
        );
    }
}


async function anexarEvidencias() {

    const outputDir =
        path.resolve('./output');

    console.log(
        '\n[ALLURE] Procurando evidências em:',
        outputDir
    );


    // ==========================================
    // SCREENSHOT
    // ==========================================

    const screenshot =
        localizarArquivoMaisRecente(
            outputDir,
            '.png'
        );

    await anexarArquivo(
        'Screenshot - Falha',
        screenshot,
        'image/png',
        'png'
    );


    // ==========================================
    // VIDEO
    // ==========================================

    const video =
        localizarArquivoMaisRecente(
            outputDir,
            '.webm'
        );

    await anexarArquivo(
        'Vídeo - Execução',
        video,
        'video/webm',
        'webm'
    );


    // ==========================================
    // TRACE
    // ==========================================

    const trace =
        localizarArquivoMaisRecente(
            outputDir,
            '.zip'
        );

    await anexarArquivo(
        'Playwright Trace',
        trace,
        'application/zip',
        'zip'
    );


    console.log(
        '[ALLURE] Evidências processadas.\n'
    );
}


module.exports = {
    anexarEvidencias
};