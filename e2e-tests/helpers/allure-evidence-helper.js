const fs = require('fs');
const path = require('path');

class AllureEvidenceHelper {

    constructor(config) {

        this.config = config;

        console.log(
            '\x1b[36m[ALLURE] AllureEvidenceHelper inicializado.\x1b[0m'
        );
    }

    async _after(test) {

        console.log(
            '\n========================================'
        );

        console.log(
            '[ALLURE] TESTE FINALIZADO'
        );

        console.log(
            `[ALLURE] Nome: ${test.title}`
        );

        console.log(
            '========================================'
        );

        console.log(
            '[ALLURE] test.artifacts:'
        );

        console.log(
            JSON.stringify(
                test.artifacts,
                null,
                2
            )
        );

        console.log(
            '========================================'
        );

        // ==========================================
        // OUTPUT
        // ==========================================

        const outputDir =
            path.resolve('./output');

        console.log(
            `[ALLURE] Output: ${outputDir}`
        );

        if (!fs.existsSync(outputDir)) {

            console.log(
                '[ALLURE] Diretório output não existe.'
            );

            return;
        }

        const arquivos =
            listarArquivos(outputDir);

        console.log(
            '[ALLURE] Arquivos encontrados:'
        );

        for (const arquivo of arquivos) {

            console.log(
                `  - ${arquivo}`
            );
        }

        console.log(
            '========================================\n'
        );
    }
}


// ==========================================
// LISTAR ARQUIVOS
// ==========================================

function listarArquivos(dir) {

    const resultado = [];

    if (!fs.existsSync(dir)) {
        return resultado;
    }

    for (const entry of fs.readdirSync(
        dir,
        {
            withFileTypes: true
        }
    )) {

        const fullPath =
            path.join(dir, entry.name);

        if (entry.isDirectory()) {

            resultado.push(
                ...listarArquivos(fullPath)
            );

        } else {

            resultado.push(
                fullPath
            );
        }
    }

    return resultado;
}


module.exports =
    AllureEvidenceHelper;