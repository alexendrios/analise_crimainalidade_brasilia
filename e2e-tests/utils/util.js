let dataHoraAtual = new Date();
let localidade = Intl.DateTimeFormat().resolvedOptions().locale;
let  dataFormatada = dataHoraAtual.toLocaleDateString(localidade);
let  horaFormatada = dataHoraAtual.toLocaleTimeString(localidade);

module.exports={


    exibirData(){
        console.log("\x1b[31m\nData:\x1b[0m", dataFormatada);
    },

    exibirHora(){
        console.log("\x1b[31mHora:\x1b[0m", horaFormatada);
    }, 

    exibirLocalidade(){
        console.log("\x1b[31mLocalidade:\x1b[0m", localidade);
    },

}