const exibir = require('../utils/util.js');

module.exports = {

   init() {

      exibir.exibirData();
      exibir.exibirHora();
      exibir.exibirLocalidade();

      console.log(
         "\x1b[34m\n\t\t\tStart run Tests:\x1b[0m\n"
      );
   }

};