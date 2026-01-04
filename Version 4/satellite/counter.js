// counter.js semplificato
const fs = require('fs');

let counter = 0;
console.log("--- APP AVVIATA ---");

// Loop infinito
setInterval(() => {
    counter++;
    console.log(`Conteggio: ${counter}`);
}, 1000);