const path = require("path");
const fs = require("fs");

let { Client } = require('@elastic/elasticsearch')
let client = new Client({ node: 'http://localhost:9200' })

const INDEX_NAME = "recettes";
const DATA_FOLDER = "data";

// Démarrage du script d'indexation
start();

async function start() {
  await deleteIndexIfExist();
  // Démarrage de l'indexation
  await startIndexer({ mode: "bulk" });
}

/**
 * Lancer l'indexation des fichiers de recettes.
 * 
 * @param {*} options d'indexation
 *  Si options.mode == "bulk" indexation de masse
 *  ...
 */
async function startIndexer(options = {}) {
  const dataPath = path.join(__dirname, DATA_FOLDER);

  if (options.mode === "bulk") {
    // Indexation de masse
    console.info("Indexation en mode BULK");
    const dataset = [];
    // Lire le contenu de /data et indexer
    fs.readdir(dataPath, async function (err, files) {
      if (err) {
        return console.log('Unable to scan directory: ' + err);
      }
      files.forEach(file => {
        const absoluteFilePath = path.join(dataPath, file);
        const data = fs.readFileSync(absoluteFilePath, "utf8");
        dataset.push(JSON.parse(data));
      });
      const body = dataset.flatMap(doc => [{ index: { _index: INDEX_NAME } }, doc])
      const { body: bulkResponse } = await client.bulk({ refresh: true, body })

      console.info(`Temps de traitement : ${bulkResponse.took}ms`);
    }); 
  } else {
    // Indexation normale
    console.info("Indexation en mode NORMAL");
    // Lire le contenu de /data et indexer
    fs.readdir(dataPath, function (err, files) {
      if (err) {
        return console.error('Unable to scan directory: ' + err);
      }
      // Indexer chaque fichier
      files.forEach(function (file) {
        const absoluteFilePath = path.join(dataPath, file);
        indexSingleJsonFile(absoluteFilePath); 
      });
    }); 
  }
}

/**
 * Supprime l'index INDEX_NAME s'il existe.
 */
async function deleteIndexIfExist() {
  // ATTENTION: supprimer l'index des recettes !
  console.log(`L'index ${INDEX_NAME} existe-t-il ?`);
  const response = await client.indices.exists({ index: INDEX_NAME });
  // Si l'index existe body === true
  if (response.body) {
    console.log("Oui, suppression en cours ...");
    await client.indices.delete({ index: INDEX_NAME });
  } else {
    console.log("Non, tout est ok !");
  }
}

/**
 * Indexer un seul fichier dans l'index INDEX_NAME, par son chemin absolu.
 */
function indexSingleJsonFile(absolutePath) {
  const jsonString = fs.readFile(absolutePath, "utf8", (err, data) => {
    if (err) {
      console.error(err);
      return;
    }
    // Si besoin de transformer la donnée (XML en JSON par exemple)
    // Ecrire une méthode de transformation, qui utilise un parseur XML
    // Ex:
    // const jsonData = JSONTransformerForES(data);

    const jsonData = data; // Ici c'est sans transformation, car déjà au bon format

    // Indexer le contenu
    const dataWithIndex = { 
        index: INDEX_NAME,
        body: jsonData
    };
    client.index(dataWithIndex);
  });
}