require('dotenv').config();
const { MongoClient } = require('mongodb');

const {
  ATLAS_URI,
  LOCAL_MONGO_URI,
  ATLAS_DB,
  ATLAS_COLLECTION
} = process.env;

async function showAllDocs(localCollection) {
  const allDocs = await localCollection.find({}).toArray();
  console.log('Current documents in local collection:');
  allDocs.forEach(doc => console.log(JSON.stringify(doc)));
}

async function mirrorChangeToLocal(change, localCollection) {
  const docId = change.documentKey._id;
  if (change.operationType === 'insert' || change.operationType === 'replace') {
    await localCollection.replaceOne({ _id: docId }, change.fullDocument, { upsert: true });
    console.log(`[${new Date().toISOString()}] Upserted doc with _id: ${docId}`);
    await showAllDocs(localCollection);
  } else if (change.operationType === 'update') {
    await localCollection.updateOne({ _id: docId }, { $set: change.updateDescription.updatedFields });
    console.log(`[${new Date().toISOString()}] Updated doc with _id: ${docId}`);
    await showAllDocs(localCollection);
  } else if (change.operationType === 'delete') {
    await localCollection.deleteOne({ _id: docId });
    console.log(`[${new Date().toISOString()}] Deleted doc with _id: ${docId}`);
    await showAllDocs(localCollection);
  } else {
    console.log(`Unhandled operation type: ${change.operationType}`);
  }
}

async function initialSync(atlasCollection, localCollection) {
  console.log('Performing initial sync...');
  const [atlasDocs, localDocs] = await Promise.all([
    atlasCollection.find({}).toArray(),
    localCollection.find({}).toArray()
  ]);
  const localMap = new Map(localDocs.map(doc => [String(doc._id), doc]));
  let syncedCount = 0;
  for (const atlasDoc of atlasDocs) {
    const localDoc = localMap.get(String(atlasDoc._id));
    // Simple deep comparison (could be optimized)
    if (!localDoc || JSON.stringify(localDoc) !== JSON.stringify(atlasDoc)) {
      await localCollection.replaceOne({ _id: atlasDoc._id }, atlasDoc, { upsert: true });
      console.log(`[${new Date().toISOString()}] Synced doc with _id: ${atlasDoc._id}`);
      syncedCount++;
      await showAllDocs(localCollection);
    }
  }
  console.log(`Initial sync complete. ${syncedCount} docs upserted/updated.`);
}

async function main() {
  const {
    EFFORTS_MONGO_URL,
    LOCAL_MONGO_URI,
    ATLAS_URI,
    ATLAS_DB,
    ATLAS_COLLECTION
  } = process.env;

  let sourceUri, sourceDbName;
  if (EFFORTS_MONGO_URL) {
    sourceUri = EFFORTS_MONGO_URL;
    sourceDbName = 'efforts';
  } else {
    sourceUri = ATLAS_URI;
    sourceDbName = ATLAS_DB;
  }

  const atlasClient = new MongoClient(sourceUri);
  const localClient = new MongoClient(LOCAL_MONGO_URI);
  await atlasClient.connect();
  await localClient.connect();

  const atlasDb = atlasClient.db(sourceDbName);
  const localDb = localClient.db(sourceDbName);
  const atlasCollection = atlasDb.collection(ATLAS_COLLECTION);
  const localCollection = localDb.collection(ATLAS_COLLECTION);

  // Initial sync before watching for changes
  await initialSync(atlasCollection, localCollection);

  console.log('Listening for changes on Atlas collection...');

  const changeStream = atlasCollection.watch();
  changeStream.on('change', async (change) => {
    try {
      await mirrorChangeToLocal(change, localCollection);
    } catch (err) {
      console.error('Error mirroring change:', err);
    }
  });
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
