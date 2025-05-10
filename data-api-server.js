require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');

// --- Atlas-to-local sync logic (from sync.js) ---
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
    if (!localDoc || JSON.stringify(localDoc) !== JSON.stringify(atlasDoc)) {
      await localCollection.replaceOne({ _id: atlasDoc._id }, atlasDoc, { upsert: true });
      console.log(`[${new Date().toISOString()}] Synced doc with _id: ${atlasDoc._id}`);
      syncedCount++;
      await showAllDocs(localCollection);
    }
  }
  console.log(`Initial sync complete. ${syncedCount} docs upserted/updated.`);
}

async function startAtlasSync() {
  try {
    const atlasClient = new MongoClient(process.env.ATLAS_URI);
    const localClient = new MongoClient(process.env.LOCAL_MONGO_URI);
    await atlasClient.connect();
    await localClient.connect();

    const atlasDb = atlasClient.db(process.env.ATLAS_DB);
    const localDb = localClient.db(process.env.ATLAS_DB);
    const atlasCollection = atlasDb.collection(process.env.ATLAS_COLLECTION);
    const localCollection = localDb.collection(process.env.ATLAS_COLLECTION);

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
  } catch (err) {
    console.error('Fatal error in Atlas sync:', err);
  }
}
// --- End sync.js logic ---

const app = express();
app.use(express.json());
app.use(cors());

// Ensure Collection Exists or Create It
app.post('/ensureCollection', async (req, res) => {
  try {
    await connectToMongo();
    const collectionName = req.body.collection;
    if (!collectionName) return res.status(400).json({ error: 'Missing collection name' });
    const db = global.mongoClient.db(process.env.ATLAS_DB);
    const collections = await db.listCollections({}, { nameOnly: true }).toArray();
    const exists = collections.some(col => col.name === collectionName);
    if (!exists) {
      // Create by inserting and deleting a dummy doc
      const tempCol = db.collection(collectionName);
      const dummy = { __dummy: true };
      const insertRes = await tempCol.insertOne(dummy);
      await tempCol.deleteOne({ _id: insertRes.insertedId });
      return res.json({ created: true });
    }
    return res.json({ created: false });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Helper to convert _id string to ObjectId in filters
function normalizeIdInFilter(filter) {
  if (!filter) return filter;
  if (
    typeof filter._id === 'string' &&
    ObjectId.isValid(filter._id) &&
    !(filter._id instanceof ObjectId)
  ) {
    // Return a new filter object to avoid mutating the original
    return { ...filter, _id: new ObjectId(filter._id) };
  }
  return filter;
}

const {
  EFFORTS_MONGO_URL,
  ATLAS_URI,
  ATLAS_DB,
  ATLAS_COLLECTION
} = process.env;

let client, collection;

async function connectToMongo() {
  if (!client) {
    client = new MongoClient(EFFORTS_MONGO_URL || ATLAS_URI);
    await client.connect();
    const db = client.db(ATLAS_DB);
    collection = db.collection(ATLAS_COLLECTION);
  }
}

// Insert One
app.post('/insertOne', async (req, res) => {
  console.log('[POST] /insertOne - Payload:', req.body);
  try {
    await connectToMongo();
    const collectionName = req.body.collection || process.env.ATLAS_COLLECTION;
    const dynamicCollection = getCollection(collectionName);
    let doc = req.body.document;
    if (doc && doc._id) {
      // Remove _id to ensure MongoDB generates it automatically
      const { _id, ...rest } = doc;
      doc = rest;
    }
    const result = await dynamicCollection.insertOne(doc);
    console.log('[SUCCESS] /insertOne - MongoDB result:', result);
    res.json({ insertedId: result.insertedId });
  } catch (err) {
    console.error('[ERROR] /insertOne', err);
    res.status(500).json({ error: err.message });
  }
});

// Find
app.post('/find', async (req, res) => {
  console.log('[POST] /find - Payload:', req.body);
  try {
    await connectToMongo();
    const collectionName = req.body.collection || process.env.ATLAS_COLLECTION;
    const dynamicCollection = getCollection(collectionName);
    let filter = req.body.filter || {};
    filter = normalizeIdInFilter(filter);
    const docs = await dynamicCollection.find(filter).toArray();
    console.log(`[SUCCESS] /find - Returned ${docs.length} documents`);
    console.log('[RESULT] /find - Documents:', docs);
    res.json({ documents: docs });
  } catch (err) {
    console.error('[ERROR] /find', err);
    res.status(500).json({ error: err.message });
  }
});

// Update One
app.post('/updateOne', async (req, res) => {
  console.log('[POST] /updateOne - Payload:', req.body);
  try {
    await connectToMongo();
    const collectionName = req.body.collection || process.env.ATLAS_COLLECTION;
    const dynamicCollection = getCollection(collectionName);
    let filter = req.body.filter;
    filter = normalizeIdInFilter(filter);
    let update = req.body.update;
    if (update && update.$set && update.$set._id) {
      // Remove _id from $set to avoid immutable field error
      const { _id, ...rest } = update.$set;
      update = { ...update, $set: rest };
    }
    const result = await dynamicCollection.updateOne(filter, update);
    console.log(`[SUCCESS] /updateOne - Matched: ${result.matchedCount}, Modified: ${result.modifiedCount}`);
    console.log('[RESULT] /updateOne - MongoDB result:', result);
    res.json({ matchedCount: result.matchedCount, modifiedCount: result.modifiedCount });
  } catch (err) {
    console.error('[ERROR] /updateOne', err);
    res.status(500).json({ error: err.message });
  }
});

// Batch Update Many
app.post('/updateMany', async (req, res) => {
  console.log('[POST] /updateMany - Payload:', req.body);
  try {
    await connectToMongo();
    const collectionName = req.body.collection || process.env.ATLAS_COLLECTION;
    const dynamicCollection = getCollection(collectionName);
    const updates = req.body.updates; // Array of { filter, update }
    const results = [];
    for (const { filter: rawFilter, update: rawUpdate } of updates) {
      const filter = normalizeIdInFilter(rawFilter);
      let update = rawUpdate;
      if (update && update.$set && update.$set._id) {
        // Remove _id from $set to avoid immutable field error
        const { _id, ...rest } = update.$set;
        update = { ...update, $set: rest };
      }
      const result = await dynamicCollection.updateOne(filter, update);
      results.push({ matchedCount: result.matchedCount, modifiedCount: result.modifiedCount });
      console.log('[RESULT] /updateMany - Single update result:', result);
    }
    console.log(`[SUCCESS] /updateMany - Updated ${results.length} documents`);
    res.json({ results });
  } catch (err) {
    console.error('[ERROR] /updateMany', err);
    res.status(500).json({ error: err.message });
  }
});

// Delete One
app.post('/deleteOne', async (req, res) => {
  console.log('[POST] /deleteOne - Payload:', req.body);
  try {
    await connectToMongo();
    const collectionName = req.body.collection || process.env.ATLAS_COLLECTION;
    const dynamicCollection = getCollection(collectionName);
    let filter = req.body.filter;
    filter = normalizeIdInFilter(filter);
    const result = await dynamicCollection.deleteOne(filter);
    console.log(`[SUCCESS] /deleteOne - DeletedCount: ${result.deletedCount}`);
    console.log('[RESULT] /deleteOne - MongoDB result:', result);
    res.json({ deletedCount: result.deletedCount });
  } catch (err) {
    console.error('[ERROR] /deleteOne', err);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Custom Data API Express server running on port ${PORT}`);
  // Start Atlas-to-local sync in background
  startAtlasSync();
});
