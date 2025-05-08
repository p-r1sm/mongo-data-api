require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');

const app = express();
app.use(express.json());
app.use(cors());

const {
  ATLAS_URI,
  ATLAS_DB,
  ATLAS_COLLECTION
} = process.env;

let client, collection;

async function connectToMongo() {
  if (!client) {
    client = new MongoClient(ATLAS_URI);
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
    const doc = req.body.document;
    const result = await collection.insertOne(doc);
    console.log('[SUCCESS] /insertOne - InsertedId:', result.insertedId);
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
    const filter = req.body.filter || {};
    const docs = await collection.find(filter).toArray();
    console.log(`[SUCCESS] /find - Returned ${docs.length} documents`);
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
    const filter = req.body.filter;
    const update = req.body.update;
    const result = await collection.updateOne(filter, update);
    console.log(`[SUCCESS] /updateOne - Matched: ${result.matchedCount}, Modified: ${result.modifiedCount}`);
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
    const updates = req.body.updates; // Array of { filter, update }
    const results = [];
    for (const { filter, update } of updates) {
      const result = await collection.updateOne(filter, update);
      results.push({ matchedCount: result.matchedCount, modifiedCount: result.modifiedCount });
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
    const filter = req.body.filter;
    const result = await collection.deleteOne(filter);
    console.log(`[SUCCESS] /deleteOne - DeletedCount: ${result.deletedCount}`);
    res.json({ deletedCount: result.deletedCount });
  } catch (err) {
    console.error('[ERROR] /deleteOne', err);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Custom Data API Express server running on port ${PORT}`);
});
