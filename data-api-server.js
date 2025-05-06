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
  try {
    await connectToMongo();
    const doc = req.body.document;
    const result = await collection.insertOne(doc);
    res.json({ insertedId: result.insertedId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Find
app.post('/find', async (req, res) => {
  try {
    await connectToMongo();
    const filter = req.body.filter || {};
    const docs = await collection.find(filter).toArray();
    res.json({ documents: docs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Update One
app.post('/updateOne', async (req, res) => {
  try {
    await connectToMongo();
    const filter = req.body.filter;
    const update = req.body.update;
    const result = await collection.updateOne(filter, update);
    res.json({ matchedCount: result.matchedCount, modifiedCount: result.modifiedCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete One
app.post('/deleteOne', async (req, res) => {
  try {
    await connectToMongo();
    const filter = req.body.filter;
    const result = await collection.deleteOne(filter);
    res.json({ deletedCount: result.deletedCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Custom Data API Express server running on port ${PORT}`);
});
