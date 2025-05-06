// CONFIGURATION
const API_BASE = 'http://localhost:3000'; // Change to your deployed API URL if needed

// Utility: HTTP POST
function apiPost(endpoint, payload) {
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  const response = UrlFetchApp.fetch(API_BASE + endpoint, options);
  return JSON.parse(response.getContentText());
}

// 1. Pull all docs from MongoDB and populate the sheet
function pullFromMongo() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const result = apiPost('/find', {});

  // Clear and set headers
  sheet.clear();
  if (result.documents.length === 0) return;

  const headers = Object.keys(result.documents[0]);
  sheet.appendRow(headers);

  // Add rows
  result.documents.forEach(doc => {
    sheet.appendRow(headers.map(h => doc[h]));
  });
}

// 2. Push changes from sheet to MongoDB (batch updates)
function pushToMongo() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const updates = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const doc = {};
    headers.forEach((h, idx) => doc[h] = row[idx]);
    if (doc.id) {
      // Prepare update statement for this doc
      updates.push({
        filter: { id: doc.id },
        update: { $set: doc }
      });
    } else {
      // Insert
      const res = apiPost('/insertOne', { document: doc });
      sheet.getRange(i + 1, headers.indexOf('id') + 1).setValue(res.id); // Set new id in sheet
    }
  }
  // Batch update
  if (updates.length > 0) {
    apiPost('/updateMany', { updates });
  }
}

// 3. Delete selected row from MongoDB and sheet
function deleteSelectedRow() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const row = sheet.getActiveRange().getRow();
  if (row === 1) return; // Don't delete header

  const id = sheet.getRange(row, 1).getValue();
  if (id) {
    apiPost('/deleteOne', { filter: { id: id } });
  }
  sheet.deleteRow(row);
}

// 4. Add custom menu for ease of use
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('MongoDB Sync')
    .addItem('Pull from MongoDB', 'pullFromMongo')
    .addItem('Push changes to MongoDB', 'pushToMongo')
    .addItem('Delete selected row', 'deleteSelectedRow')
    .addToUi();
}
