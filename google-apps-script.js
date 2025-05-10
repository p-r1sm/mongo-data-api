// CONFIGURATION
const API_BASE = 'https://mongo-data-api-v1g7.onrender.com'; // Change to your deployed API URL if needed

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
  const collection = sheet.getName();
  // Ensure the collection exists (create if not)
  apiPost('/ensureCollection', { collection });
  const result = apiPost('/find', { collection });

  // Clear and set headers
  sheet.clear();
  if (result.documents.length === 0) return;

  const headers = Object.keys(result.documents[0]);
  // Build all rows for batch write
  const allRows = [headers];
  result.documents.forEach(doc => {
    allRows.push(headers.map(h => {
      const val = doc[h];
      // Dates (MongoDB's $date or JS Date)
      if (val && typeof val === 'object' && val.$date) return new Date(val.$date).toISOString();
      if (val instanceof Date) return val.toISOString();
      // Arrays/Objects (but not null)
      if (val && typeof val === 'object') return JSON.stringify(val);
      return val;
    }));
  });
  sheet.getRange(1, 1, allRows.length, headers.length).setValues(allRows);
}

// 2. Push changes from sheet to MongoDB (batch updates)
function pushToMongo() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const collection = sheet.getName();
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const updates = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const doc = {};
    headers.forEach((h, idx) => {
      let value = row[idx];
      // Try to parse JSON for arrays/objects
      if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
        try {
          value = JSON.parse(value);
        } catch (e) {}
      }
      // Convert ISO date strings to Date objects
      if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{3})?Z$/.test(value)) {
        try {
          value = new Date(value);
        } catch (e) {}
      }
      doc[h] = value;
    });
    if (doc._id) {
      // Prepare update statement for this doc
      const docCopy = { ...doc };
      delete docCopy._id; // Remove _id from update doc
      updates.push({
        filter: { _id: doc._id },
        update: { $set: docCopy }
      });
    } else {
      // Insert
      if (doc._id === '' || doc._id == null) delete doc._id; // Remove _id if empty
      const res = apiPost('/insertOne', { collection, document: doc });
      sheet.getRange(i + 1, headers.indexOf('_id') + 1).setValue(res.insertedId); // Set new _id in sheet
    }
  }
  // Batch update
  if (updates.length > 0) {
    apiPost('/updateMany', { collection, updates });
  }
}

// 3. Delete selected row from MongoDB and sheet
function deleteSelectedRow() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const row = sheet.getActiveRange().getRow();
  if (row < 2) {
    SpreadsheetApp.getUi().alert('Cannot delete the header or an invalid row.');
    return;
  }

  // Get the _id from column 1
  const _id = sheet.getRange(row, 1).getValue();
  if (!_id) {
    SpreadsheetApp.getUi().alert('No ID found in the selected row.');
    return;
  }

  // Confirmation popup
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert('Are you sure you want to delete this row?', ui.ButtonSet.YES_NO);
  if (response !== ui.Button.YES) {
    ui.alert('Deletion cancelled.');
    return;
  }

  try {
    // Delete from MongoDB first
    const collection = sheet.getName();
    const delResponse = apiPost('/deleteOne', { collection, filter: { _id: _id } });
    if (delResponse && delResponse.deletedCount === 1) {
      sheet.deleteRow(row);
      ui.alert('Row deleted from MongoDB and sheet.');
    } else {
      ui.alert('Failed to delete from MongoDB. Row not deleted from sheet.');
    }
  } catch (e) {
    ui.alert('Error deleting row: ' + e.message);
  }
}

// 4. Add custom menu for ease of use
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('MongoDB Sync')
    .addItem('Pull from MongoDB', 'pullFromMongo')
    .addItem('Push changes to MongoDB', 'pushToMongo')
    .addItem('Delete selected row', 'deleteSelectedRow')
    .addToUi();

  // Add a User Guide sheet if not present
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const guideName = 'User Guide';
  let guideSheet = ss.getSheetByName(guideName);
  if (!guideSheet) {
    guideSheet = ss.insertSheet(guideName, 0);
    guideSheet.getRange(1, 1, 11, 3).setValues([
      ['MongoDB Google Sheets Sync - User Guide', '', 'Sample Value'],
      ['Editing Arrays/Objects:', 'Enter valid JSON (e.g., [1,2,3] or {"a":1}) in the cell.', '[1,2,3] or {"foo":42}'],
      ['Editing Dates:', 'Enter date as ISO string (e.g., 2024-05-10T12:00:00.000Z).', '2024-05-10T12:00:00.000Z'],
      ['Editing Primitives:', 'Edit numbers, strings, booleans directly.', '42, hello, true'],
      ['Adding Rows:', 'Add a new row at the bottom. Leave _id blank for new docs.', ''],
      ['Deleting Rows:', 'Select a row and use the menu. Confirmation required.', ''],
      ['Syncing:', 'Use the custom menu to pull/push data.', ''],
      ['Notes:', 'Malformed JSON will be ignored. Dates must be valid ISO format.', '{"a":1}, [2,3], 2024-05-10T12:00:00.000Z'],
      ['Best Practice:', 'Double-check JSON syntax when editing arrays/objects.', ''],
      ['Need Help?', 'Contact your admin or developer.', '']
    ]);
    guideSheet.setColumnWidths(1, 3, 320);
    guideSheet.setFrozenRows(1);
  }
}
