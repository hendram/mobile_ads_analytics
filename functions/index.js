const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");
const { BigQuery } = require("@google-cloud/bigquery");

admin.initializeApp();
const db = admin.firestore();
const bigquery = new BigQuery();

const datasetId = "firestore_export"; // BigQuery dataset
const tableId = "car1collection";     // BigQuery table name

exports.syncCar1CollectionToBQ = functions.firestore
  .document("car1collection/{docId}")
  .onWrite(async (change, context) => {
    const docId = context.params.docId;
    console.log("Triggered for document:", docId);

    // =============== CASE 1: Document Deleted ===============
    if (!change.after.exists) {
      console.log(`[DELETE] Firestore doc deleted: ${docId}`);
      try {
        const query = `
          DELETE FROM \`${datasetId}.${tableId}\`
          WHERE Id = @docId
        `;
        const options = { query, params: { docId } };
        const [job] = await bigquery.query(options);
        console.log(`[DELETE] BigQuery delete completed for: ${docId}`);
      } catch (err) {
        console.error(`[DELETE ERROR] Failed to delete from BigQuery for ${docId}:`, err);
      }
      return null;
    }

    // =============== CASE 2: Document Created or Updated ===============
    const data = change.after.data();

    // Prepare BigQuery row
    const row = {
      Id: docId, // Must match your BQ schema exactly (case sensitive)
      carId: data.carId || null,
      lat: data.lat ?? null,
      lng: data.lng ?? null,
      timestamp: data.timestamp
        ? new Date(data.timestamp)
        : new Date(), // ensure valid timestamp
    };

    console.log("[UPSERT] Prepared row for BigQuery:", JSON.stringify(row));

    try {
      await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert([row], { ignoreUnknownValues: true });
      console.log(`[UPSERT] Successfully inserted/updated row in BigQuery for: ${docId}`);
    } catch (err) {
      // Show full context of error
      console.error(`[UPSERT ERROR] Failed to insert/update BigQuery for ${docId}:`, err);

      // If PartialFailureError, print failed rows for debugging
      if (err.name === "PartialFailureError" && err.errors) {
        err.errors.forEach(e => {
          console.error("Row insert error details:", JSON.stringify(e));
        });
      }
    }

    return null;
  });
