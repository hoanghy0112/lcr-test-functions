const functions = require("firebase-functions");
const admin = require("firebase-admin");
const express = require("express");
const Busboy = require('busboy');
const { Storage } = require("@google-cloud/storage");

admin.initializeApp();
const app = express();
const storage = new Storage();
const bucket = storage.bucket("hy-lcr-test");

app.post("/upload", async (req, res) => {
  try {
    if (req.method !== "POST") {
      return res.status(405).end();
    }

    const busboy = Busboy({ headers: req.headers });
    const uploads = [];

    // Handle file upload
    busboy.on('file', (fieldname, file, { filename, mimeType }) => {
      const filepath = `uploads/${Date.now()}_${filename}`;
      const fileUpload = bucket.file(filepath);

      const writeStream = fileUpload.createWriteStream({
        metadata: {
          contentType: mimeType
        },
        resumable: false
      });

      file.pipe(writeStream)
        .on('error', (error) => {
          console.error('Upload error:', error);
        })
        .on('finish', () => {
          uploads.push({ filepath, fileUpload });
        });
    });

    // Handle completion
    busboy.on('finish', async () => {
      try {
        const results = await Promise.all(
          uploads.map(async ({ filepath, fileUpload }) => {
            await fileUpload.makePublic();
            return {
              url: `https://storage.googleapis.com/${bucket.name}/${filepath}`,
              filepath
            };
          })
        );

        res.json({
          message: "Upload successful",
          files: results
        });
      } catch (error) {
        console.error('Post-upload error:', error);
        res.status(500).json({ error: "Upload processing failed" });
      }
    });

    // Handle the buffered data
    const buffer = req.rawBody;  // For Firebase Functions
    // If rawBody isn't available, you might need to use:
    // const buffer = Buffer.from(req.body);
    
    busboy.end(buffer);

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: "Upload failed" });
  }
});

exports.uploadFile = functions.https.onRequest(app);