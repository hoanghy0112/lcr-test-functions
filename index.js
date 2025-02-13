/* eslint-disable object-curly-spacing */
/* eslint-disable no-tabs */
/* eslint-disable indent */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const express = require("express");
const Busboy = require("busboy");
const cors = require("cors");
const { Storage } = require("@google-cloud/storage");
const csv = require("csv-parser");
const { Pool } = require("pg");

const { defineString } = require("firebase-functions/params");
const databaseURL = defineString("DATABASE_URL");

admin.initializeApp();
const app = express();
const storage = new Storage();
const bucket = storage.bucket("hy-lcr-test");

const pool = new Pool({
	connectionString: databaseURL.value(),
	ssl: {
		rejectUnauthorized: false,
	},
});

app.use(cors()); // Default allows all origins (*)
app.use(express.json({ limit: "2000mb" }));
app.use(express.urlencoded({ limit: "2000mb", extended: true }));

app.get("/get-signed-url", async (req, res) => {
	try {
		const fileName = `${req.query.fileName
			.split(".")
			.slice(0, -1)
			.join(".")}_${new Date().getTime()}.${req.query.fileName
			.split(".")
			.at(-1)}`;
		const file = bucket.file(fileName);

		const [url] = await file.getSignedUrl({
			version: "v4",
			action: "write",
			expires: Date.now() + 15 * 60 * 1000, // URL valid for 15 min
			contentType: "application/octet-stream", // Accept any file type
		});

		res.json({ url, fileName });
	} catch (error) {
		res.status(500).json({ error });
	}
});

app.post("/save-to-db", async (req, res) => {
	try {
		const fileName = req.query.fileName;
		const file = bucket.file(fileName);

		const stream = file.createReadStream();
		const csvStream = csv();

		const client = await pool.connect();

		const results = [];

		stream
			.pipe(csvStream) // Parses CSV row by row
			.on("data", (row) => {
				results.push(row);
			})
			.on("end", async () => {
				console.log("CSV processing completed");
				await client.query(
					results
						.map(
							(v) =>
								`INSERT INTO transactions(account_id) VALUES (${v.id})`
						)
						.join(";")
				);
				res.json({ success: true, size: results.length });
			})
			.on("error", (error) => {
				console.error("Error reading CSV file:", error);
				res.status(500).json({ error: error.message });
			});
	} catch (error) {
		console.error("Error:", error);
		res.status(500).json({ error: error.message });
	}
});

app.post("/upload", async (req, res) => {
	try {
		if (req.method !== "POST") {
			return res.status(405).end();
		}

		const busboy = Busboy({ headers: req.headers });
		const uploads = [];

		// Handle file upload
		busboy.on("file", (fieldname, file, { filename, mimeType }) => {
			const filepath = `${Date.now()}_${filename}`;
			const fileUpload = bucket.file(filepath);

			const writeStream = fileUpload.createWriteStream({
				metadata: {
					contentType: mimeType,
				},
				resumable: false,
			});

			file
				.pipe(writeStream)
				.on("error", (error) => {
					console.error("Upload error:", error);
				})
				.on("finish", async () => {
					await fileUpload.makePublic();
					const results = {
						url: `https://storage.googleapis.com/${bucket.name}/${filepath}`,
						filepath,
					};

					const responseData = {
						message: "Upload successful",
						files: results,
						uploads,
					};

					res.json(responseData);
				});
		});
		// Handle completion
		// busboy.on("finish", async () => {
		// 	try {
		// 		const { filepath, fileUpload } = uploads;
		// 		await fileUpload.makePublic();
		// 		const results = {
		// 			url: `https://storage.googleapis.com/${bucket.name}/${filepath}`,
		// 			filepath,
		// 		};

		// 		const responseData = {
		// 			message: "Upload successful",
		// 			files: results,
		// 			uploads,
		// 		};

		// 		res.json(responseData);
		// 	} catch (error) {
		// 		console.error("Post-upload error:", error);
		// 		res.status(500).json({ error: "Upload processing failed" });
		// 	}
		// });

		// Handle the buffered data
		const buffer = req.rawBody; // For Firebase Functions
		// If rawBody isn't available, you might need to use:
		// const buffer = Buffer.from(req.body);

		busboy.end(buffer);
	} catch (error) {
		console.error("Upload error:", error);
		res.status(500).json({ error: "Upload failed" });
	}
});

exports.uploadFile = functions.https.onRequest(app);
