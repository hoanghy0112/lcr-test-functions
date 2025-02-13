/* eslint-disable max-len */
/* eslint-disable new-cap */
/* eslint-disable comma-dangle */
/* eslint-disable require-jsdoc */
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

const fields = [
	"account_id",
	"order_id",
	"email",
	"service_offer",
	"principal",
	"first_name",
	"last_name",
	"address",
	"address_2",
	"city",
	"state",
	"zip",
	"country",
	"default_date",
	"phone_number_1",
	"ip_address",
	"tracking_number",
	"notes",
	"card_number",
];

function isNumber(value) {
	return typeof value === "number" && !isNaN(value);
}

const parseToFloat = (value) => {
	if (typeof value === "string") {
		let cleanedStr = value.replace(/[^0-9,.]/g, "");

		if (cleanedStr.includes(",") && cleanedStr.includes(".")) {
			cleanedStr = cleanedStr.replace(/,/g, "");
		} else {
			cleanedStr = cleanedStr.replace(/,/g, ".");
		}

		const number = parseFloat(cleanedStr);

		return isNaN(number) ? null : number;
	}
	return typeof value === "number" ? value : 0;
};

function convertValueInSQL(value) {
	if (value === null || value === undefined) {
		return "null";
	}
	if (isNumber(value)) {
		return value || 0;
	} else {
		return `'${value.replaceAll("'", '"')}'`;
	}
}

function getSQLQuery(
	batch,
	mappingConfig,
	clientId,
	clientFileName,
	additionalNotes,
	collectionFee,
	chargebackFee
) {
	const insertData = batch.map((item) => {
		return fields.map((field) => item[mappingConfig[field]] || null);
	});
	const placeholders = insertData
		.map(
			(rowData, rowIndex) =>
				`(${rowData
					.map((d, i) =>
						convertValueInSQL(
							fields[i] === "principal" ? parseToFloat(d) : d
						)
					)
					.join(",")}, ${clientId}, ${convertValueInSQL(
					clientFileName
				)}, ${convertValueInSQL(additionalNotes)}, ${convertValueInSQL(
					collectionFee
				)}, ${convertValueInSQL(chargebackFee)})`
		)
		.join(",");

	let query = `INSERT INTO transactions (${fields.join(
		","
	)}, client_id, file_name, global_terms_and_conditions, collection_fee, chargeback_fee) VALUES ${placeholders};`;
	query += `
	UPDATE uploading_files
	SET uploaded_rows = uploaded_rows + ${batch.length}
	WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`;
	return query;
}

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
		const fileName = req.body.fileName;
		const clientFileName = req.body.clientFileName;
		const clientId = req.body.clientId;
		const mappingConfig = req.body.mappingConfig;
		const additionalNotes = req.body.additionalNotes;
		const collectionFee = req.body.collectionFee;
		const chargebackFee = req.body.chargebackFee;
		const defaultBatchSize = req.body.batchSize || 1000;

		const file = bucket.file(fileName);

		const stream = file.createReadStream();
		const csvStream = csv();

		const client = await pool.connect();

		const results = { total: 0 };
		const BATCH_SIZE = defaultBatchSize;
		let batch = [];
		const promises = [];

		stream
			.pipe(csvStream)
			.on("data", async (row) => {
				batch.push(row);
				if (batch.length >= BATCH_SIZE) {
					results.total += batch.length;
					const query = getSQLQuery(
						batch,
						mappingConfig,
						clientId,
						clientFileName,
						additionalNotes,
						collectionFee,
						chargebackFee
					);
					batch = [];
					console.log({ total: results.total });
					// const promise = client.query(query);
					// promises.push(promise);
					// await promise;
					// promises.splice(promises.indexOf(promise), 1);
					client.query(query);
				}
			})
			.on("end", async () => {
				console.log("CSV processing completed");
				if (batch.length > 0) {
					results.total += batch.length;
					const query = getSQLQuery(
						batch,
						mappingConfig,
						clientId,
						clientFileName,
						additionalNotes,
						collectionFee,
						chargebackFee
					);
					batch = [];
					// const promise = client.query(query);
					// promises.push(promise);
					// await promise;
					client.query(query);
				}
				await Promise.all(promises);
				await client.query(`
					UPDATE uploading_files
					SET is_save_to_db_done = TRUE, max_rows = ${results.total}
					WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
				`);
				res.json({ success: true, size: results.total });
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
