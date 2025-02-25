/* eslint-disable no-constant-condition */
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
const { writeFileSync } = require("fs");

require("dotenv").config();

const {
	saveBatchToDb,
	sendErrorEmail,
	getDbClient,
	convertValueInSQL,
} = require("./libs/utils");

const { DATABASE_URL, RETOOL_DATABASE, CLOUD_SQL_DATABASE } = process.env;

const { RETRY_URL, MAX_SAVING_BATCH_SIZE } = require("./libs/constants");

admin.initializeApp();
const app = express();
const storage = new Storage();
const bucket = storage.bucket("lcr-uploaded-files");

const pool = new Pool({
	connectionString: DATABASE_URL,
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

app.post("/update-uploading-state", async (req, res) => {
	const client = await pool.connect();

	const fileName = req.body.fileName;
	const clientId = req.body.clientId;

	await client.query(`
		UPDATE uploading_files
		SET last_upload_at = NOW()
		WHERE client_id = ${clientId} AND file_name = '${fileName}';
	`);

	client.release();

	return res.json({});
});

app.post("/retry-save-to-db", async (req, res) => {
	const client = await pool.connect();

	const clientFileName = req.body.clientFileName;
	const clientId = req.body.clientId;

	await client.query(`
		UPDATE uploading_files
		SET error_msg = NULL, uploaded_rows = 0, last_sent_notification_email = NULL 
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	const result = await client.query(`
		SELECT * FROM uploading_files
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	const savingData = result.rows?.[0].saving_data;
	console.log({ type: typeof savingData, savingData });

	// await client.query(`
	// 	DELETE FROM uploading_files
	// 	WHERE client_id = ${clientId} AND file_name = '${clientFileName}';

	// 	UPDATE uploading_files
	// 	SET error_msg = NULL, uploaded_rows = 0, is_save_to_db_done = FALSE
	// 	WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	// `);

	fetch(RETRY_URL, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(savingData),
	});

	client.release();

	return res.json({
		success: true,
	});
});

app.post("/continue-save-to-db", async (req, res) => {
	const client = await pool.connect();

	const clientFileName = req.body.clientFileName;
	const clientId = req.body.clientId;

	await client.query(`
		UPDATE uploading_files
		SET is_pause = FALSE
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	const result = await client.query(`
		SELECT * FROM uploading_files
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	const savingData = result.rows?.[0].saving_data;
	const uploadedRows = result.rows?.[0].uploaded_rows;

	fetch(RETRY_URL, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ ...savingData, uploadedRows }),
	});

	client.release();

	return res.json({
		success: true,
	});
});

app.post("/save-to-db", async (req, res) => {
	const client = await pool.connect();

	const clientFileName = req.body.clientFileName;
	const clientId = req.body.clientId;
	const fileName = req.body.fileName;
	const mappingConfig = req.body.mappingConfig;
	const additionalNotes = req.body.additionalNotes;
	const collectionFee = req.body.collectionFee;
	const chargebackFee = req.body.chargebackFee;
	const defaultBatchSize = req.body.batchSize || 1000;
	const startDate = req.body.startDate;
	const receiver = req.body.receiver;
	const timezone = req.body.timezone;
	const uploadedRows = req.body.uploadedRows ?? 0;

	try {
		await client.query(`
			UPDATE uploading_files
			SET is_done = TRUE, saving_data = '${JSON.stringify(req.body)}'
			WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
		`);

		const file = bucket.file(fileName);

		const stream = file.createReadStream();
		const csvStream = csv();

		const results = { total: 0 };
		const status = { count: 0 };
		let BATCH_SIZE = defaultBatchSize;
		let batch = [];

		stream
			.pipe(csvStream)
			.on("data", async (row) => {
				if (status.count < uploadedRows) {
					status.count += 1;
					return;
				}
				try {
					if (batch.length >= BATCH_SIZE) {
						stream.pause();
						results.total += batch.length;
						const copiedBatch = [...batch];
						batch = [];
						const newBatchSize = await saveBatchToDb(
							copiedBatch,
							mappingConfig,
							clientId,
							clientFileName,
							additionalNotes,
							collectionFee,
							chargebackFee,
							client,
							stream
						);
						// AIMD algorithm to optimize batch size
						if (newBatchSize === BATCH_SIZE) {
							BATCH_SIZE = Math.min(
								BATCH_SIZE * 1.2,
								MAX_SAVING_BATCH_SIZE
							);
						} else BATCH_SIZE = newBatchSize;

						console.log({
							total: results.total,
							clientId,
							clientFileName,
							BATCH_SIZE,
						});

						const { rows: uploadingFileRows } = await client.query(`
							SELECT * FROM uploading_files
							WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
						`);
						const isPause = uploadingFileRows[0]?.is_pause;
						if (isPause) {
							stream.destroy();
							return;
						}

						stream.resume();
					}
					batch.push(row);
				} catch (error) {
					await sendErrorEmail({
						filename: clientFileName,
						clientId,
						totalRows: results.total,
						doneAt: new Date().getTime(),
						createdAt: startDate,
						receiver,
						timezone,
						reason: `[Fail to reading CSV file] ${error}`,
					});
				}
			})
			.on("end", async () => {
				console.log("CSV processing completed");
				if (batch.length > 0) {
					results.total += batch.length;

					await saveBatchToDb(
						[...batch],
						mappingConfig,
						clientId,
						clientFileName,
						additionalNotes,
						collectionFee,
						chargebackFee,
						client,
						stream
					);
					batch = [];
				}

				const { rows: uploadingFileRows } = await client.query(`
					SELECT * FROM uploading_files
					WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
				`);
				const isPause = uploadingFileRows[0]?.is_pause;

				await client.query(`VACUUM ANALYZE transactions`);
				if (!isPause) {
					await client.query(`
						UPDATE uploading_files
						SET is_save_to_db_done = TRUE, max_rows = ${results.total}
						WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
					`);

					client.release();
					await fetch(
						"https://netpartnerservices.retool.com/url/send-email",
						{
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: JSON.stringify({
								filename: clientFileName,
								clientId,
								totalRows: results.total,
								doneAt: new Date().getTime(),
								createdAt: startDate,
								receiver,
								timezone,
							}),
						}
					);
				}
				res.json({ success: true, size: results.total });
			})
			.on("error", async (error) => {
				console.error("Error reading CSV file:", error);
				await sendErrorEmail({
					filename: clientFileName,
					clientId,
					totalRows: results.total,
					doneAt: new Date().getTime(),
					createdAt: startDate,
					receiver,
					timezone,
					reason: `[Fail to reading CSV file] ${error}`,
				});
				client.release();
				res.status(500).json({ error: error.message });
			});
	} catch (error) {
		console.error("Error:", error);
		await client.query(`
			UPDATE uploading_files
			SET error_msg = '${JSON.stringify(error)}'
			WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
		`);
		await sendErrorEmail({
			filename: clientFileName,
			clientId,
			totalRows: results.total,
			doneAt: new Date().getTime(),
			createdAt: startDate,
			receiver,
			timezone,
			reason: `[Unknown error] ${error}`,
		});
		client.release();
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

app.post("/migrate", async (req, res) => {
	const num = parseInt(req.query.num);
	const batch = parseInt(req.query.batch);
	const clientId = parseInt(req.query.clientId);

	const { client: cloudSqlClient, pool: cloudSqlPool } = await getDbClient(
		CLOUD_SQL_DATABASE
	);
	const { client: retoolClient, pool: retoolPool } = await getDbClient(
		RETOOL_DATABASE
	);
	console.log("Connect successfully...");

	try {
		const { rows } = clientId
			? await cloudSqlClient.query(`
				SELECT COUNT(*) AS current_num FROM transactions WHERE client_id = ${clientId};	
			`)
			: await cloudSqlClient.query(`
				SELECT * FROM migrate LIMIT 1;	
			`);
		const from = parseInt(rows[0].current_num);
		console.log(`From: ${from}, batch: ${batch}`);

		for (let i = 0; i < num; i++) {
			const queries = [];

			console.log(
				`Fetch with offset: ${
					from + i * batch
				} (${new Date().toTimeString()})`
			);
			const dataList = await retoolClient.query(`
				SELECT 
					*,
					TO_CHAR(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS iso_timestamp,
					TO_CHAR(default_date AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS iso_default_date
				FROM transactions
				${clientId ? `WHERE client_id = ${clientId}` : ""}
				ORDER BY id
				OFFSET ${from + i * batch}
				LIMIT ${batch}	
			`);
			console.log("Finish getting data list");

			for (let data of dataList.rows) {
				// console.log({
				// 	data: data.timestamp,
				// 	type: typeof data.timestamp,
				// 	string: new Date(data.timestamp)
				// 		.toISOString()
				// 		.split("T")
				// 		.join(" ")
				// 		.slice(0, 19),
				// });
				queries.push(`
				INSERT INTO transactions (
					account_id, order_id, service_offer, principal, collection_fee, chargeback_fee, outstanding_balance, 
					email, first_name, last_name, default_date, phone_number_1, address, address_2, city, state, zip, country, 
					ip_address, tracking_number, notes, card_number, product_terms_and_conditions, 
					global_terms_and_conditions, file_name, timestamp, client_id
				) VALUES (
					${convertValueInSQL(data.account_id)}, 
					${convertValueInSQL(data.order_id)}, 
					${convertValueInSQL(data.service_offer)}, 
					${convertValueInSQL(parseFloat(data.principal))}, 
					${convertValueInSQL(parseFloat(data.collection_fee))}, 
					${convertValueInSQL(parseFloat(data.chargeback_fee))}, 
					${convertValueInSQL(parseFloat(data.outstanding_balance))}, 
					${convertValueInSQL(data.email)}, 
					${convertValueInSQL(data.first_name)}, 
					${convertValueInSQL(data.last_name)}, 
					${data.iso_default_date ? `'${data.iso_default_date}'` : "'null'"}, 
					${convertValueInSQL(data.phone_number_1)}, 
					${convertValueInSQL(data.address)}, 
					${convertValueInSQL(data.address_2)}, 
					${convertValueInSQL(data.city)}, 
					${convertValueInSQL(data.state)}, 
					${convertValueInSQL(data.zip)}, 
					${convertValueInSQL(data.country)}, 
					${convertValueInSQL(data.ip_address)}, 
					${convertValueInSQL(data.tracking_number)}, 
					${convertValueInSQL(data.notes)}, 
					${convertValueInSQL(data.card_number)}, 
					${convertValueInSQL(data.product_terms_and_conditions)}, 
					${convertValueInSQL(data.global_terms_and_conditions)}, 
					${convertValueInSQL(data.file_name)}, 
					${data.iso_timestamp ? `'${data.iso_timestamp}'` : "'null'"}, 
					${convertValueInSQL(parseInt(data.client_id))}
				)
			`);
			}

			const queryString = queries.join(";");

			await cloudSqlClient.query(queryString);
			await cloudSqlClient.query(`
				UPDATE migrate
				SET current_num = ${from + i * batch + batch}
				WHERE id = 1;
			`);
			console.log("Finish save data list to db");
		}

		return res.json({ from });
	} catch (error) {
		console.log(error);
		retoolClient.release();
		cloudSqlClient.release();
		retoolPool.end();
		cloudSqlPool.end();
		return res.json({ error });
	}
});

process.on("SIGINT", async () => {
	await pool.end();
	process.exit(0);
});

exports.uploadFile = functions.https.onRequest(app);
