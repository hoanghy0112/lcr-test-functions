/* eslint-disable prefer-const */
/* eslint-disable operator-linebreak */
/* eslint-disable no-constant-condition */
/* eslint-disable max-len */
/* eslint-disable new-cap */
/* eslint-disable comma-dangle */
/* eslint-disable require-jsdoc */
/* eslint-disable object-curly-spacing */
/* eslint-disable no-tabs */
/* eslint-disable indent */
import functions from "firebase-functions";
import admin from "firebase-admin";
import express from "express";
import Busboy from "busboy";
import cors from "cors";
import { Storage } from "@google-cloud/storage";
import csv from "csv-parser";
import pg from "pg";
import { Connector } from "@google-cloud/cloud-sql-connector";
import dotenv from "dotenv";

import {
	saveBatchToDb,
	sendErrorEmail,
	getDbClient,
	convertValueInSQL,
	checkFileExists,
} from "./libs/utils.mjs";

import { RETRY_URL, MAX_SAVING_BATCH_SIZE } from "./libs/constants.mjs";

dotenv.config();

const { Pool } = pg;

const {
	RETOOL_DATABASE,
	CLOUD_SQL_DATABASE,
	DB_USER,
	DB_PASSWORD,
	DB_NAME,
	DB_CONNECTION_NAME,
} = process.env;

admin.initializeApp();
const app = express();
const storage = new Storage();
const bucketName = "lcr-uploaded-files";
const bucket = storage.bucket(bucketName);

const connector = new Connector();
const clientOpts = await connector.getOptions({
	instanceConnectionName: DB_CONNECTION_NAME,
	authType: "IAM",
});

const pool = new Pool({
	...clientOpts,
	user: DB_USER,
	password: DB_PASSWORD,
	database: DB_NAME,
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

async function saveToDB(inputData) {
	const client = await pool.connect();

	const clientFileName = inputData.clientFileName;
	const clientId = inputData.clientId;
	const fileName = inputData.fileName;
	const mappingConfig = inputData.mappingConfig;
	const additionalNotes = inputData.additionalNotes;
	const collectionFee = inputData.collectionFee;
	const chargebackFee = inputData.chargebackFee;
	const defaultBatchSize = inputData.batchSize || 1000;
	const startDate = inputData.startDate;
	const receiver = inputData.receiver;
	const timezone = inputData.timezone;
	const uploadedRows = inputData.uploadedRows ?? 0;

	const results = { total: uploadedRows, times: 0 };
	const status = { count: 0 };
	let BATCH_SIZE = defaultBatchSize;
	let batch = [];

	let returnData = null;

	try {
		console.time("Save uploading state");
		await client.query(`
			UPDATE uploading_files
			SET is_done = TRUE, saving_data = '${JSON.stringify(inputData)}'
			WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
		`);
		console.timeEnd("Save uploading state");

		console.time("Get GCS file");
		const isFileExists = await checkFileExists(bucketName, fileName);
		if (!isFileExists) {
			console.log("Storage error: fail to find filename");
			throw new Error(
				"Storage error: Fail to load the file from Google Cloud Storage (the file was deleted in GCS)"
			);
		}

		const file = bucket.file(fileName);
		console.timeEnd("Get GCS file");

		const stream = file.createReadStream();
		const csvStream = csv();
		console.log(`Start read file: ${fileName}`);
		console.log(`Batch size: ${BATCH_SIZE}`);

		stream
			.pipe(csvStream)
			.on("data", async (row) => {
				if (status.count < uploadedRows) {
					status.count += 1;
					return;
				}

				if (batch.length % 1000 === 0) {
					console.log(`1k batch rows: ${batch.length}`);
				}
				try {
					if (batch.length >= BATCH_SIZE) {
						console.log("Start to save a batch...");
						stream.pause();
						results.total += batch.length;
						results.times += 1;
						const copiedBatch = [...batch];
						batch = [];
						console.time("Save to batch");
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
						console.timeEnd("Save to batch");

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

						console.time("Check is pause");
						const { rows: uploadingFileRows } = await client.query(`
							SELECT is_pause FROM uploading_files
							WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
						`);
						const isPause = uploadingFileRows[0]?.is_pause;
						if (isPause) {
							stream.destroy();
							client.query("VACUUM ANALYZE transactions");
							return;
						}
						console.timeEnd("Check is pause");

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
						SET is_save_to_db_done = TRUE, uploaded_rows = max_rows
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
				returnData = { status: 200, success: true, size: results.total };
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
				returnData = { status: 500, error: error.message };
			});
	} catch (error) {
		console.error("Error:", error);
		await client.query(`
			UPDATE uploading_files
			SET error_msg = '${error?.message ?? JSON.stringify(error)}'
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
			reason: `[Unknown error] ${error?.message ?? ""}`,
		});
		client.release();
		returnData = { status: 500, error: error.message };
	}

	return returnData;
}

app.post("/retry-save-to-db", async (req, res) => {
	const client = await pool.connect();

	const clientFileName = req.body.clientFileName;
	const clientId = req.body.clientId;

	await client.query(`
		UPDATE uploading_files
		SET error_msg = NULL, uploaded_rows = 0, last_sent_notification_email = NULL, last_upload_at = NOW()
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	const result = await client.query(`
		SELECT * FROM uploading_files
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	const savingData = result.rows?.[0].saving_data;
	console.log({ type: typeof savingData, savingData });

	client.release();

	await saveToDB(savingData);

	return res.json({
		success: true,
	});
});

app.post("/continue-save-to-db", async (req, res) => {
	const client = await pool.connect();

	const clientFileName = req.body.clientFileName;
	const clientId = req.body.clientId;

	console.time("Update uploading files data");
	await client.query(`
		UPDATE uploading_files
		SET is_pause = FALSE, last_upload_at = NOW()
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	console.timeEnd("Update uploading files data");
	console.time("Get uploading data");
	const result = await client.query(`
		SELECT * FROM uploading_files
		WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
	`);
	console.timeEnd("Get uploading data");
	const savingData = result.rows?.[0].saving_data;
	const uploadedRows = parseInt(result.rows?.[0].uploaded_rows);

	console.log({ uploadedRows, data: savingData });

	client.release();

	await saveToDB({ ...savingData, uploadedRows });

	return res.json({
		success: true,
	});
});

app.post("/save-to-db", async (req, res) => {
	const responseData = await saveToDB(req.body);
	return res.status(responseData?.status ?? 500).json(responseData ?? {});
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

export const uploadFile = functions.https.onRequest(app);
