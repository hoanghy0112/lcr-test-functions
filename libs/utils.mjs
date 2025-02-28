/* eslint-disable max-len */
/* eslint-disable comma-dangle */
/* eslint-disable no-undef */
/* eslint-disable no-tabs */
/* eslint-disable indent */
/* eslint-disable require-jsdoc */
/* eslint-disable object-curly-spacing */
import { fields } from "./constants.mjs";
import pg from "pg";
import { Storage } from "@google-cloud/storage";
import { Readable } from "stream";

const storage = new Storage();

const { Pool } = pg;

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

export function convertValueInSQL(value) {
	if (value === null || value === undefined) {
		return "null";
	}
	if (isNumber(value)) {
		return value || 0;
	} else if (isNaN(value) && typeof value === "number") {
		return "null";
	} else {
		return `'${value.replaceAll("'", "''")}'`;
	}
}

export async function saveBatchToDb(
	batch,
	mappingConfig,
	clientId,
	clientFileName,
	additionalNotes,
	collectionFee,
	chargebackFee,
	client,
	stream
) {
	// const stream = client.query(copyFrom('COPY test (title, description) FROM STDIN WITH (FORMAT text, DELIMITER \';\')'));

	const query = getSQLQuery(
		batch,
		mappingConfig,
		clientId,
		clientFileName,
		additionalNotes,
		collectionFee,
		chargebackFee
	);
	console.log({ size: batch.length, clientId, clientFileName });

	try {
		await client.query(query);
		return batch.length;
	} catch (error) {
		console.log({ error });
		if (error.code != "53200") {
			await client.query(`
                UPDATE uploading_files
                SET error_msg = '${JSON.stringify(error)}'
                WHERE client_id = ${clientId} AND file_name = '${clientFileName}';
            `);
			throw error;
		}
		const leftBatch = batch.slice(0, Math.floor(batch.length / 2));
		const rightBatch = batch.slice(Math.floor(batch.length / 2));

		const leftBatchSize = await saveBatchToDb(
			leftBatch,
			mappingConfig,
			clientId,
			clientFileName,
			additionalNotes,
			collectionFee,
			chargebackFee,
			client,
			stream
		);
		const rightBatchSize = await saveBatchToDb(
			rightBatch,
			mappingConfig,
			clientId,
			clientFileName,
			additionalNotes,
			collectionFee,
			chargebackFee,
			client,
			stream
		);

		return Math.min(leftBatchSize, rightBatchSize);
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

export async function sendErrorEmail(data) {
	return await fetch(
		"https://netpartnerservices.retool.com/url/send-fail-to-save",
		{
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(data),
		}
	);
}

export async function getDbClient(url) {
	console.log({ url });
	const pool = new Pool({
		connectionString: url,
		ssl: {
			rejectUnauthorized: false,
		},
	});
	try {
		const client = await pool.connect();
		return { client, pool };
	} catch (error) {
		pool.end();
		throw error;
	}
}

export async function checkFileExists(bucketName, fileName) {
	try {
		const [metadata] = await storage
			.bucket(bucketName)
			.file(fileName)
			.getMetadata();
		return true;
	} catch (error) {
		if (error.code === 404) {
			return false;
		} else {
			throw new Error("Error checking file:", error);
		}
	}
}
