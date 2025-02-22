/* eslint-disable max-len */
/* eslint-disable comma-dangle */
/* eslint-disable no-undef */
/* eslint-disable no-tabs */
/* eslint-disable indent */
/* eslint-disable require-jsdoc */
/* eslint-disable object-curly-spacing */
const { fields } = require("./constants");
const { Pool } = require("pg");

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
	} else if (isNaN(value)) {
		return "null";
	} else {
		return `'${value.replaceAll("'", "''")}'`;
	}
}

async function saveBatchToDb(
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

async function sendErrorEmail(data) {
	return await fetch(
		"https://netpartnerservices.retool.com/url/send-fail-to-save",
		{
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(data),
		}
	);
}

async function getDbClient(url) {
	const pool = new Pool({
		connectionString: url,
		ssl: {
			rejectUnauthorized: false,
		},
	});
	return await pool.connect();
}

module.exports = {
	getDbClient,
	sendErrorEmail,
	saveBatchToDb,
	convertValueInSQL,
};
