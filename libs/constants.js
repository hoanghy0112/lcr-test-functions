/* eslint-disable max-len */
/* eslint-disable comma-dangle */
/* eslint-disable no-undef */
/* eslint-disable no-tabs */
/* eslint-disable indent */
/* eslint-disable require-jsdoc */
/* eslint-disable object-curly-spacing */
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
const RETRY_URL =
	"https://us-central1-valiant-pager-451605-v0.cloudfunctions.net/uploadFile/save-to-db";
const MAX_SAVING_BATCH_SIZE = 25000;

module.exports = {
	fields,
	RETRY_URL,
	MAX_SAVING_BATCH_SIZE,
};
