/* eslint-disable max-len */
/* eslint-disable comma-dangle */
/* eslint-disable no-undef */
/* eslint-disable no-tabs */
/* eslint-disable indent */
/* eslint-disable require-jsdoc */
/* eslint-disable object-curly-spacing */
export const fields = [
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
export const RETRY_URL =
	"https://us-central1-valiant-pager-451605-v0.cloudfunctions.net/uploadFile/save-to-db";
export const MAX_SAVING_BATCH_SIZE = 30000;
