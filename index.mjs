import {Client} from 'reduct-js';
import crypto from 'crypto';
import md5 from 'md5';
import consoleStamp from "console-stamp";

consoleStamp(console, 'HH:MM:ss.l');

const serverUrl = process.env.REDUCT_STORAGE_URL;
const apiToken = process.env.REDUCT_API_TOKEN;
const entryName = process.env.REDUCT_ENTRY_NAME ? process.env.REDUCT_ENTRY_NAME : 'test-entry';
const role = process.env.REDUCT_ROLE ? process.env.REDUCT_ROLE : 'reader';
const intervalMs = process.env.TIME_INTERVAL ? process.env.TIME_INTERVAL : 1000;

console.log(`Server URL ${serverUrl}`);

const client = new Client(serverUrl, {apiToken: apiToken, timeout: 5000});
const bigBlob = crypto.randomBytes(2 ** 20);

const sleep = ms => new Promise(r => setTimeout(r, ms));

const writer = async (bucket) => {
    while (true) {
        const now = Date.now();
        const blob = bigBlob.slice(0,
            Math.round(Math.random() * (bigBlob.length - 1)));
        let size = "medium";
        if (blob.length > bigBlob.length * 0.66) {
            size = "big";
        }
        if (blob.length < bigBlob.length * 0.33) {
            size = "small";
        }

        const record = await bucket.beginWrite(entryName, {labels: {md5: md5(blob), size: size}});
        await record.write(blob);
        console.log(`Write ${blob.length} bytes`);
        await sleep(intervalMs - (Date.now() - now));
    }
};

const reader = async (bucket) => {
    await sleep(1000);
    while (true) {
        const entryInfo = await bucket.getEntryList();
        let entry = entryInfo.find(entry => entry.name === entryName);
        console.info('query');
        const now = Date.now();
        console.log(`start query ${entry.latestRecord - 10_000_000n}`)
        for await (const record of bucket.query(entryName, entry.latestRecord - 10_000_000n, undefined, {limit: 5})) {
            const blob = await record.read();
            if (md5(blob) !== record.labels.md5) {
                throw {
                    message: 'Wrong MD5 sum',
                    expected: record.labels.md5,
                    received: md5(blob),
                    timestamp: record.time,
                };
            }
            console.info(`Read ${blob.length} bytes`);
        }

        await sleep(intervalMs - (Date.now() - now));

    }
};

console.log(`IO interval ${intervalMs} ms`);

if (role !== 'reader') {
    client.getBucket('stress_test').then(async (bucket) => {
        console.info('Run writer');
        await writer(bucket);
    }).catch((err) => {
        console.error('WRITER %s', err);
        process.exit(-1);
    });
}

if (role === 'reader') {
    client.getBucket('stress_test').then(async (bucket) => {
        console.info('Run reader');
        await reader(bucket);
    }).catch((err) => {
        console.error('READER %s', err);
        process.exit(-1);
    });
}