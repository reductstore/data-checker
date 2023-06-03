import {Client, QuotaType} from 'reduct-js';
import crypto from 'crypto';
import md5 from 'md5';

const serverUrl = process.env.REDUCT_STORAGE_URL;
const apiToken = process.env.REDUCT_API_TOKEN;
const size30Gb = 30_000_000_000;
const entryName = 'test-entry';
const intervalMs = process.env.TIME_INTERVAL ? process.env.TIME_INTERVAL : 1000;

console.log(`Server URL ${serverUrl}`);

const clientReader = new Client(serverUrl, {apiToken: apiToken});
const clientWriter = new Client(serverUrl, {apiToken: apiToken});

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
        await sleep(intervalMs - (Date.now() - now));
    }
};

const reader = async (bucket) => {
    await sleep(1000);
    while (true) {
        const entryInfo = await bucket.getEntryList();
        const info = entryInfo.find(entry => entry.name === entryName);

        console.info('query');
        for await (const record of bucket.query(entryName)) {
            const now = Date.now();
            const blob = await record.read();
            if (md5(blob) !== record.labels.md5) {
                throw {
                    message: 'Wrong MD5 sum',
                    expected: record.labels.md5,
                    received: md5(blob),
                    timestamp: record.time,
                };
            }
            await sleep(intervalMs * 0.8 - (Date.now() - now)); // be faster than writer
        }

    }
};

console.log(`IO interval ${intervalMs} ms`);

clientWriter.getOrCreateBucket('stress_test',
    {quotaType: QuotaType.FIFO, quotaSize: size30Gb}).then(async (bucket) => {
    console.info('Run writer');
    await writer(bucket);
}).catch((err) => {
    console.error('[ERROR] %s', err);
    process.exit(-1);
});

clientReader.getBucket('stress_test').then(async (bucket) => {
    console.info('Run reader');
    await reader(bucket);
}).catch((err) => {
    console.error('[ERROR] %s', err);
    process.exit(-1);
});
