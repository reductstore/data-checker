import {Client, QuotaType} from 'reduct-js';
import crypto from 'crypto';
import md5 from 'md5';

const serverUrl = process.env.REDUCT_STORAGE_URL;
const apiToken = process.env.REDUCT_API_TOKEN;
const size30Gb = 30_000_000_000;
const entryName = 'test';

const clientReader = new Client(serverUrl, {apiToken: apiToken});
const clientWriter = new Client(serverUrl, {apiToken: apiToken});

const bigBlob = crypto.randomBytes(2 ** 20);

const sleep = ms => new Promise(r => setTimeout(r, ms));

const writer = async (bucket) => {
  while (true) {
    const ts = BigInt(Date.now()) * 1000n;
    const blob = bigBlob.slice(0,
        Math.round(Math.random() * (bigBlob.length - 1)));

    await bucket.write(entryName, Buffer.concat([blob, Buffer.from(md5(blob))]),
        ts);
    await sleep(100);
  }
};

const reader = async (bucket) => {
  await sleep(1000);
  while (true) {
    const entryInfo = await bucket.getEntryList();
    const info = entryInfo.find(entry => entry.name === entryName);

    console.info("query");
    for await (const record of bucket.query(entryName)) {
      const blob = await record.read();
      const expected = md5(blob.slice(0, blob.length - 32));
      const received =
          blob.slice(blob.length - 32).toString();
      if (expected !== received) {
        throw {
          message: 'Wrong MD5 sum',
          expected: expected,
          received: received,
          timestamp: recordList[i].timestamp.toString(),
        };
      }

    }

    await sleep(100);
  }
};

clientWriter.getOrCreateBucket('data',
    {quotaType: QuotaType.FIFO, quotaSize: size30Gb}).
    then(async (bucket) => {
      console.info('Run writer');
      await writer(bucket);
    }).
    catch((err) => {
      console.error('[ERROR] %s', err.original);
      process.exit(-1);
    });

clientReader.getBucket('data').
    then(async (bucket) => {
      console.info('Run reader');
      await reader(bucket);
    }).
    catch((err) => {
      console.error('[ERROR] %s', err.original);
      process.exit(-1);
    });
