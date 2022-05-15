import {Client, QuotaType} from 'reduct-js';
import crypto from 'crypto';
import md5 from 'md5';

const serverUrl = process.env.REDUCT_STORAGE_URL;
const size30Gb = 32212254720n;

const client = new Client(serverUrl);
const bigBlob = crypto.randomBytes(2 ** 20);

const sleep = ms => new Promise(r => setTimeout(r, ms));

const writer = async (bucket) => {
  while (true) {
    const ts = BigInt(Date.now()) * 1000n;
    const blob = bigBlob.slice(0,
        Math.round(Math.random() * (bigBlob.length - 1)));

    await bucket.write('test', Buffer.concat([blob, Buffer.from(md5(blob))]),
        ts);
    await sleep(100);
  }
};

const reader = async (bucket) => {
  await sleep(1000);
  while (true) {
    const entryInfo = await bucket.getEntryList();
    const info = entryInfo.find(entry => entry.name === 'test');

    const recordList = await bucket.list('test',
        info.latestRecord - 3_600_000_000n,
        info.latestRecord);
    for (let i = 0; i < recordList.length; ++i) {
      const blob = await bucket.read('test', recordList[i].timestamp);
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

client.getOrCreateBucket('test-bucket',
    {quotaType: QuotaType.FIFO, quotaSize: size30Gb}).
    then(async (bucket) => {
      console.info("Run checker");
      await Promise.all([writer(bucket), reader(bucket)]);
    }).
    catch((err) => {
      console.error('[ERROR] %s', JSON.stringify(err));
      process.exit(-1);
    });
