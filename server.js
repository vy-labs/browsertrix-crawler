import child_process from "child_process";
import yaml from "js-yaml";
import fs from "fs";
import md5 from "md5";
import { v4 as uuidv4 } from "uuid";
import {initBroadCrawlRedis} from "./util/broadCrawlRedis.js";
import {logger} from "./util/logger.js";
import {sleep} from "./util/timing.js";
import {RedisHelper} from "./util/redisHelper.js";
import {fetchInstanceId} from "./util/ec2Util.js";
import { promisify } from 'util';

let crawlProcess = null;
let fixedArgs = createArgsFromYAML();
const MAX_REDIS_TRIES = 3;
const EVENT_QUEUE = "test_queue:start_urls";

(async function() {
  const redisHelper = await getBroadCrawlRedisHelper();
  const redisClient = redisHelper.redisClient();
  const acquireLockAsync = promisify(redisClient.set).bind(redisClient);
  const releaseLockAsync = promisify(redisClient.del).bind(redisClient);
  const getLockAsync = promisify(redisClient.get).bind(redisClient);


  async function acquireLock(lockName, timeout) {
    const identifier = Math.random().toString(36).slice(2);
    const lockKey = `lock:${lockName}`;
    const lockTimeout = parseInt(timeout);

    console.log("trying to acquire lock")
    const result = await acquireLockAsync(lockKey, identifier, 'NX', 'EX', lockTimeout);
    if (result === 'OK') {
      return identifier;
    } else {
      return null;
    }
  }

  async function releaseLock(lockName, identifier) {
    const lockKey = `lock:${lockName}`;
    const result = await getLockAsync(lockKey);

    if (result === identifier) {
      await releaseLockAsync(lockKey);
      return true;
    } else {
      return false;
    }
  }

  while(true){
    let event = null;
    try {
      const identifier = await acquireLock('myLock', 120);
      if (identifier) {
        console.log('Lock acquired with identifier:', identifier);
        try {
          event = JSON.parse(await redisHelper.getEventFromQueue(EVENT_QUEUE));
          if(event === null) {
            process.exit(1);
          }
        } catch (err) {
          console.log(err.message);
          continue;
        }
        const released = await releaseLock('myLock', identifier);
        if (released) {
          console.log('Lock released');
        } else {
          console.error('Failed to release lock');
        }
      } else {
        console.error('Failed to acquire lock');
      }
    } catch (err) {
      console.error('Error:', err);
    }

    if(event === null){
      continue;
    }

    const url = event.url;
    const level = event.level;
    const domain = event.domain;
    const retry = event.retry || 0;
    const collection = md5(url);
    const crawlId = uuidv4();
    console.log("preparing to crawl")

    await redisHelper.pushEventToQueue("crawlStatus",JSON.stringify({
      url: url,
      event: "CRAWL_PROCESSING",
      domain: domain,
      level: level,
      retry: retry,
      crawlId: crawlId,
      instance_id: fetchInstanceId() || "dev-testing"
    }));

    const args = [
      "--url", url,
      "--domain", domain,
      "--level", level,
      "--collection", String(collection),
      "--id", String(crawlId),
      "--retry", retry
    ];
    args.push(...fixedArgs);
    crawlProcess = child_process.spawnSync("crawl", args, {stdio: "inherit"});
    const status = crawlProcess.status;
    if(status === 0){
      console.log(`crawl success, url: ${url} domain: ${domain}`);
    } else {
      console.log(`crawl failed, url: ${url} domain: ${domain}`);
    }
  }
})();

// Handle SIGTSTP signal (Ctrl+Z)
process.on("SIGTSTP", () => {
  if (crawlProcess) {
    crawlProcess.kill();
  }
  process.exit(0);
});

function createArgsFromYAML(){
  // Parse the YAML content
  const data = yaml.load(fs.readFileSync("/app/config.yaml", "utf8"));
  let args = [];
  // Iterate through each key-value pair
  Object.entries(data.server).forEach(([key, value]) => {
    args.push(`--${key}`, value);
  });
  return args;
}

async function getBroadCrawlRedisHelper(){
  let broadCrawlRedis;
  let redis_tries = 0;
  while (true) {
    try {
      broadCrawlRedis = await initBroadCrawlRedis();
      redis_tries = redis_tries + 1;
      break;
    } catch (e) {
      if(redis_tries >= MAX_REDIS_TRIES) {
        logger.fatal("unable to connect to broad crawl redis");
      }
      logger.warn("Waiting for redis at broad crawl", {}, "state");
      await sleep(3);
    }
  }
  return new RedisHelper(broadCrawlRedis);
}