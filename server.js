import express from "express";
import bodyParser from "body-parser";
import child_process from "child_process";
import yaml from "js-yaml";
import fs from "fs";
import {initBroadCrawlRedis} from "./util/broadCrawlRedis.js";
import {logger} from "./util/logger.js";
import {sleep} from "./util/timing.js";
import {RedisHelper} from "./util/redisHelper.js";
import md5 from "md5";
import { v4 as uuidv4 } from "uuid";

const app = express();
const port = 3000;
const MAX_REDIS_TRIES = 3;
const EVENT_QUEUE = "test_queue:start_urls";

app.use(bodyParser.json());

let crawlProcess = null;
let fixedArgs = createArgsFromYAML();
(async function() {
  const redisHelper = await getBroadCrawlRedisHelper();

  while(true){
    const event = JSON.parse(await redisHelper.getEventFromQueue(EVENT_QUEUE));
    if(event === null){
      process.exit(1);
    }
    console.log(typeof event);


    const url = event["url"];
    const level = event.level;
    const domain = event.domain;
    console.log(url);
    const url_md5 = calculateMd5Hash(url);
    const retry = event.retry || 0;
    const collection = url_md5;
    const crawlId = uuidv4();

    await redisHelper.pushEventToQueue("crawlStatus",JSON.stringify({
      url: url,
      event: "CRAWL_PROCESSING",
      domain: domain,
      level: level,
      retry: retry
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
  }
})();

function calculateMd5Hash(string) {
  return md5(string);
}


// app.post("/crawl", (req, res) => {
//   try {
//     const reqDict = {...req.body};
//     const requiredKeys = ["url", "collection", "id", "domain", "level", "retry"];
//     const missingKeys = requiredKeys.filter((key) => !(key in reqDict));
//     if (missingKeys.length === 0) {
//       const args = [
//         "--url", reqDict.url,
//         "--domain", reqDict.domain,
//         "--level", reqDict.level,
//         "--collection", String(reqDict.collection),
//         "--id", String(reqDict.id),
//         "--retry", reqDict.retry
//       ];
//       args.push(...fixedArgs);
//
//       crawlProcess = child_process.spawnSync("crawl", args, {stdio: "inherit"});
//       res.status(200).json({info: `${reqDict.url} crawl finished`});
//     } else {
//       res.status(404).json({error: `Ensure that ${requiredKeys.join(". ")} is present as keys in json`});
//     }
//   } catch (e) {
//     res.status(500).json({error: e.message});
//   }
//
// });
//
//
// app.listen(port, () => {
//   console.log(`Server running at http://localhost:${port}`);
// });


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
  const redisUrl = "redis://localhost:6379/0";
  let broadCrawlRedis;
  let redis_tries = 0;
  while (true) {
    try {
      broadCrawlRedis = await initBroadCrawlRedis();
      redis_tries = redis_tries + 1;
      break;
    } catch (e) {
      if(redis_tries >= MAX_REDIS_TRIES) {
        logger.fatal("Unable to connect to state store Redis: " + redisUrl);
      }
      logger.warn("Waiting for redis at broad crawl", {}, "state");
      await sleep(3);
    }
  }
  return new RedisHelper(broadCrawlRedis);
}