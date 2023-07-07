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
import path from "path";
import AWS from "aws-sdk";

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
    const status = crawlProcess.status;
    const redirectionChain = "";
    const collDir = "";
    await postCrawl(collDir);
    if(status === 54){
      await redisHelper.pushEventToQueue("crawlStatus",JSON.stringify({
        url: url,
        event: "CRAWL_SUCCESS",
        domain: domain,
        level: level,
        retry: retry,
        s3Path: "",
        redirection_chain: redirectionChain
      }));
    }else if(status === 55){
      //fail
    }
  }
})();

async function postCrawl(collDir){
  const uploadFilesInDirectory = async (directory) => {
    if (!fs.existsSync(directory)) {
      return;
    }

    for (const file of fs.readdirSync(directory)) {
      const filePath = path.join(directory, file);
      const fileName = path.basename(filePath);
      if (!fileName.startsWith("crawl")) {
        await uploadToS3(filePath, fileName);
      }
    }
  };

  const uploadWarcFile = async (directory) => {
    if (!fs.existsSync(directory)) {
      return;
    }

    for (const file of fs.readdirSync(directory)) {
      const filePath = path.join(directory, file);
      if (!fs.lstatSync(filePath).isDirectory()){
        const fileName = path.basename(filePath);
        if (fileName.endsWith(".warc.gz")) {
          await uploadToS3(filePath, fileName);
        }
      }
    }
  };

  await uploadWarcFile(collDir);
  const logDir = path.join(collDir, "logs");
  const pagesDir = path.join(collDir, "pages");
  await uploadFilesInDirectory(logDir);
  await uploadFilesInDirectory(pagesDir);

  removeCollection(collDir);
}

async function uploadToS3(filePath, filename) {
  const s3 = new AWS.S3();
  // Define the bucket name and file path
  const bucketName = process.env.BUCKET_NAME;
  logger.info("bucket name: " + bucketName);

  let prefixKey = `${process.env.ENVIRONMENT}/${this.params.domain}/level_${this.params.level}/${this.params.collection}/${this.current_date}/`;

  // Set the parameters for the S3 upload
  const params = {
    Bucket: bucketName,
    Key: `${prefixKey}${filename}`, // Specify the desired destination file name in the bucket
    Body: fs.createReadStream(filePath),
  };

  try {
    // Upload the file to S3
    const result = await s3.upload(params).promise();
    this.s3FilePath = result.Location;
    logger.info("File uploaded successfully:", result.Location);
  } catch (e){
    logger.error("error",e);
  }
}

function removeCollection(directoryPath) {

  // Function to recursively delete a directory and its contents
  const deleteDirectory = (directory) => {
    if (!fs.existsSync(directory)) {
      return;
    }

    fs.readdirSync(directory).forEach((file) => {
      const filePath = path.join(directory, file);
      if (fs.lstatSync(filePath).isDirectory()) {
        deleteDirectory(filePath);
      } else {
        fs.unlinkSync(filePath);
        logger.debug(`Deleted file: ${filePath}`);
      }
    });

    fs.rmdirSync(directory);
    logger.debug(`Deleted directory: ${directory}`);
  };

  // Function to delete all files and subdirectories within the main directory, excluding specific directories
  const deleteFilesAndSubdirectories = (directory) => {
    if (!fs.existsSync(directory)) {
      return;
    }

    fs.readdirSync(directory).forEach((file) => {
      const filePath = path.join(directory, file);
      if (fs.lstatSync(filePath).isDirectory()) {
        deleteDirectory(filePath);
      } else {
        fs.unlinkSync(filePath);
        logger.debug(`Deleted file: ${filePath}`);
      }
    });

    try {
      fs.rmdirSync(directory);
      logger.debug(`Deleted directory: ${directory}`);
    } catch (error) {
      logger.error(`Failed to delete directory: ${directory}`);
    }
  };

  // Call the function to delete all files and subdirectories within the main directory, excluding specific directories
  deleteFilesAndSubdirectories(directoryPath);
}

function calculateMd5Hash(string) {
  return md5(string);
}


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