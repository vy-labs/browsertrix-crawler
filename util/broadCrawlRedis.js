import Redis from "ioredis";

export async function initBroadCrawlRedis() {
  let board_crawl_redis = null;
  if(process.env.ENVIRONMENT === "dev") {
    board_crawl_redis = new Redis({
      host: process.env.REDIS_HOST,
      port: process.env.REDIS_PORT,
      password: process.env.REDIS_PASSWORD,
      lazyConnect: true
    });
  } else {
    board_crawl_redis = new Redis({
      host: process.env.REDIS_HOST,
      port: process.env.REDIS_PORT,
      lazyConnect: true
    });
  }
  await board_crawl_redis.connect();
  return board_crawl_redis;
}