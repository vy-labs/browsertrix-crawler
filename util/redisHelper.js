import {logger} from "./logger.js";

export class RedisHelper{

  constructor(redis) {
    this.redis = redis;
  }

  async pushEventToQueue(queueName, event) {
    try {
      await this.redis.rpush(queueName, event);
      logger.info(`Event pushed to the ${queueName} queue: ${event}`);
    } catch (error) {
      logger.error("Error pushing event to queue:", error);
    }
  }

  async getEventFromQueue(queueName){
    try {
      const result = await this.redis.blpop(queueName, 120); // wait for 120 secs for an event

      if (result === null) {
        console.log("Timeout occurred, no item available in the queue within 120 seconds");
        return null;
      }
      // The result is an array with the queue name and the dequeued item
      const dequeuedItem = result[1];

      console.log(`Dequeued from ${queueName}:`, dequeuedItem);

      return dequeuedItem;
    } catch (error) {
      console.error("Error while dequeuing from the queue:", error);
    }
  }
}