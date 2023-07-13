import {logger} from "./logger.js";
import child_process from "child_process";

export function fetchInstanceId() {
  let instanceId = null;
  try {
    instanceId = child_process.execSync("wget -q -O - http://169.254.169.254/latest/meta-data/instance-id").toString().trim();
    logger.info(`Instance ID: ${instanceId}`);

    // You can use the `instanceId` variable for further processing.
  } catch (error) {
    logger.error(`Command execution error: ${error.message}. This may happen if you are not executing this on ec2`);
  }
  return instanceId;
}