import express from "express";
import bodyParser from "body-parser";
import child_process from "child_process";
import yaml from "js-yaml";
import fs from "fs";

const app = express();
const port = 3000;

app.use(bodyParser.json());

let crawlProcess = null;
let fixedArgs = createArgsFromYAML();

app.post("/crawl", (req, res) => {
  try {
    const reqDict = {...req.body};
    const requiredKeys = ["url", "collection", "id", "domain", "level", "retry"];
    const missingKeys = requiredKeys.filter((key) => !(key in reqDict));
    console.log("crawl starting");
    if (missingKeys.length === 0) {
      const args = [
        "--url", reqDict.url,
        "--domain", reqDict.domain,
        "--level", reqDict.level,
        "--collection", String(reqDict.collection),
        "--id", String(reqDict.id),
        "--retry", reqDict.retry,
        "--pywb_http_socket", ":8080",
        "--pywb_socket", ":8081",
        "--xvfb_screen", "0",
        "--redis_db",0
      ];
      args.push(...fixedArgs);

      const secondaryArgs = [
        "--url", "https://www.google.com/",
        "--domain", "google.com",
        "--level", 0,
        "--collection", "google",
        "--id", "randomId",
        "--retry", 0,
        "--pywb_http_socket", ":8082",
        "--pywb_socket", ":8083",
        "--xvfb_screen", "1",
        "--redis_db",1
      ];
      secondaryArgs.push(...fixedArgs);

      child_process.spawn("crawl", secondaryArgs, {stdio: "inherit"});
      console.log("starting");
      crawlProcess = child_process.spawnSync("crawl", args, {stdio: "inherit"});
      res.status(200).json({info: `${reqDict.url} crawl finished`});
    } else {
      res.status(404).json({error: `Ensure that ${requiredKeys.join(". ")} is present as keys in json`});
    }
  } catch (e) {
    res.status(500).json({error: e.message});
  }

});


app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});


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