const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "..", "..", ".env") });

const ctrl = require("../controllers/firebase_flags_sync_controller");

(async () => {
  const req = { body: { limit: 50, dryRun: true } };
  const res = {
    json: (o) => {
      console.log("\n=== DryRun Results ===\n");
      console.log(JSON.stringify(o, null, 2));
    },
    status: (c) => ({ json: (o) => console.log("Status", c, JSON.stringify(o, null, 2)) }),
  };

  await ctrl.syncFlaggedReadingsToAggregatedLocations(req, res);
})();
