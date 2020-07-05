const SQLITE = require("../index.js");
const UTIL = require("common-fn-js");
const DBPATH = "./test/chinook.db";
const TEST_DBPATH = "./test/chinook.test.db";

// Always overwrites copy of DB when testing:
UTIL.execAsync(`cp ${DBPATH} ${TEST_DBPATH}`).then(() => SQLITE.of(TEST_DBPATH)
  .getRows("artists", ["Name"]).log(SQLITE.CURSOR)
  .create("artists", ["Name"], ["Rage Against The Machine"]).apto("ratm", SQLITE.SET_LAST_ID)
  .create("artists", ["Name"], ["Nine Inch Nails"]).apto("nin", SQLITE.SET_LAST_ID)
  .chain(STATE=>SQLITE(STATE)
    .getRow("artists", "ArtistId", STATE.ratm).log(SQLITE.CURSOR)
    .updateRow("artists", ["Name"], ["Not Rage Against The Machine"], "ArtistId", STATE.ratm)
    .getRow("artists", "ArtistId", STATE.nin).log(SQLITE.CURSOR)
    .deleteRow("artists", "ArtistId", STATE.nin)
    .exists("artists", "ArtistId", STATE.nin))
    .log(SQLITE.CURSOR)
  .deleteRows("artists")
  .read("artists", ["ArtistId"], "ArtistID > 100")
  .log(SQLITE.CURSOR)
  .close()
  .fail((err) => {
    throw err;
  })
).catch(UTIL.err)
