const sqlite3 = require('sqlite3');
const METAPROC = require('metaproc');
const UTIL = require('common-fn-js');

// Implents CRUD operations for SQLITE3 using METAPROC:
module.exports = SQLITE = (STATE) => METAPROC.Standard(STATE)

  /**
   *
   *  Create Operations
   *
   */

   // create :: (STRING, [STRING], [*]) -> (METAPROC) -> METAPROC
   // Creates single row for existing table, storing result in "_CURSOR_" PROPERTY:
   .augment("create", (table, columnNames, values) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
     let sanitizedValues = await SQLITE.sanitizeValues(STATE.DB, table, columnNames, values);
     let stmt = `INSERT INTO ${table} (${columnNames.join(", ")}) VALUES(${sanitizedValues.join(", ")})`;
     return await SQLITE.run(STATE.DB,  stmt);
   }))

   // createMany :: (STRING, [STRING], [[*]]) -> (METAPROC) -> METAPROC
   // Creates multiple rows for existing table, storing result in "_CURSOR_" PROPERTY:
   .augment("createMany", (table, columnNames, values) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
     let valuesToSanitize = values.map((values) => {
       return SQLITE.sanitizeValues(STATE.DB, table, columnNames, values).then((values) => {
         return `(${values.join(", ")})`;
       })
     })
     let sanitizedValues = await Promise.all(valuesToSanitize);
     let stmt = `INSERT INTO ${table} (${columnNames.join(", ")}) VALUES ${sanitizedValues.join(", ")};`
     return await SQLITE.run(STATE.DB, stmt);
   }))

   /**
    *
    *  Read Operations
    *
    */

    // read :: (STRING, [STRING], STRING) -> (METAPROC) -> METAPROC
    // Stores rows from query in "CURSOR" PROPERTY:
    .augment("read", (table, columnNames, criteria) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      return SQLITE.all(STATE.DB, `SELECT ${columnNames.join(", ")} FROM ${table} WHERE ${criteria}`);
    }))

    // getRow :: (STRING, STRING, *) -> (METAPROC) -> METAPROC
    // Stores row in "_CURSOR_" for the given critera:
    .augment("getRow", (table, columnName, value) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      let sanitizedValue = await SQLITE.sanitizeValues(STATE.DB, table, [columnName], [value]);
      let query = `SELECT * FROM ${table} WHERE ${columnName} = ${sanitizedValue} LIMIT 1`;
      return SQLITE.all(STATE.DB, query);
    }))

    // getRows :: (STRING, [STRING]) -> (METAPROC) -> METAPROC
    // Returns all selected columns for all rows in given table, storing result in CURSOR:
    .augment("getRows", (table, columnNames) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      return await SQLITE.all(STATE.DB, `SELECT ${columnNames.join(", ")} FROM ${table}`);
    }))

    // exists :: (STRING, STRING, *) -> (METAPROC) -> METAPROC
    // Stores BOOLEAN in "_CURSOR_" for if row exists for the given critera:
    .augment("exists", (table, columnName, value) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      let result = await metaproc.getRow(table, columnName, value);
      return await result.lift(state=>state).then(state=>state.CURSOR.length === 1);
    }))

  /**
   *
   *  Update Operations
   *
   */

   // :: (STRING, [STRING], [*], STRING, *) -> (METAPROC) -> METAPROC
   // Updates the given values for the given columns in the given table using the given criteria
   .augment("update", (table, columnNames, values, critera) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
     let sanitizedValues = await SQLITE.sanitizeValues(STATE.DB, table, columnNames, values);
     let valuesToUpdate = sanitizedValues.map((value, index) => `${columnNames[index]} = ${value}`);
     let stmt = `UPDATE ${table} SET ${valuesToUpdate.join(",")} WHERE ${critera}`;
     let result = SQLITE.run(STATE.DB, stmt);
     return result;
   }))

   // :: (STRING, [STRING], [*], STRING, *) -> (METAPROC) -> METAPROC
   // Updates the given values for the given columns in the given table where the "criteria" value matches the "criteria" column:
   .augment("updateRow", (table, columnNames, values, criteriaColumn, criteriaValue) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
     let sanitizedValues = await SQLITE.sanitizeValues(STATE.DB, table, columnNames, values);
     let sanitizedCriteria =  await SQLITE.sanitizeValues(STATE.DB, table, [criteriaColumn], [criteriaValue]);
     let valuesToUpdate = sanitizedValues.map((value, index) => `${columnNames[index]} = ${value}`);
     let stmt = `UPDATE ${table} SET ${valuesToUpdate.join(",")} WHERE ${criteriaColumn} = ${sanitizedCriteria}`;
     return SQLITE.run(STATE.DB, stmt);
   }))

   /**
    *
    *  Delete Operations
    *
    */

    // delete :: (STRING, STRING) -> (METAPROC) -> METAPROC
    // Delete rows from the given table for the given criteria:
    .augment("delete", (table, criteria) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      let stmt = `DELETE FROM ${table} WHERE ${criteria}`;
      return SQLITE.run(STATE.DB, stmt);
    }))

    // deleteRow :: (STRING, STIRNG, *) -> (METAPROC) -> METAPROC
    // Delete row from given table for given column and value:
    .augment("deleteRow", (table, columnName, value) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      let sanitizedValue = await SQLITE.sanitizeValues(STATE.DB, table, [columnName], [value]);
      return metaproc.delete(table, `${columnName} = ${sanitizedValue}`);
    }))

    // deleteRows :: (STRING) -> (METAPROC) -> METAPROC
    // Delete all rows in the given table:
    .augment("deleteRows", (table) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      return SQLITE.run(STATE.DB, `DELETE FROM ${table}`);
    }))

   /**
    *
    * General Operations
    *
    */

    // query :: (STRING) -> (METAPROC) -> STMT
    // Runs query, returning a STATMENT object as it's result:
    .augment("query", (query) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      return await SQLITE.run(STATE.DB, query);
    }))

    // queryRows :: (STRING) -> (METAPROC) -> [ROW]
    // Runs query, returning an array of ROW objects
    .augment("queryRows", (query) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      return await SQLITE.all(STATE.DB, query);
    }))

    // map :: (STRING, (ROW) -> *) -> (METAPROC) -> METAPROC
    // Applies function to every row of query, storing the result of each function in "CURSOR"
    .augment("map", (query, fn) => metaproc => metaproc.apto("CURSOR", async (CURSOR, STATE) => {
      return await SQLITE.each(STATE.DB, query, async (row) => fn(row, STATE));
    }))

    // close :: (VOID) -> METAPROC
    // Closes DB connection:
    .augment("close", () => metaproc => {
      return metaproc.ap(STATE => new Promise((resolve, reject) => {
        STATE.DB.close(closeErr => {
          if (closeErr) reject(closeErr);
          resolve(STATE);
        });
      }))
    })

    // fail :: (VOID) -> METAPROC
    // Overrides "fail" to ensure DB connection is closed when exception is thrown:
    .augment("fail", (fn) => metaproc => {
      let fail = (err) => {
        err.STATE.DB.close(closeErr => {
          fn === undefined ? console.log(err) : fn(err)
        })
      }
      return metaproc.lift((STATE, OPS) => METAPROC.of(STATE.catch(fail), OPS))
    })

/**
 *
 *  "Static" Methods
 *
 */

 // "Unit" monadic operator :: (STRING, STRING) -> METAPROC
 // NOTE: Mode default to OPEN_READWRITE | OPEN_CREATE
 // NOTE: Connection to DB remains open after initialized until explicitly closed
 SQLITE.of = (dbpath, mode) => SQLITE(new Promise((resolve, reject) => {
   let db =  new sqlite3.Database(dbpath || ':memory:' , mode, (err) => {
     if (err) reject(err);
   });
   resolve({
     "DB":db,       // Database connection
     "CURSOR":[]    // Where database operation results are stored
   });
 }))


 // :: (DATABASE, STRING) -> PROMISE([ROW])
 // Return promise of rows for given DB and query:
 // NOTE: This will load ALL of query result into memory:
 SQLITE.all = (db, query) => new Promise((resolve, reject) => {
   db.all(query, [], (err, rows) => {
     if (err) {
        reject(err);
     } else {
       resolve(rows);
     }
   });
 });

 // each :: (DATABASE, STRING, (ROW) -> PROIMSE([*])
 // Returns promise of result for each function applied to each row of query result:
 // NOTE: This prevents all of query result from needing to be loaded into memory:
 SQLITE.each = (db, query, fn) => new Promise((resolve, reject) => {
   let result = [];
   db.each(query, (err, row) => {
     if (err) {
       reject(err);
     }
     result.push(fn(row));
   },  () => {
     resolve(result)
   });
 });

 // run :: (DATABASE, STRING) -> PROMISE(STATEMENT)
 // Runs the given SQLite statment, resolving with PROMISE of "STATMENT":
 SQLITE.run = (db, stmt) => new Promise((resolve,reject) => {
   db.run(stmt, function(err) {
     if (err) {
       reject(err)
     } else {
       resolve(this);
     }
   });
 });

 // :: (DATABASE, STRING) -> PROMISE({COLUMN.NAME:COLUMN})
 // Returns PROMISE of COLUMN objects by COLUMN.Name for given DATABASE and table name:
 SQLITE.columns = (db, tableName) => new Promise(async (resolve, reject) => {
   try {
      let columns =  await SQLITE.all(db, `PRAGMA table_info(${tableName})`);
      let columnsByName = columns.reduce((result, column) => UTIL.assoc(column.name, column)(result), {});
      resolve(columnsByName);
   } catch (err) {
     reject(err);
   }
 });

 // :: (STRING) -> (*) -> INTEGER|STRING|FLOAT|NUMBER
 // Tries to ensure value passed to partial application is cast based on column type of given column
 SQLITE.sanitize = (column) => {

  // Find column type to compare value against:
  // NOTE: https://sqlite.org/datatype3.html
  let columnType = ((columnType) => {
    // Is "INTEGER":
    if (/(INT)/.test(columnType) === true) return "INTEGER";
    // Is "TEXT":
    if (/(CHAR|TEXT|CLOB)/.test(columnType) === true) return "TEXT";
    // Is "BLOB":
    if (/(BLOB)/.test(columnType) === true) return "BLOB";
    // Is "REAL":
    if (/(REAL|FLOA|DOUB)/.test(columnType) === true) return "REAL";
    // Otherwise is "NUMERIC":
    return "NUMERIC";
  })(column.type);

  // Returns function to that casts value to approriate column Type:
  return (val) => {
    switch (columnType) {
      case "INTEGER":
        return parseInt(val);
      case "TEXT":
        return `"${val}"`;
      case "BLOB":
        return val;
      case "REAL":
        return parseFloat(val);
      case "NUMERIC":
        return Number(val);
      default:
        throw "Could not santize value for unknown column type"
    }
  }
};

// :: (DATABASE, STRING, [STRING], [*]) -> [INTEGER|STRING|FLOAT|NUMBER]
// Convenience method for santizing an array of values using an array of column names from the given table:
// NOTE: Index of column MUST MATCH index of value to reliably sanitize value of appropriate column type
SQLITE.sanitizeValues = (db, tableName, columnNames, values) => new Promise(async (resolve, reject) => {
  try {
    let columnsByName = await SQLITE.columns(db, tableName);
    let sanitizedValues = columnNames.map((columnName, index) => {
      if (columnsByName[columnName] === undefined) {
        reject("Column does not exist");
      }
      let column = columnsByName[columnName];
      return SQLITE.sanitize(column)(values[index]);
    })
    resolve(sanitizedValues);
  } catch (err) {
    reject(err);
  }
});

// :: (OBJECT) -> OBJECT
// Convenience method for returning CURSOR from STATE:
// NOTE: This is ready from the value in STATE, not the PROMISE of STATE:
SQLITE.CURSOR = (STATE) => STATE.CURSOR

// :: (OBJECT) -> NUMBER
// NOTE: This is ready from the value in STATE, not the PROMISE of STATE:
// Convenience method for returning last ID from CURSOR:
SQLITE.GET_LAST_ID = (STATE) => STATE.CURSOR.lastID

// :: (OBJECT) -> NUMBER
// NOTE: This is ready from the value in STATE, not the PROMISE of STATE:
// Convenience method for storing last ID from CURSOR in STATE:
SQLITE.SET_LAST_ID = (PROPERTY, STATE) => STATE.CURSOR.lastID
