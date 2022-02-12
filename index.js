const mongodb = require("mongodb");
const utils = require("./utils");

const url = GetConvar("mongodb_url", "changeme");
const dbName = GetConvar("mongodb_database", "changeme");

let db;

if (url != "changeme" && dbName != "changeme") {
    mongodb.MongoClient.connect(url, {useNewUrlParser: true, useUnifiedTopology: true}, function (err, client) {
        if (err) return console.log("[MongoDB][ERROR] Failed to connect: " + err.message);
        db = client.db(dbName);

        console.log(`[MongoDB] Connected to database "${dbName}".`);
        emit("onDatabaseConnect", dbName);
    });
} else {
    if (url == "changeme") console.log(`[MongoDB][ERROR] Convar "mongodb_url" not set (see README)`);
    if (dbName == "changeme") console.log(`[MongoDB][ERROR] Convar "mongodb_database" not set (see README)`);
}

function checkDatabaseReady() {
    if (!db) {
        console.log(`[MongoDB][ERROR] Database is not connected.`);
        return false;
    }
    return true;
}

function callbackOrElse(callback, ...everythingElse) {
    if (callback) {
        return utils.safeCallback(callback, ...everythingElse)
    } else {
        return everythingElse
    }
}

function checkParams(params) {
    return params !== null && typeof params === 'object';
}

function getParamsCollection(params) {
    if (!params.collection) return;
    return db.collection(params.collection)
}

/* MongoDB methods wrappers */

/**
 * MongoDB insert method
 * @param {Object} params - Params object
 * @param {Array}  params.documents - An array of documents to insert.
 * @param {Object} params.options - Options passed to insert.
 */
async function dbInsert(params, callback) {
    if (!checkDatabaseReady()) return;
    if (!checkParams(params)) return console.log(`[MongoDB][ERROR] exports.insert: Invalid params object.`);

    let collection = getParamsCollection(params);
    if (!collection) return console.log(`[MongoDB][ERROR] exports.insert: Invalid collection "${params.collection}"`);

    let documents = params.documents;
    if (!documents || !Array.isArray(documents))
        return console.log(`[MongoDB][ERROR] exports.insert: Invalid 'params.documents' value. Expected object or array of objects.`);

    const options = utils.safeObjectArgument(params.options);
    try {
        const result = await collection.insertMany(documents, options)
        let arrayOfIds = [];
        // Convert object to an array
        for (let key in result.insertedIds) {
            if (result.insertedIds.hasOwnProperty(key)) {
                arrayOfIds[parseInt(key)] = result.insertedIds[key].toString();
            }
        }
        return callbackOrElse(callback, true, result.insertedCount, arrayOfIds)
    } catch (err) {
        console.log(`[MongoDB][ERROR] exports.insert: Error "${err.message}".`);
        return callbackOrElse(callback, false, err.message)
    }
}


/**
 * MongoDB find method
 * @param {Object} params - Params object
 * @param callback
 * @param {Object} params.query - Query object.
 * @param {Object} params.options - Options passed to insert.
 * @param {number} params.limit - Limit documents count.
 */
async function dbFind(params, callback) {
    if (!checkDatabaseReady()) return;
    if (!checkParams(params)) return console.log(`[MongoDB][ERROR] exports.find: Invalid params object.`);

    let collection = getParamsCollection(params);
    if (!collection) return console.log(`[MongoDB][ERROR] exports.insert: Invalid collection "${params.collection}"`);

    const query = utils.safeObjectArgument(params.query);
    const options = utils.safeObjectArgument(params.options);
    try {
        let cursor = await collection.find(query, options);
        if (params.limit) cursor = cursor.limit(params.limit);
        const documents = await cursor.toArray();
        return callbackOrElse(callback, true, utils.exportDocuments(documents))
    } catch (err) {
        console.log(`[MongoDB][ERROR] exports.find: Error "${err.message}".`);
        return callbackOrElse(callback, false, err.message)
    }
}

/**
 * MongoDB update method
 * @param {Object} params - Params object
 * @param callback
 * @param isUpdateOne
 * @param {Object} params.query - Filter query object.
 * @param {Object} params.update - Update query object.
 * @param {Object} params.options - Options passed to insert.
 */
async function dbUpdate(params, callback, isUpdateOne) {
    if (!checkDatabaseReady()) return;
    if (!checkParams(params)) return console.log(`[MongoDB][ERROR] exports.update: Invalid params object.`);

    let collection = getParamsCollection(params);
    if (!collection) return console.log(`[MongoDB][ERROR] exports.insert: Invalid collection "${params.collection}"`);

    const query = utils.safeObjectArgument(params.query);
    const update = utils.safeObjectArgument(params.update);
    const options = utils.safeObjectArgument(params.options);
    try {
        const res = await (isUpdateOne ? collection.updateOne(query, update, options) : collection.updateMany(query, update, options));
        return callbackOrElse(callback, true, res.result.nModified)
    } catch (err) {
        console.log(`[MongoDB][ERROR] exports.update: Error "${err.message}".`, params);

        return callbackOrElse(callback, false, err.message)
    }
}

/**
 * MongoDB count method
 * @param {Object} params - Params object
 * @param callback
 * @param {Object} params.query - Query object.
 * @param {Object} params.options - Options passed to insert.
 */
async function dbCount(params, callback) {
    if (!checkDatabaseReady()) return;
    if (!checkParams(params)) return console.log(`[MongoDB][ERROR] exports.count: Invalid params object.`);

    let collection = getParamsCollection(params);
    if (!collection) return console.log(`[MongoDB][ERROR] exports.insert: Invalid collection "${params.collection}"`);

    const query = utils.safeObjectArgument(params.query);
    const options = utils.safeObjectArgument(params.options);
    try {
        const count = await collection.countDocuments(query, options)
        return callbackOrElse(callback, true, count)
    } catch (err) {
        console.log(`[MongoDB][ERROR] exports.count: Error "${err.message}".`);
        return callbackOrElse(callback, false, err.message)
    }
}

/**
 * MongoDB delete method
 * @param {Object} params - Params object
 * @param {Object} params.query - Query object.
 * @param {Object} params.options - Options passed to insert.
 */
async function dbDelete(params, callback, isDeleteOne) {
    if (!checkDatabaseReady()) return;
    if (!checkParams(params)) return console.log(`[MongoDB][ERROR] exports.delete: Invalid params object.`);

    let collection = getParamsCollection(params);
    if (!collection) return console.log(`[MongoDB][ERROR] exports.insert: Invalid collection "${params.collection}"`);

    const query = utils.safeObjectArgument(params.query);
    const options = utils.safeObjectArgument(params.options);

    try {
        const res = await isDeleteOne ? collection.deleteOne(query, options) : collection.deleteMany(query, options);
        return callbackOrElse(callback, true, res.result.n)
    } catch (err) {
        console.log(`[MongoDB][ERROR] exports.delete: Error "${err.message}".`);
        return callbackOrElse(callback, false, err.message)
    }
}

async function dbCollectionExists(collectionName, callback) {
    if (!checkDatabaseReady()) return;

    const result = await db.listCollections({name: collectionName}).toArray()
    return callbackOrElse(callback, true, result.length > 0)
}

/* Exports definitions */

exports("isConnected", () => !!db);

exports("collectionExists", dbCollectionExists)

exports("insert", dbInsert);
exports("insertOne", async (params, callback) => {
    if (checkParams(params)) {
        params.documents = [params.document];
        params.document = null;
    }
    return await dbInsert(params, callback)
});


exports("find", dbFind);
exports("findOne", async (params, callback) => {
    if (checkParams(params)) params.limit = 1;
    return await dbFind(params, callback);
});

exports("update", dbUpdate);
exports("updateOne", async (params, callback) => {
    return await dbUpdate(params, callback, true);
});

exports("count", dbCount);

exports("delete", dbDelete);
exports("deleteOne", async (params, callback) => {
    return await dbDelete(params, callback, true);
});
