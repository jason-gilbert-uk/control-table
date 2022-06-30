const AWS = require('aws-sdk')
const axios = require('axios')
const cheerio = require('cheerio')
AWS.config.update({region:'eu-west-1'});
const docClient = new AWS.DynamoDB.DocumentClient();


//----------------------------------------------------------------------------
// function: checkTableExists(tableName)
// returns true if table name exists, false otherwise.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function checkTableExists(tableName) {
    var status=false
    var params = {
        TableName: tableName
    };
    try {
        var dynamodb = new AWS.DynamoDB();
        var result = await dynamodb.describeTable(params).promise();
        status = true;
        return status;
    } catch (err) {
        if (err.code !== 'ResourceNotFoundException') {
            console.log('control-table.checkTableExists encountered error : ',err);
            throw err;
        }
        return status
    }
}

//----------------------------------------------------------------------------
// function: createControlTableIfDoesntExist(tableName)
// Checks to see if database exists, and creates it if not.
// Returns true if database exists, false otherwise.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function createControlTableIfDoesntExist(tableName) {
    if (await checkTableExists(tableName)) {
        return false;
    } else {
        var result = await createControlTable(tableName);
        return true;
    }
}

//----------------------------------------------------------------------------
// function: createControlTable(tableName)
// creates the control table. Returns true if successful.
// Note: This function is very app specific.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function createControlTable (tableName) {
    var dynamodb = new AWS.DynamoDB();
 
    var params = {
        TableName : tableName,
        KeySchema: [       
            { AttributeName: "id", KeyType: "HASH"}
        ],
        AttributeDefinitions: [       
            { AttributeName: "id", AttributeType: "S" }
        ],
        ProvisionedThroughput: {       
            ReadCapacityUnits: 5, 
            WriteCapacityUnits: 5
           }
    };
 
    try {
        var result = await dynamodb.createTable(params).promise(); 
        return true
    } catch (err) {
        console.log('control-table.createControlTable encountered error : ',err);
        throw err;
    }
    
};


//----------------------------------------------------------------------------
// function: writeItemToTable(tableName,item)
// Writes the item to the table.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function writeItemToControlTable(dbTableName,item) {

    var params = {TableName: dbTableName, Item: item }

    try {
        const result = await docClient.put(params).promise();
        return result;      
    } catch (err) {
        console.log('error in control-table.writeItemToControlTable: ',err)
        throw err;
    }
}

//----------------------------------------------------------------------------
// function: extractTescoConfig()
// Extracts the main config from the main tesco groceries page.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function extractTescoConfig() {
    const url = 'https://www.tesco.com/groceries/en-GB/shop'
    try {
        var response = await axios.get(url);
    }
    catch (error) {
        console.log('*** received an error requesting url ',url);
        console.log(error);
        throw error;
    }

    let config = { urls: []}

    try {
        const html = response.data;
        const $ = cheerio.load(html)
        var current = $('.current .list .list-item',html).each(function(){
            var title = $(this).find('.list-item-single-line').text()
            var urlsub = $(this).find('a').attr('href');
            let category = {title: title,url:urlsub, state:'ready',nextInChain:''};
            config.urls.push(category)
        })
        return config;

    } catch (error) {
        console.log('*** received an error extracting config ',url);
        console.log(error);
        throw error;
    }
}

//----------------------------------------------------------------------------
// function: resetConfig(tableName)
// Writes the application control configuration to the table.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function resetConfig(dbTableName) {
    const config = await extractTescoConfig();
    var item = {id: 'scrapingconfig', config: config};
    try {
        var response = await writeItemToControlTable(dbTableName,item);
        return response;
    } catch (err) {
        console.log('error in control-table.resetConfig: ',err)
        throw err
    }
}

//----------------------------------------------------------------------------
// function: readItemFromControlTable(tableName,id,attributes)
// Reads the defined attributes from the entry in the the table with the id
// specified.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function readItemFromControlTable(dbTableName,id,attributes) {
    var params = {
        Key: { 'id': id },
        TableName: dbTableName,
        AttributesToGet: attributes,
        ConsistentRead: false
      };

    try{
      var result = docClient.get(params).promise(); 
      var returnValue = await result;
      return returnValue.Item;   

    } catch (err) {
        console.log('error in control-table.readItemFromControlTable')
        console.log(err);
        throw err
    }
}

//----------------------------------------------------------------------------
// function: readConfigFromControlTable(tableName)
// Reads the application configuration from the table.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function readConfigFromControlTable(dbTableName) {
    try{
        var result = await readItemFromControlTable(dbTableName,'scrapingconfig',['config']);
        return result.config;
    } catch (err) {
        console.log('error in control-table.readConfigFromControlTable: ',err)
        throw err
    }
}

//----------------------------------------------------------------------------
// function: deleteControlTable(tableName)
// Deletes the named table. Returns true if succeeds, false if table doesnt 
// exist
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function deleteControlTable(dbTableName) {
    var dynamodb = new AWS.DynamoDB();
    var params = {
        TableName: dbTableName
    }

    try {
        var result = await dynamodb.deleteTable(params).promise();
        return true;
    } catch (err) {
        if (err.code !== 'ResourceNotFoundException') {
            console.log('error in control-table.deleteControlTable : ',err);
            throw err;
        }
        return false;
    }
}

exports.createControlTableIfDoesntExist = createControlTableIfDoesntExist;
exports.createControlTable = createControlTable;
exports.resetConfig = resetConfig;
exports.writeItemToControlTable = writeItemToControlTable;
exports.readConfigFromControlTable = readConfigFromControlTable;
exports.deleteControlTable = deleteControlTable;
