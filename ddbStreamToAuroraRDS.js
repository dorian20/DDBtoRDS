const AWS = require("aws-sdk");
const mysql = require("mysql");
const jsonSchema = require('jsonschema');

const schema = {  
    "$schema":"http://json-schema.org/draft-04/schema#",
    "type":"object",
    "properties":{  
         "MEM_NO":{ "type":"string" },
         "ORD_NO":{ "type":"string" },
         "TX_ID": { "type":"string" },
         "GOODS_LST":{  
             "type":"object",
             "properties":{  
                 "DLV_NO":   { "type":"string" },
                 "GIFT_MSG": { "type":"string" },
                 "GOODS_AMT":{ "type":"integer" },
                 "GOODS_NO": { "type":"string" },
                 "ORD_CNT":  { "type":"integer" }
             },
             "required":[ "DLV_NO", "GIFT_MSG", "GOODS_AMT", "GOODS_NO", "ORD_CNT" ]
         },
         "DSCNT_INFO":{  
             "type":"object",
             "properties":{  
                 "DLV_CPN_NO":{ "type":"string" },
                 "MKT_CPN_NO":{ "type":"string" }
             },
             "required":[ "DLV_CPN_NO", "MKT_CPN_NO" ]
         },
         "PAY_INFO":{  
             "type":"object",
             "properties":{  
                 "PNT_USE_AMT": { "type":"integer" },
                 "CARD_USE_AMT":{ "type":"integer" }
             },
             "required":[ "PNT_USE_AMT", "CARD_USE_AMT" ]
         }
    },
    "required":[ "MEM_NO", "ORD_NO", "TX_ID", "GOODS_LST", "DSCNT_INFO", "PAY_INFO" ]
};

const pool = mysql.createPool({
    host     : process.env.DB_ADDR,
    port     : process.env.DB_PORT,
    database : process.env.DB_NAME,
    user     : process.env.DB_USER,
    password : process.env.DB_PWD,
    connectionLimit: 10,
    waitForConnections: false
});

exports.handler = (event, context, callback) => {
    const parse = AWS.DynamoDB.Converter.output;
    const jsonValidator = new jsonSchema.Validator();
    let data = [];

    event.Records.forEach((record) => {

        if (record.eventName == "INSERT") {

            let order = parse({ "M": record.dynamodb.NewImage });
            let _data = [];
            console.log('ORDER INFO:' + JSON.stringify(order));

            let validationResult = jsonValidator.validate(order, schema);
            console.log('Json Schema Validation result = ' + validationResult.errors);
    
            if (validationResult.errors.length === 0) {
                _data.push(order.ORD_NO);
                _data.push(order.MEM_NO);
                _data.push(order.GOODS_LST.GOODS_NO);
                _data.push(order.GOODS_LST.ORD_CNT);
                _data.push(order.GOODS_LST.GOODS_AMT);
                _data.push(order.GOODS_LST.DLV_NO);
                _data.push(order.GOODS_LST.GIFT_MSG);
                _data.push(order.PAY_INFO.PNT_USE_AMT);
                _data.push(order.PAY_INFO.CARD_USE_AMT);
                _data.push(order.DSCNT_INFO.DLV_CPN_NO);
                _data.push(order.DSCNT_INFO.MKT_CPN_NO);

                data.push(_data);
            }
        }
    });

    if(data.length === 0) {
        return callback(null, 'No data to load into RDS.');
    }

    console.log('Insert Data: ' + JSON.stringify(data));

    let sql = 'INSERT INTO poc_ord_tx.poc_op_ord_tmp ' + 
            '(ORD_NO, MEM_NO, GOODS_NO, ORD_CNT, GOODS_AMT, DLV_NO, GIFT_MSG, PNT_USE_AMT, CARD_USE_AMT, DLV_CPN_NO, MKT_CPN_NO) VALUES ?';
    
    pool.getConnection( function(err, conn){
        
        conn.query( sql, [data], function (err, result) {
            if (err) console.log(err, err.stack);
            else console.log(result);
            if(conn) conn.release();
        });
    });

    callback(null, 'Successfully load data into RDS.');
};
