// Import the MongoDB driver
// const MongoClient = require("mongodb").MongoClient;

import { MongoClient } from "mongodb";

// Define our connection string. Info on where to get this will be described below. In a real world application you'd want to get this string from a key vault like AWS Key Management, but for brevity, we'll hardcode it in our serverless function here.
const MONGODB_URI = process.env.MONGODB_URI;

// Once we connect to the database once, we'll store that connection and reuse it so that we don't have to connect to the database on every request.
let cachedDb = null;

async function connectToDatabase() {
  if (cachedDb) {
    return cachedDb;
  }

  // Connect to our MongoDB database hosted on MongoDB Atlas
  const client = await MongoClient.connect(MONGODB_URI);

  // Specify which database we want to use
  const db = await client.db("checkvar");

  cachedDb = db;
  return db;
}

// export const handler = async (event) => { }
// exports.handler = async (event, context) => { }

export const handler = async (event, context) => {


  // Lấy query từ event
  var searchQuery = ''
  if(event.queryStringParameters && event.queryStringParameters.fontbat)
  {
    searchQuery = event.queryStringParameters.fontbat;
  }
  
  var page = 1
  if(event.queryStringParameters && event.queryStringParameters.page)
  {
    page = event.queryStringParameters.page;
  }

  /* By default, the callback waits until the runtime event loop is empty before freezing the process and returning the results to the caller. Setting this property to false requests that AWS Lambda freeze the process soon after the callback is invoked, even if there are events in the event loop. AWS Lambda will freeze the process, any state data, and the events in the event loop. Any remaining events in the event loop are processed when the Lambda function is next invoked, if AWS Lambda chooses to use the frozen process. */
  context.callbackWaitsForEmptyEventLoop = false;

  // Get an instance of our database
  const db = await connectToDatabase();

  // 
  const limitDocument = 30;
  const pageSkip = limitDocument * (page - 1);
  var totalDocument = 0;
  totalDocument = searchQuery === '' ?  await db.collection("trans").estimatedDocumentCount() : await db.collection("trans").countDocuments({ notes: {$regex: searchQuery, $options: 'i' }});
  const trans = searchQuery === '' ?  await db.collection("trans").find({}).skip(pageSkip).limit(limitDocument).toArray() : await db.collection("trans").find({ notes: {$regex: searchQuery, $options: 'i' }}).skip(pageSkip).limit(limitDocument).toArray();

  const response = {
    statusCode: 200,
    headers: {
                "Access-Control-Allow-Origin": "*",
                // "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With, access-control-allow-headers,access-control-allow-methods,access-control-allow-origin",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
    body: JSON.stringify({
        count: totalDocument,
        currentPage: page,
        next: '?page=3',
        previous: '?page=1',
        results: trans,
        status: "get_data_success"
    }),
  };

  return response;
};