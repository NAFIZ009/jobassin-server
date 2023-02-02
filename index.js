const express = require('express');
const multer = require("multer");
const cors = require('cors');
require('dotenv').config();
const app = express();
const PORT = process.env.PORT || 5000;
const upload = multer();
const { MongoClient, ServerApiVersion } = require('mongodb');
const uri = `mongodb+srv://Jalal_Ahmed:${process.env.DB_PASS}@cluster0.hy1ku46.mongodb.net/?retryWrites=true&w=majority`;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverApi: ServerApiVersion.v1 });



//middleware
app.use(cors());
app.use(express.json());

const chunkSize=1000;


const dataCollection = client.db("data").collection("datacollection");

//test api
app.get('/', (req, res) => {
  res.send('Hello World!');
});

// Split data into chunks
const chunkData = (data) => {
  let chunks = [];
  for (let i = 0; i < data.length; i += chunkSize) {
      chunks.push(data.slice(i, i + chunkSize));
  }
  // console.log(chunks);
  return chunks;
};


//checking for duplicate chunk
const checkChunk = async (chunk) => {
  const chunkId = chunk.map(chk => chk.pnum);
  let foundedChunkIds = [];
  for (const id of chunkId) {
    const foundedChunk = await dataCollection.findOne({ pnum: id });
    if (foundedChunk) {
      foundedChunkIds.push(parseInt(id));
    }
  }
  return foundedChunkIds;
};


//api for uploading data
app.post("/data", upload.single("file"),async (req, res) => {
  try {
    const data = req.file.buffer.toString();
    const jsonData=JSON.parse(data);
    const chunks = chunkData(jsonData);
    for (let chunk of chunks) {
      let foundedChunkId=await checkChunk(chunk);
      const pushAbleChunk=chunk.filter(chk=>{
        return !foundedChunkId.includes(chk.pnum);
      });
      console.log(pushAbleChunk);
      if(pushAbleChunk.length==0){
        throw new Error('No Data Is Added');
      }
        const resu=await dataCollection.insertMany(pushAbleChunk);
    };

    res.send({ message: "Data sent successfully" });

} catch (error) {
    res.status(500).send({ error: error.message });
}
});

app.delete('/dlt',async(req, res) => {
  const dlt=await dataCollection.deleteMany({});
});


app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});