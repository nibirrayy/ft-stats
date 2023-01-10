const express = require("express");
const app = express();

const port = 3000;

// Modifying the headers of all responses to allow CORS (Cross-Origin-Resource-Sharing)
app.use((req, res) => {
  res.header("Access-Control-Allow-Origin", " * ");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept, Authotization"
  );
  if (req.method === "OPTIONS") {
    res.header("Access-Control-Allow-Methods", "PUT,POST,PATCH,DELTE,GET");
    return res.status(200).json({});
  }
});

app.get("/", (req, res) => {
  res.send("Hello world");
});

app.listen(port, () => console.log(`App listining on port ${port}`));
