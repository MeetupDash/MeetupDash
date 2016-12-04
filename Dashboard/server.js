const express = require('express');
const app = express();
const MongoClient = require('mongodb').MongoClient

var db

// MongoClient.connect('mongodb://localhost:27017/envoprov', (err, database) => {
//   if (err) return console.log(err)
//   db = database
//   app.listen(3000, () => {
//     console.log('listening on 3000')
//   })
// })


MongoClient.connect('mongodb://152.46.19.205:27017/meetup', (err, database) => {
  if (err) return console.log(err)
  db = database
  app.listen(3000, () => {
    console.log('listening on 3000')
  })
})

app.get('/:msg', (req, res) => {
  db.collection('cityEvent').find({city:req.params.msg}).toArray(function(err, results) {
  	if (err) return console.log(err)
    	// renders index.ejs
    //console.log("city:"+"'"+req.params.msg+"'")
    //console.log(db.collection('cityEvent').find({"city":'"'+req.params.msg+'"'}).toArray())
	console.log(results)
    	res.render('index.ejs', {cityEvent: results})
  })
})
