const express = require('express');
const app = express();
const MongoClient = require('mongodb').MongoClient
var bodyParser = require('body-parser')
var redis = require('redis')
var client = redis.createClient(6379, '152.46.19.205', {})
var db

app.use(bodyParser.urlencoded({
    extended: true
}));

app.use(bodyParser.json());
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

app.get('/', (req, res) => {
   res.render('main.ejs')
})

app.get('/:msg', (req, res) => {
  db.collection('cityEvent').find({city:req.params.msg}).toArray(function(err, results) {
  	if (err) return console.log(err)
	console.log(results)
    	res.render('index.ejs', {cityEvent: results})
  })
})

app.post('/findcities', (req, res) => {
    client.select(7);
    client.llen(req.body.events, function(err, len) {
        client.lrange(req.body.events, 0, 5, function(err, val) {
            //res.write('Top Cities hosting this event: ' + val);
            res.render('index.ejs', {cityEvent: val})
	    res.end();
        })
    });
})

app.post('/findevents', (req, res) => {
    client.select(10);
    client.llen(req.body.city, function(err, len) {
        client.lrange(req.body.city, 0, 5, function(err, val) {
            //res.write('Top Cities hosting this event: ' + val);
            res.render('indexevents.ejs', {cityEvent_original: val})
            res.end();
        })
    });
})

