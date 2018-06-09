var express = require('express');
var getIncidents = require('./api/getIncidents.js');
var addIncident = require('./api/addIncident.js');
var twilioText = require('./api/twilioText.js')

//Create out app

var app = express();
const PORT = process.env.PORT || 3000;

//making sure traffic is through http, if not converting to http because
// openWeatherMap doesn't work with https
app.use(function(req,res,next){
  if(req.headers['x-forwarded-proto']==='https'){
    res.redirect('http://' + req.hostname + req.url);
  } else {
    next();
  }
})
app.use(express.static('public'));

app.get('/getIncidents', function(req,res,next){
  console.log("getting incidents");
  getIncidents.connectDB(function(docs){
    res.json(docs)
  });
});
app.get('/sendText/:lat/:long', function(req,res,next){
  console.log(req.params.lat,req.params.long);
  const message = `Hello Herminio, There is a Leak located at ${req.params.long} long and ${req.params.lat} lat that requires your immediate attention.`
  twilioText.sendText('AC722a93dc407dc8dc0747750f8765780a','3072390f3d09657456b3cd9f59d3ec1e','17655884976','7655436533',message);
  addIncident.connectDB(req.params.long,req.params.lat);
  res.json({"message":"sent","database":"updated"});

});

app.listen(PORT,function(){
  console.log('Express server is up on port ' + PORT);
});
