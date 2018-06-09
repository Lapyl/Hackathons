MongoClient = require('mongodb').MongoClient;
module.exports = {

  connectDB : function(long,lat){
    var dbLink = 'mongodb://'+'hermano360'+':'+'f00tball'+'@'+'ds137090.mlab.com:37090/meadowlark';
    var query = {};
    MongoClient.connect(dbLink, function(err, db) {
      console.log("Successfully connected to database");
      var doc = {
        type:'leak',
        lat:parseFloat(lat),
        lng:parseFloat(long)
      }
      db.collection('reports').insertOne(doc,function(err,res){
        console.log('document added');
        db.close();
      });
    });
  }
}
