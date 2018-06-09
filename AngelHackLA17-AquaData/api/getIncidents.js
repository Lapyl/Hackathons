MongoClient = require('mongodb').MongoClient;
module.exports = {

  connectDB : function(callback){
    var dbLink = 'mongodb://'+'hermano360'+':'+'f00tball'+'@'+'ds137090.mlab.com:37090/meadowlark';
    var query = {};
    MongoClient.connect(dbLink, function(err, db) {
      console.log("Successfully connected to database");
      console.log(query);
      db.collection('reports').find(query).toArray(function(err, docs) {
        callback(docs);
        db.close();
      });
    });
  }
}
