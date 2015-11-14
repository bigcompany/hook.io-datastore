var datastore = require('../index');

var ds = new datastore.Datastore({ port: 6379, host: "localhost" });

// callback api
ds.set("testvalue2", 99, function(err, result) {
  console.log(result);
  ds.get("testvalue2", function(err, result) {
    console.log(result);
  });
});

// promises api
ds.set("testvalue", 42).then(function() {
  return ds.get("testvalue");
}).then(console.log, console.error);