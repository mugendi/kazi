const options = require('./config.json');

port = process.argv[2] || 1337;

var app = require('bee-queue-ui/app')(options);

app.listen(port, function(){
  console.log('UI-APP started listening on port', this.address().port);
});
