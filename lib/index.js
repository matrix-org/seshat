const seshat = require('../native');

class Seshat extends seshat.Seshat {
  commit_promise() {
    return new Promise((resolve, reject) => {
      this.commit_async((err, res) => {
        resolve(res)
      })
    })
  }
}

var db = new Seshat("./");
var x = db.commit_promise();

console.log(x)

module.exports = Seshat;
