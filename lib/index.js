// Copyright 2019 The Matrix.org Foundation CIC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const seshat = require('../native');

class Seshat extends seshat.Seshat {
  commit_promise() {
    return new Promise((resolve, reject) => {
      this.commit_async((err, res) => {
        resolve(res);
      });
    });
  }

  search_promise(term) {
    return new Promise((resolve) => {
      this.search_async(term, (err, res) => {
        resolve(res);
      });
    });
  }
}

module.exports = Seshat;
