// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

var Nouveau = (function() {

  var index_results = []; // holds temporary emitted values during index

  function handleIndexError(err, doc) {
    if (err == "fatal_error") {
      throw(["error", "map_runtime_error", "function raised 'fatal_error'"]);
    } else if (err[0] == "fatal") {
      throw(err);
    }
    var message = "function raised exception " + err.toSource();
    if (doc) message += " with doc._id " + doc._id;
    log(message);
  };

  function assertType(expected, actual) {
    if (typeof actual !== expected) {
      throw({name: 'TypeError', message: 'type must be a ' + expected + ' not ' + typeof actual});
    }
  };

  return {
    nouveau_index: function(type, name) {
      assertType('string', type);
      assertType('string', name);
      if (name.substring(0, 1) === '_') {
        throw({name: 'ReservedName', message: 'name must not start with an underscore'});
      }
      switch (type) {
      case 'binary_dv':
      case 'stored_binary':
      case 'sorted_set_dv':
      case 'sorted_dv':
        assertType('string', arguments[2]);
        index_results.push({'@type': type, 'name': name, 'value': arguments[2]});
        break;
      case 'double_point':
      case 'float_dv':
      case 'float_point':
      case 'int_point':
      case 'long_point':
      case 'sorted_numeric_dv':
      case 'double_dv':
        assertType('number', arguments[2]);
        index_results.push({'@type': type, 'name': name, 'value': arguments[2]});
        break;
      case 'latlon_dv':
      case 'latlon_point':
        assertType('number', arguments[2]);
        assertType('number', arguments[3]);
        index_results.push({'@type': type, 'name': name, 'lat': arguments[2], 'lon': arguments[3]});
        break;
      case 'xy_dv':
      case 'xy_point':
        assertType('number', arguments[2]);
        assertType('number', arguments[3]);
        index_results.push({'@type': type, 'name': name, 'x': arguments[2], 'y': arguments[3]});
        break;
      case 'string':
      case 'text':
        assertType('string', arguments[2]);
        if (arguments.length === 4) {
          assertType('boolean', arguments[3]);
        }
        index_results.push({'@type': type, 'name': name, 'value': arguments[2], 'stored': arguments[3] || false});
        break;
      case 'stored_double':
        assertType('number', arguments[2]);
        index_results.push({'@type': type, 'name': name, 'value': arguments[2]});
        break;
      case 'stored_string':
        assertType('string', arguments[2]);
        index_results.push({'@type': type, 'name': name, 'value': arguments[2]});
        break;
      default:
        throw({name: 'TypeError', message: type + ' not supported'});
      }
    },

    indexDoc: function(doc) {
      Couch.recursivelySeal(doc);
      var buf = [];
      for (var fun in State.funs) {
        index_results = [];
        try {
          State.funs[fun](doc);
          buf.push(index_results);
        } catch (err) {
          handleIndexError(err, doc);
          buf.push([]);
        }
      }
      print(JSON.stringify(buf));
    }

  }
})();
