var AWS = require("aws-sdk");
var util = require("util");
var _ = require("lodash");

function CloudwatchBackend(startupTime, config, emitter) {
  var self = this;

  this.config = config || {};
  AWS.config = this.config;

  function setEmitter() {
    var cloudwatchApi = new AWS.CloudWatch(self.config);
    emitter.on("flush", function(timestamp, metrics) {
      flush(timestamp, metrics, cloudwatchApi, self.config);
    });
  }
  setEmitter();
}
exports.CloudwatchBackend = CloudwatchBackend;

function processKey(key) {
  var parts = key.split(/[\.\/-]/);
  return {
    metricName: parts[parts.length - 1],
    namespace: parts.length > 1 ? parts.splice(0, parts.length - 1).join("/") : null,
  };
}
exports.processKey = processKey;

/*
  whitelist can be a list of regexp or simple regexp
  if whitelist is not defined in the config (undefined or null) it returns true
*/
function isWhitelisted(key, whitelist) {
  // if there is no white listed metrics all the metrics will be send
  if (_.isNil(whitelist)) return true;

  var whitelistArr = _.castArray(whitelist);
  for (var value of whitelistArr) {
    var reg = RegExp("^" + value + "$");
    if (reg.test(key)) return true;
  }
  return false;
}
exports.isWhitelisted = isWhitelisted;

function chunk(arr, chunkSize) {
  var groups = [],
    i;
  for (i = 0; i < arr.length; i += chunkSize) {
    groups.push(arr.slice(i, i + chunkSize));
  }
  return groups;
}
exports.chunk = chunk;

function batchSend(currentMetricsBatch, cloudwatchApi, callback) {
  _.forEach(currentMetricsBatch, function(metrics, namespace) {
    // Chunk into groups of 20
    var chunkedGroups = chunk(metrics, 20);
    for (var i = 0, len = chunkedGroups.length; i < len; i++) {
      cloudwatchApi.putMetricData(
        {
          MetricData: chunkedGroups[i],
          Namespace: namespace,
        },
        callback
      );
    }
  });
}
exports.batchSend = batchSend;

function createCounterMetrics(metrics, config, timestamp) {
  // put all currently accumulated counter metrics into an array
  var currentCounterMetrics = {};
  for (var key in metrics) {
    if (key.indexOf("statsd.") == 0) continue;

    if (!isWhitelisted(key, config.whitelist)) {
      continue;
    }

    var names = config.processKeyForNamespace ? processKey(key) : {};
    var namespace = names.namespace || config.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = config.metricName || names.metricName || key;

    if (_.isNil(currentCounterMetrics[namespace])) {
      currentCounterMetrics[namespace] = [];
    }
    currentCounterMetrics[namespace].push({
      MetricName: metricName,
      Unit: "Count",
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: metrics[key],
    });
  }
  console.log("Counter metrics created : " + JSON.stringify(currentCounterMetrics));
  return currentCounterMetrics;
}
exports.createCounterMetrics = createCounterMetrics;

function createTimerMetrics(metrics, config, timestamp) {
  var currentTimerMetrics = {};
  for (var key in metrics) {
    if (metrics[key].length > 0) {
      if (!isWhitelisted(key, config.whitelist)) {
        continue;
      }

      var values = metrics[key].sort(function(a, b) {
        return a - b;
      });
      var count = values.length;
      var min = values[0];
      var max = values[count - 1];

      var cumulativeValues = [min];
      for (var i = 1; i < count; i++) {
        cumulativeValues.push(values[i] + cumulativeValues[i - 1]);
      }

      var sum = cumulativeValues[count - 1];

      var names = config.processKeyForNamespace ? processKey(key) : {};
      var namespace = names.namespace || config.namespace || "AwsCloudWatchStatsdBackend";
      var metricName = config.metricName || names.metricName || key;

      if (_.isNil(currentTimerMetrics[namespace])) {
        currentTimerMetrics[namespace] = [];
      }

      currentTimerMetrics[namespace].push({
        MetricName: metricName,
        Unit: "Milliseconds",
        Timestamp: new Date(timestamp * 1000).toISOString(),
        StatisticValues: {
          Minimum: min,
          Maximum: max,
          Sum: sum,
          SampleCount: count,
        },
      });
    }
  }
  console.log("Timer metrics created : " + JSON.stringify(currentTimerMetrics));
  return currentTimerMetrics;
}
exports.createTimerMetrics = createTimerMetrics;

function createGaugeMetrics(metrics, config, timestamp) {
  var currentGaugeMetrics = {};
  for (var key in metrics) {
    if (!isWhitelisted(key, config.whitelist)) {
      continue;
    }

    var names = config.processKeyForNamespace ? processKey(key) : {};
    namespace = names.namespace || config.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = config.metricName || names.metricName || key;

    if (_.isNil(currentGaugeMetrics[namespace])) currentGaugeMetrics[namespace] = [];

    currentGaugeMetrics[namespace].push({
      MetricName: metricName,
      Unit: "None",
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: metrics[key],
    });
  }
  console.log("Gauge metrics created : " + JSON.stringify(currentGaugeMetrics));
  return currentGaugeMetrics;
}
exports.createGaugeMetrics = createGaugeMetrics;

function createSetMetrics(metrics, config, timestamp) {
  var currentSetMetrics = {};
  for (var key in metrics) {
    if (!isWhitelisted(key, config.whitelist)) {
      continue;
    }

    var names = config.processKeyForNamespace ? processKey(key) : {};
    var namespace = names.namespace || config.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = config.metricName || names.metricName || key;

    if (_.isNil(currentSetMetrics[namespace])) currentSetMetrics[namespace] = [];
    currentSetMetrics[namespace].push({
      MetricName: metricName,
      Unit: "None",
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: metrics[key].values().length,
    });
  }
  console.log("Set metrics created : " + JSON.stringify(currentSetMetrics));
  return currentSetMetrics;
}
exports.createSetMetrics = createSetMetrics;

function flush(timestamp, metrics, cloudwatchApi, config) {
  console.log("Flushing metrics at " + new Date(timestamp * 1000).toISOString());

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;

  function batchCallback(err) {
    if (err) {
      console.log(util.inspect(err));
    }
  }

  var currentCounterMetrics = createCounterMetrics(counters, config, timestamp);
  batchSend(currentCounterMetrics, cloudwatchApi, batchCallback);

  var currentTimerMetrics = createTimerMetrics(timers, config, timestamp);
  batchSend(currentTimerMetrics, cloudwatchApi, batchCallback);

  var currentGaugeMetrics = createGaugeMetrics(gauges, config, timestamp);
  batchSend(currentGaugeMetrics, cloudwatchApi, batchCallback);

  var currentSetMetrics = createSetMetrics(sets, config, timestamp);
  batchSend(currentSetMetrics, cloudwatchApi, batchCallback);
}
exports.flush = flush;

exports.init = function(startupTime, config, events) {
  var cloudwatch = config.cloudwatch || {};
  var instances = cloudwatch.instances || [cloudwatch];
  for (var key in instances) {
    var instanceConfig = instances[key];
    console.log("Starting cloudwatch reporter instance in region:", instanceConfig.region);
    new CloudwatchBackend(startupTime, instanceConfig, events);
  }
  return true;
};
