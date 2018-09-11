var AWS = require("aws-sdk");
var util = require("util");

function CloudwatchBackend(startupTime, config, emitter) {
  var self = this;

  this.config = config || {};
  AWS.config = this.config;

  function setEmitter() {
    var cloudwatchApi = emitter.on("flush", function(timestamp, metrics) {
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

function isWhitelisted(key, whitelist) {
  if (whitelist && whitelist.length > 0 && whitelist.indexOf(key) >= 0) {
    return true;
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

function batchSend(currentMetricsBatch, namespace, cloudwatchApi, callback) {
  // send off the array (instead of one at a time)
  if (currentMetricsBatch.length > 0) {
    // Chunk into groups of 20
    var chunkedGroups = chunk(currentMetricsBatch, 20);
    for (var i = 0, len = chunkedGroups.length; i < len; i++) {
      cloudwatchApi.putMetricData(
        {
          MetricData: chunkedGroups[i],
          Namespace: namespace,
        },
        callback
      );
    }
  }
}
exports.batchSend = batchSend;

function createCounterMetrics(metrics, config, timestamp) {
  // put all currently accumulated counter metrics into an array
  var currentCounterMetrics = [];
  var namespace = "AwsCloudWatchStatsdBackend";
  for (var key in metrics) {
    if (key.indexOf("statsd.") == 0) continue;

    if (!isWhitelisted(key, config.whitelist)) {
      continue;
    }

    var names = config.processKeyForNamespace ? processKey(key) : {};
    namespace = config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = config.metricName || names.metricName || key;

    currentCounterMetrics.push({
      MetricName: metricName,
      Unit: "Count",
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: metrics[key],
    });
  }
  console.log("Counter metrics created : " + JSON.stringify(currentCounterMetrics) + " with namespace : " + namespace);
  return { metrics: currentCounterMetrics, namespace: namespace };
}

function createTimerMetrics(metrics, config, timestamp) {
  var currentTimerMetrics = [];
  var namespace = "AwsCloudWatchStatsdBackend";
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
      namespace = config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
      var metricName = config.metricName || names.metricName || key;

      currentTimerMetrics.push({
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
  console.log("Timer metrics created : " + JSON.stringify(currentTimerMetrics) + " with namespace : " + namespace);
  return { metrics: currentTimerMetrics, namespace: namespace };
}

exports.createTimerMetrics = createTimerMetrics;

function createGaugeMetrics(metrics, config, timestamp) {
  var currentGaugeMetrics = [];
  var namespace = "AwsCloudWatchStatsdBackend";
  for (var key in metrics) {
    if (!isWhitelisted(key, config.whitelist)) {
      continue;
    }

    var names = config.processKeyForNamespace ? processKey(key) : {};
    namespace = config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = config.metricName || names.metricName || key;

    currentGaugeMetrics.push({
      MetricName: metricName,
      Unit: "None",
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: metrics[key],
    });
  }
  console.log("Gauge metrics created : " + JSON.stringify(currentGaugeMetrics) + " with namespace : " + namespace);
  return { metrics: currentGaugeMetrics, namespace: namespace };
}

function createSetMetrics(metrics, config, timestamp) {
  var currentSetMetrics = [];
  var namespace = "AwsCloudWatchStatsdBackend";
  for (var key in metrics) {
    if (!isWhitelisted(key, config.whitelist)) {
      continue;
    }

    var names = config.processKeyForNamespace ? processKey(key) : {};
    namespace = config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = config.metricName || names.metricName || key;

    currentSetMetrics.push({
      MetricName: metricName,
      Unit: "None",
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: metrics[key].values().length,
    });
  }
  console.log("Set metrics created : " + JSON.stringify(currentSetMetrics) + " with namespace : " + namespace);
  return { metrics: currentSetMetrics, namespace: namespace };
}

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
  batchSend(currentCounterMetrics.metrics, currentCounterMetrics.namespace, cloudwatchApi, batchCallback);

  var currentTimerMetrics = createTimerMetrics(timers, config, timestamp);
  batchSend(currentTimerMetrics.metrics, currentTimerMetrics.namespace, cloudwatchApi, batchCallback);

  var currentGaugeMetrics = createGaugeMetrics(gauges, config, timestamp);
  batchSend(currentGaugeMetrics.metrics, currentGaugeMetrics.namespace, cloudwatchApi, batchCallback);

  var currentSetMetrics = createSetMetrics(sets, config, timestamp);
  batchSend(currentSetMetrics.metrics, currentSetMetrics.namespace, cloudwatchApi, batchCallback);
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
