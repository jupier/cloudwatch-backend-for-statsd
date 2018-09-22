var expect = require("chai").expect;
var sinon = require("sinon");
var _ = require("lodash");

var lib = require("../index");

describe("cloudwatch backend for statsd tests", function() {
  describe("chunk", function() {
    it("should createe an array of elements split into groups the length of size", function() {
      var chunk = lib.chunk;
      var data = [1, 2, 3, 4, 5, 6];
      expect(chunk(data, 3)).to.deep.equal([[1, 2, 3], [4, 5, 6]]);
      expect(chunk(data, 4)).to.deep.equal([[1, 2, 3, 4], [5, 6]]);
      expect(chunk([], 10)).to.deep.equal([]);
    });
  });

  describe("isWhitelisted", function() {
    var isWhitelisted = lib.isWhitelisted;
    it("should check if the key is white listed", function() {
      var whitelist = ["metric.name.1", "metric.name.2"];
      expect(isWhitelisted("metric.name.3", whitelist)).to.be.false;
      expect(isWhitelisted("metric.name.1", whitelist)).to.be.true;
    });
    it("should return true if there is no white listed metrics", function() {
      expect(isWhitelisted("metric.name.3", [])).to.be.false;
      expect(isWhitelisted("metric.name.3", null)).to.be.true;
      expect(isWhitelisted("metric.name.3", undefined)).to.be.true;
    });
    it("should check regexp and string white list metrics", function() {
      expect(isWhitelisted("metric.name.database.error", "metric.name.*")).to.be.true;
      expect(isWhitelisted("a.b.c.d", ".*b.c.*")).to.be.true;
      expect(isWhitelisted("a.b.c.d", ".*b.c.d")).to.be.true;
      expect(isWhitelisted("a.b.c.d", ".*b.c")).to.be.false;
      expect(isWhitelisted("a.b.c.d", "a.b.c.d")).to.be.true;
      expect(isWhitelisted("a.b.c.d", "a.b.c")).to.be.false;
      expect(isWhitelisted("a.b.c.d", "a.b.c.*")).to.be.true;
      expect(isWhitelisted("a.b.c.d", ["b.c.d", "a.*"])).to.be.true;
      expect(isWhitelisted("a.b.c.d", ".*")).to.be.true;
      expect(isWhitelisted("instance3.database.error", "instance[1-9].database.*")).to.be.true;
    });
  });

  describe("processKey", function() {
    it("should parse the statsd key and extract the namespace and the metric name", function() {
      var processKey = lib.processKey;
      expect(processKey("test")).to.deep.equal({
        metricName: "test",
        namespace: null,
      });
      expect(processKey("namespace.test")).to.deep.equal({
        metricName: "test",
        namespace: "namespace",
      });
      expect(processKey("namespace/bucket.name-test")).to.deep.equal({
        metricName: "test",
        namespace: "namespace/bucket/name",
      });
    });
  });

  describe("batchSend", function() {
    var cloudwatchApi = {
      putMetricData: function(data, callback) {
        callback(data);
      },
    };
    it("should send the data to cloudwatch via the putMetricData of aws-sdk library", function() {
      var batchSend = lib.batchSend;
      var data = { namespace1: _.times(21), namespace2: _.times(10) };
      var chunkedDataNamespace1 = _.chunk(_.times(21), 20);
      var chunkedDataNamespace2 = _.chunk(_.times(10), 20);
      var callback = sinon.spy();
      batchSend(data, cloudwatchApi, callback);
      expect(callback.calledThrice).to.be.true;
      expect(callback.firstCall.lastArg).to.deep.equal({
        Namespace: "namespace1",
        MetricData: chunkedDataNamespace1[0],
      });
      expect(callback.secondCall.lastArg).to.deep.equal({
        Namespace: "namespace1",
        MetricData: chunkedDataNamespace1[1],
      });
      expect(callback.thirdCall.lastArg).to.deep.equal({
        Namespace: "namespace2",
        MetricData: chunkedDataNamespace2[0],
      });
    });
  });

  describe("createTimerMetrics", function() {
    it("should return the timer metric data for cloudwatch", function() {
      var createTimerMetrics = lib.createTimerMetrics;
      var timers = createTimerMetrics(
        { test: [1, 3], test2: 2, test4: 4 },
        { namespace: "mynamespace", whitelist: ["test"] },
        Date.now()
      );
      expect(timers.mynamespace).to.have.lengthOf(1);
    });
    it("should return the timer metrics related to the processed key", function() {
      var createTimerMetrics = lib.createTimerMetrics;
      var timers = createTimerMetrics(
        {
          "my.metric.name.request": [1, 2, 3, 8, 9],
          "my.metric2.name.request": [2, 3],
          "my.metric.name.error": [1, 2],
        },
        {
          namespace: "mynamespace",
          processKeyForNamespace: true,
        },
        Date.now()
      );
      expect(timers["my/metric/name"]).to.have.lengthOf(2);
      expect(timers["my/metric2/name"]).to.have.lengthOf(1);
    });
  });

  describe("createGaugeMetrics", function() {
    it("should return the gauge metric data for cloudwatch", function() {
      var createGaugeMetrics = lib.createGaugeMetrics;
      var gauges = createGaugeMetrics(
        { test: 1, test2: 2, test4: 4 },
        { namespace: "mynamespace", whitelist: ["test"] },
        Date.now()
      );
      expect(gauges.mynamespace).to.have.lengthOf(1);
    });
    it("should return the gauge metrics related to the processed key", function() {
      var createGaugeMetrics = lib.createGaugeMetrics;
      var gauges = createGaugeMetrics(
        {
          "my.metric.name.request": 122,
          "my.metric2.name.request": 23,
          "my.metric.name.error": 10,
        },
        {
          namespace: "mynamespace",
          processKeyForNamespace: true,
        },
        Date.now()
      );
      expect(gauges["my/metric/name"]).to.have.lengthOf(2);
      expect(gauges["my/metric2/name"]).to.have.lengthOf(1);
    });
  });

  describe("createSetMetrics", function() {
    it("should return the set metric data for cloudwatch", function() {
      var createSetMetrics = lib.createSetMetrics;
      var sets = createSetMetrics(
        { test: new Set([1]), test2: new Set([1, 2]), test4: new Set([3, 4]) },
        { namespace: "mynamespace", whitelist: ["test"] },
        Date.now()
      );
      expect(sets.mynamespace).to.have.lengthOf(1);
    });
    it("should return the set metrics related to the processed key", function() {
      var createSetMetrics = lib.createSetMetrics;
      var sets = createSetMetrics(
        {
          "my.metric.name.request": new Set([122]),
          "my.metric2.name.request": new Set([23]),
          "my.metric.name.error": new Set([10]),
        },
        {
          namespace: "mynamespace",
          processKeyForNamespace: true,
        },
        Date.now()
      );
      expect(sets["my/metric/name"]).to.have.lengthOf(2);
      expect(sets["my/metric2/name"]).to.have.lengthOf(1);
    });
  });

  describe("createCounterMetrics", function() {
    it("should return the counter metrics data", function() {
      var createCounterMetrics = lib.createCounterMetrics;
      var counters = createCounterMetrics(
        {
          test: 1,
          test2: 2,
          test3: 3,
        },
        {
          namespace: "mynamespace",
        },
        Date.now()
      );
      expect(counters.mynamespace).to.have.lengthOf(3);
    });
    it("should return the counter metrics related to the white listed metrics", function() {
      var createCounterMetrics = lib.createCounterMetrics;
      var counters = createCounterMetrics(
        {
          test: 1,
          test2: 2,
          test3: 3,
        },
        {
          namespace: "mynamespace",
          whitelist: ["test[1-2]*"],
        },
        Date.now()
      );
      expect(counters.mynamespace).to.have.lengthOf(2);
    });
    it("should return the counter metrics related to the processed key", function() {
      var createCounterMetrics = lib.createCounterMetrics;
      var counters = createCounterMetrics(
        {
          "my.metric.name.request": 1,
          "my.metric2.name.request": 2,
          "my.metric2.name.error": 3,
        },
        {
          namespace: "mynamespace",
          processKeyForNamespace: true,
        },
        Date.now()
      );
      expect(counters["my/metric/name"]).to.have.lengthOf(1);
      expect(counters["my/metric2/name"]).to.have.lengthOf(2);
    });
  });
});
