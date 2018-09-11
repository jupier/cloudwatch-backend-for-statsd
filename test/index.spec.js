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
    it("should check if the key is white listed", function() {
      var isWhitelisted = lib.isWhitelisted;
      var whitelist = ["metric.name.1", "metric.name.2"];
      expect(isWhitelisted("metric.name.3", whitelist)).to.be.false;
      expect(isWhitelisted("metric.name.1", whitelist)).to.be.true;
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
      var data = _.times(41);
      var chunkedData = _.chunk(data, 20);
      var callback = sinon.spy();
      batchSend(data, "namespace", cloudwatchApi, callback);
      expect(callback.calledThrice).to.be.true;
      expect(callback.firstCall.lastArg).to.deep.equal({
        Namespace: "namespace",
        MetricData: chunkedData[0],
      });
      expect(callback.secondCall.lastArg).to.deep.equal({
        Namespace: "namespace",
        MetricData: chunkedData[1],
      });
      expect(callback.thirdCall.lastArg).to.deep.equal({
        Namespace: "namespace",
        MetricData: chunkedData[2],
      });
    });
  });
});
