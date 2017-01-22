var fsp = require('fs-promise');
var fs = require('fs');
var redis = require('redis');
var bluebird = require('bluebird');
var mkdirp = require('mkdirp');

var MAX_RETRY = 3;

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

var Promise = bluebird;

var _ = require('underscore');

var Kewt = require('./kewt.js');

var videoDownloadQ = new Kewt('video-downloads', {
    clearBeforeInit: true,
    traceErrors: false
});
// var screenCapperQ = new Kewt('screencaps');

videoDownloadQ.init(function(kewt) {
    var itemsToPush = [];
    for (var i = 0; i < 5; i++) {
        itemsToPush.push(i);
    }

    return kewt.pushToQueue('todo', itemsToPush);
});

videoDownloadQ.checkCompletion(function(kewt, queueLengths) {
    if (queueLengths.todo == 0) {
        return false;
    } else {
        return false;
    }
});

videoDownloadQ.worker('todo', {
    promiseConcurrency: 1,
    processConcurrency: 1,
    promise: function(kewt, item) {
        return kewt.log('todo item' + item.toString())
        .then(function() {
            throw new RangeError("Hello from rangerror for " + item.toString())
        })
    },
    error: function(kewt, item, err) {
        return kewt.log('pushToRetry ' + item + ' (' + err.toString() + ')')
        .then(function() {
            return kewt.pushToRetry(item);
        })
    }
});

videoDownloadQ.worker('retry', {
    promiseConcurrency: 1,
    processConcurrency: 1,
    promise: function(kewt, item, retryNum) {
        throw new RangeError("Goodbye from rangeerror on retry num " + retryNum.toString() + " for " + item.toString());
        // return kewt.log("Retry worker item " + item.toString() + " retryNum " + retryNum.toString())
        // .then(function() {
        //     return kewt.logerror(retryNum.toString());
        // })
        // return kewt.log('Retry number ' + retryNum.toString() + ' for item ' + item.toString());
        // return Promise.resolve();
    },
    error: function(kewt, item, err, retryNum) {
        console.log('retry error handler for item ' + item.toString() + ' retryNum ' + retryNum.toString());

        if (retryNum >= 3) {
            console.log('over max retry');

            return kewt.log('Push ' + item + ' to failed')
            .then(function() {
                return kewt.pushToFailed(item);
            });
        } else {
            console.log('UNDER max retry');
            return kewt.log('Push ' + item + ' to retryNum ' + retryNum.toString())
            .then(function() {
                return kewt.pushToRetry(item, retryNum + 1);
            });
        }
    }
});

videoDownloadQ.run()
.then(function() {
    console.log('Kewt complete')
})
.catch(function(e) {
    console.log('Kewt error:');
    console.log(e);
    console.log(e.stack);
});
