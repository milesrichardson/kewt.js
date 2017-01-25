var fsp = require('fs-promise');
var fs = require('fs');
var redis = require('redis');
var bluebird = require('bluebird');
var mkdirp = require('mkdirp');
var spawn = require('child_process').spawn;
var colors = require('colors/safe');
var moment = require('moment');
var stackTrace = require('stack-trace');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

var Promise = bluebird;

var _ = require('underscore');

var Kewt = function(namespace, opts) {
    try {
        sysOpts = this._parseSysArgs(process.argv);
    } catch(e) {
        console.warn('Warning:', 'Could not parse args');
        sysOpts = {
            isMaster: false,
            isWorker: false,
            isLogger: false
        }
        // process.exit(1);
    }

    this.isComplete = false;

    this.namespace = namespace;
    this.isMaster = sysOpts.isMaster;
    this.isWorker = sysOpts.isWorker;
    this.isLogger = sysOpts.isLogger;

    this.opts = {
        clearBeforeInit: true,
        traceErrors: false
    }

    if (typeof opts !== 'undefined') {
        for (key in opts) {
            if (!opts.hasOwnProperty(key)) { continue; }
            if (!this.opts.hasOwnProperty(key)) {
                console.log("Warning: Unrecognized option " + key);
            } else {
                this.opts[key] = opts[key];
            }
        }
    }

    var redisOpts = {
        host: (process.env.REDIS_HOST || 'localhost'),
        port: (parseInt(process.env.REDIS_PORT) || 6379),
    };

    console.log(redisOpts);

    this.client = redis.createClient(redisOpts);

    this.workers = {};

    this.workerArgs = {};

    this.queues = {
        todo: this._keyName('todo'),
        doing: this._keyName('doing'),
        done: this._keyName('done'),
        retry: this._keyName('retry'),
        failed: this._keyName('failed'),
        errorhandling: this._keyName('errorhandling'),
        log: this._keyName('log'),
        errorlog: this._keyName('errorlog')
    };

    this.callbacks = {
        checkCompletion: undefined,
        init: undefined
    };

    this.childprocs = {
        logger: undefined,
        workers: {}
    };

    var kewt = this;
    process.on('SIGINT', function() {
        console.log('Received SIGINT.');
        kewt._cleanupBeforeExit().then(function() {
            process.exit(0);
        });
    });
};

Kewt.prototype.pushToQueue = function(queueName, item) {
    var queue = this._keyName(queueName);
    return this.client.rpushAsync(queue, item);
};

Kewt.prototype.pushToRetry = function(item, retryNum) {
    // return this.pushToQueue('retry', item);
    if (retryNum == undefined) {
        retryNum = 0;
    }

    var itemParts = item.split(':retry:');
    if (itemParts.length > 1) {
        retryNum = parseInt(itemParts[1]) + 1;
    }
    var cleanItem = itemParts[0];

    var kewt = this;

    return kewt.log('pushing to retry ' + cleanItem +':retry:' + retryNum.toString())
    .then(function() {
        return kewt.pushToQueue('retry', cleanItem + ':retry:' + retryNum.toString());
    })
};

Kewt.prototype.pushToFailed = function(item) {
    return this.pushToQueue('failed', item);
};

Kewt.prototype.pushToNamespace = function(namespace, queueName, item) {
    var queue = this._keyNameExternalNamespace(queueName, namespace);
    return this.client.rpushAsync(queue, item);
};

Kewt.prototype.log = function(msg) {
    var logmsg = [this._logPrefix(), msg].join(" ");
    return this.client.lpushAsync(this.queues.log, logmsg)
    .catch(function(e) {
        console.warn("Warning: Error pushing to log:" + e);
        return Promise.resolve();
    });
};

Kewt.prototype.logerror = function(msg) {
    var logmsg = [this._logPrefix(), msg].join(" ");
    return this.client.lpushAsync(this.queues.errorlog, logmsg)
    .catch(function(e) {
        console.warn("Warning: Error pushing to log:" + e);
        return Promise.resolve();
    });
};

Kewt.prototype.getStats = function() {
    var queues = [this.queues.todo, this.queues.retry, this.queues.done, this.queues.failed];

    var self = this;

    var stats = {};

    return self.client.multi()
    .llen(this.queues.todo)
    .llen(this.queues.retry)
    .llen(this.queues.done)
    .llen(this.queues.failed)
    .execAsync()
    .then(function(queueLengths) {
        stats[self.queues.todo] = queueLengths[0];
        stats[self.queues.retry] = queueLengths[1];
        stats[self.queues.done] = queueLengths[2];
        stats[self.queues.failed] = queueLengths[3];

        return Promise.resolve(stats);
    })
};

Kewt.prototype.checkCompletion = function(completionCheckFunc) {
    this.callbacks.checkCompletion = completionCheckFunc;
};

Kewt.prototype.init = function(initFunc) {
    this.callbacks.init = initFunc;
};

Kewt.prototype.WorkerConfigError = function(msg) { this.message = msg; };
Kewt.prototype.WorkerConfigError.prototype = Object.create(Error.prototype);

Kewt.prototype.worker = function(queueName, opts) {

    var errors = [];
    var promiseConcurrency = parseInt(opts.promiseConcurrency)
    var processConcurrency = parseInt(opts.processConcurrency)
    var promiseFunc = opts.promise || errors.push('NO_PROMISE_FUNC')
    var errorFunc = opts.error || errors.push('NO_ERROR_FUNC')

    if (errors.length > 0) {
        _.each(errors, function(err) {
            console.error(err)
        })
        throw new kewt.WorkerConfigError(errors.join(' '));
    }

    var workerName = queueName + 'Worker';

    worker = { queueName: this._keyName(queueName),
               workerName: workerName,
               workerFunc: promiseFunc,
               errorFunc: errorFunc,
               numWorkers: processConcurrency,
               promiseConcurrency: promiseConcurrency };

    // this.client.set(this._keyName('numWorkers:' + workerName), numWorkers);

    this.workers[workerName] = worker;
    this.childprocs.workers[workerName] = [];
};

Kewt.prototype.WorkerSuccess = function() {};
Kewt.prototype.WorkerSuccess.prototype = Object.create(Error.prototype);

Kewt.prototype.PoppedNull = function() {};
Kewt.prototype.PoppedNull.prototype = Object.create(Error.prototype);

Kewt.prototype.WorkerCallbackError = function(wrappedErr) {
    this.wrappedErr = wrappedErr;
}
Kewt.prototype.WorkerCallbackError.prototype = Object.create(Error.prototype);

Kewt.prototype.WorkerErrorHandlerFailed = function(msg) { this.message = msg; };
Kewt.prototype.WorkerErrorHandlerFailed.prototype = Object.create(Error.prototype);

Kewt.prototype.KewtComplete = function(msg) { this.message = msg; };
Kewt.prototype.KewtComplete.prototype = Object.create(Error.prototype);

Kewt.prototype._isKewtError = function(e) {
    return (
            Kewt.prototype.PoppedNull.prototype.isPrototypeOf(e)
        ||  Kewt.prototype.WorkerSuccess.prototype.isPrototypeOf(e)
        ||  Kewt.prototype.WorkerErrorHandlerFailed.prototype.isPrototypeOf(e)
        ||  Kewt.prototype.KewtComplete.prototype.isPrototypeOf(e)
        ||  Kewt.prototype.WorkerConfigError.prototype.isPrototypeOf(e)
    )
};

Kewt.prototype._singleWorkerPromise = function(worker, workerId) {
    var kewt = this;
    var client = kewt.client;
    var item = undefined;
    var retryNum = undefined;
    var itemWithRetryString = undefined;

    return Promise.resolve()
    .then(function() {
        // console.log('worker.queueName', worker.queueName);
        // console.log('kewt.queues.doing', kewt.queues.doing);
        return client.rpoplpushAsync(worker.queueName, kewt.queues.doing);
    })
    .then(function(poppedItem) {
        if (poppedItem == null || poppedItem == undefined) {
            throw new kewt.PoppedNull();
        }

        item = poppedItem;

        if (worker.queueName == kewt.queues.retry) {
            itemWithRetryString = (' ' + item).slice(1);
            retryNum = parseInt(item.split(':retry:')[1]) || 0;
            item = item.split(':retry:')[0];
        }
    })
    .then(function() {
        if (retryNum !== undefined) {
            return worker.workerFunc(kewt, item, retryNum);
        } else {
            return worker.workerFunc(kewt, item);
        }
    })
    .then(function() {
        var itemToPush = (retryNum !== undefined) ? itemWithRetryString : item;

        return kewt.client.multi()
        .lrem(kewt.queues.doing, -1, itemToPush)
        .rpush(kewt.queues.done, item)
        .execAsync()
        .then(function() {
            return kewt.log("" + item + ": success")
        })
    })
    .catch(function(e) {
        if (kewt._isKewtError(e)) {
            throw e
        } else {
            throw new kewt.WorkerCallbackError(e);
        }
    })
    .catch(kewt.WorkerCallbackError, function(e) {
        var moveFromDoingPromise;
        if (retryNum !== undefined) {
            moveFromDoingPromise = client.lremAsync(kewt.queues.doing, -1, itemWithRetryString)
        } else {
            moveFromDoingPromise = client.lremAsync(kewt.queues.doing, -1, item)
        }

        return moveFromDoingPromise
        .then(function() {
            if (kewt.opts.traceErrors && e.wrappedErr.hasOwnProperty('stack')) {
                return kewt.logerror(e.wrappedErr.stack)
            } else {
                return kewt.logerror(e.wrappedErr.toString());
            }
        })
        .then(function() {
            if (retryNum !== undefined) {
                return worker.errorFunc(kewt, item, e.wrappedErr, retryNum);
            } else {
                return worker.errorFunc(kewt, item, e.wrappedErr);
            }
        }).catch(function(e) {
            throw new kewt.WorkerErrorHandlerFailed(e);
        });
    })
    .then(function() {
        return Promise.resolve();
    });
};

Kewt.prototype._recursiveWorkerPromise = function(worker, workerId) {

    var kewt = this;

    var activeWorkersKey = kewt._keyName('activeWorkers:' + worker.workerName);

    return kewt.client.incrAsync(activeWorkersKey)
    .catch(function(e) {
        console.warn('Warning: error from incrAsync:', e);
        return Promise.resolve();
    })
    .then(function() {
        return kewt._singleWorkerPromise(worker, workerId)
    }).then(function() {
        throw new kewt.WorkerSuccess();
    })
    .catch(kewt.WorkerSuccess, function(e) {
        // console.log('Worker success');

        return Promise.resolve();
    })
    .catch(kewt.WorkerErrorHandlerFailed, function(e) {
        console.log('WorkerErrorHandlerFailed:', e);
        console.log(e);
        return Promise.delay(1000);
    })
    .catch(kewt.PoppedNull, function(e) {
        console.log('Popped null, sleep');
        return Promise.delay(1000);
    })
    .catch(function(err) {
        if (Kewt.prototype.isPrototypeOf(e)) {
            console.log("Kewt error");
        } else {
            console.log('Unknown Worker error:' + err);
            console.trace(err);
        }
        return Promise.resolve();
    })
    .then(function() {
        return kewt.client.decrAsync(activeWorkersKey);
    })
    .catch(function(e) {
        console.warn("Warning: error from decrAsync:" + e);
        return Promise.resolve();
    })
    .then(function() {
        return kewt._recursiveWorkerPromise(worker, workerId);
    });
};

Kewt.prototype._logPrefix = function() {

    var kewt = this;

    var workerPrefix = function() {
        return [
            "[" + kewt.namespace + "]",
            "[worker " + kewt.workerOpts.workerName + " " + kewt.workerOpts.workerId.toString() + "]",
        ].join("");
    };

    var masterPrefix = function() {
        return [
            "[" + kewt.namespace + "]",
            "[master]"
        ].join("");
    };

    var str = "[" + new Date().toISOString() + "]";

    if (this.isWorker) {
        str = str.concat(workerPrefix());
    } else if (this.isMaster) {
        str = str.concat(masterPrefix());
    }

    return str.toString();
};

Kewt.prototype._queueLength = function(queue) {
    var kewt = this;
    var rval;
    return Promise.resolve()
    .then(function() {
        return kewt.client.llenAsync(queue);
    })
    .then(function(length) {
        if (length == 0 || length == null) {
            rval = 0;
        } else {
            rval = length;
        }
    })
    .then(function() {
        return Promise.resolve(rval);
    });
};

Kewt.prototype._checkForCompletion = function() {
    var kewt = this;

    var queueLengths = {
        todo: undefined,
        retry: undefined,
        done: undefined,
        failed: undefined
    };

    var fillQueueLengthPromise = function(queue, dest) {
        return Promise.resolve()
        .then(function() {
            return kewt._queueLength(queue)
        })
        .then(function(length) {
            queueLengths[dest] = length;

            return Promise.resolve();
        });
    };

    var lengthPromises = [];
    lengthPromises.push(fillQueueLengthPromise(kewt.queues.todo, 'todo'));
    lengthPromises.push(fillQueueLengthPromise(kewt.queues.retry, 'retry'));
    lengthPromises.push(fillQueueLengthPromise(kewt.queues.done, 'done'));
    lengthPromises.push(fillQueueLengthPromise(kewt.queues.failed, 'failed'));

    return Promise.all(lengthPromises)
    .then(function() {

        var isComplete = false;

        if (kewt.callbacks.checkCompletion !== undefined) {
            isComplete = kewt.callbacks.checkCompletion(kewt, queueLengths);
        } else {
            isComplete = (queueLengths.todo == 0 && queueLengths.retry == 0);
        }

        if (isComplete) {
            kewt.endTime = new moment();
            var duration = kewt.endTime.diff(kewt.startTime, 'seconds');
            console.log('Kewt complete')
            console.log('' + duration + ' seconds');
            console.log(queueLengths);
            throw new kewt.KewtComplete();
        } else {
            return Promise.resolve();
        }
    });
};

Kewt.prototype._recursivelyManageChildProcs = function(itern) {
    if (typeof itern == 'undefined') {
        itern = 0;
    }

    var kewt = this;

    if (this.isComplete == true) {
        console.log('Scrape complete, do not manage child procs.');
        return Promise.resolve();
    }

    var _spawnLoggerIfNecessary = function() {
        if (kewt.childprocs.logger !== undefined) {
            return;
        }

        var cmd = process.argv[0];
        var args = [process.argv[1], 'logger', kewt.namespace];
        var opts = {
            stdio: 'inherit'
        };

        console.log('Spawn logger process');
        kewt.childprocs.logger = spawn(cmd, args, opts);
    }

    var _equalizeWorkerProcs = function(workerName) {

        var numWorkers = kewt.workers[workerName].numWorkers;
        var numWorkersKeyName = 'global:config:numWorkers:' + workerName;

        var _spawnWorker = function(workerId) {

            console.log("Spawn ", workerName, workerId);

            var cmd = process.argv[0];
            var args = [process.argv[1], 'worker', kewt.namespace, workerName, workerId];
            var opts = {
                stdio: 'ignore',
                stdout: 'inherit'
            };

            var workerProc = spawn(cmd, args, opts);

            kewt.childprocs.workers[workerName].push(workerProc);

            return;
        };

        var _destroyOneWorker = function() {
            var workerProcs = kewt.childprocs.workers[workerName];
            var workerProc = workerProcs.splice(-1,1)[0];

            workerProc.kill();
        };

        return kewt.client.getAsync(numWorkersKeyName)
        .then(function(numWorkersResult) {
            // if (numWorkersResult == null) {
            //     // console.log('numWorkersResult is null, fallback to default');
            // } else {
            //     numWorkers = numWorkersResult;
            // }
            if (numWorkersResult !== null) {
                numWorkers = numWorkersResult;
            }
        })
        .catch(function(e) {
            console.warn('Warning: error getting ' + numWorkersKeyName + ':' + e);
            return Promise.resolve()
        })
        .then(function() {
            var workerProcs = kewt.childprocs.workers[workerName];

            // console.log('Currently: ' + workerProcs.length + ' for ' + workerName + ' but should be ' + numWorkers);
            if (workerProcs.length > numWorkers) {
                console.log('Too many workers for ' + workerName);

                while (workerProcs.length > numWorkers) {
                    _destroyOneWorker(workerId);
                }
            } else if (workerProcs.length < numWorkers) {
                console.log('Not enough workers for ' + workerName)
                for (var workerId = workerProcs.length; workerId < numWorkers; workerId++) {
                    _spawnWorker(workerId);
                }
            } else if (workerProcs.length == numWorkers) {
                return Promise.resolve();
            }
        })
        .then(function() {
            return Promise.resolve();
        });
    };

    return Promise.resolve()
    .then(function() {
        _spawnLoggerIfNecessary();
    })
    .then(function() {
        var equalizeWorkerPromises = [];
        for (workerName in kewt.workers) {
            if (!kewt.workers.hasOwnProperty(workerName)) { continue; }

            equalizeWorkerPromises.push(_equalizeWorkerProcs(workerName));
        }

        return Promise.all(equalizeWorkerPromises);
    })
    .then(function() {
        if (itern == 0) {
            console.log('Wait 20 seconds after first process manager iteration');
            itern++;
            return Promise.delay(5000);
        } else {
            return Promise.delay(1000);
        }
    })
    .then(function() {
        return kewt._checkForCompletion();
    })
    .then(function() {
        return kewt._recursivelyManageChildProcs(itern);
    })
    .catch(kewt.KewtComplete, function(e) {
        console.log('---- Kewt is complete. Shutting down in 10 seconds. -----');

        kewt.isComplete = true;
        return Promise.delay(10000).then(function() {
            return kewt._exitMaster("Scrape complete, goodbye.");
        });
    });
};

Kewt.prototype._cleanupBeforeExit = function() {
    var kewt = this;

    return Promise.resolve()
    .then(function() {
        return kewt.client.quitAsync();
    })
};

Kewt.prototype._killChildProcs = function(signal) {
    var kewt = this;

    var procsToKill = [];

    for (workerName in this.childprocs.workers) {
        if (!this.childprocs.workers.hasOwnProperty(workerName)) { continue; }

        for (var workerId = 0 ; workerId < this.childprocs.workers[workerName].length; workerId++) {
            procsToKill.push(this.childprocs.workers[workerName][workerId]);
        }
    }

    var errors = [];

    for (var i = 0; i < procsToKill.length; i++) {
        if (typeof procsToKill[i] == 'undefined') { continue; }
        console.log('Kill child process', procsToKill[i].pid);

        try {
            procsToKill[i].kill('SIGINT');
        } catch (e) {
            errors.push(e);
        }
    }

    if (this.childprocs.logger !== undefined) {
        console.log('Kill logger with SIGTERM');
        this.childprocs.logger.kill();
    }

    if (errors.length > 0) {
        console.log(errors);
        return Promise.reject(errors);
    } else {
        return Promise.delay(5000)
        .then(function() {
            return Promise.resolve();
        });
    }
};

Kewt.prototype._exitMaster = function(msg) {

    var kewt = this;

    return Promise.resolve()
    .then(function() {
        return kewt._killChildProcs();
    })
    .then(function() {
        return kewt._cleanupBeforeExit();
    })
    .then(function() {
        throw new Error("Generic catch all");
    })
    .catch(function(e) {
        console.error(msg);
        process.exit(0);
    });
};

Kewt.prototype.run = function() {
    console.log(process.argv);
    if (this.isMaster) {
        return this._run_master();
    } else if (this.isWorker) {
        return this._run_worker();
    } else if (this.isLogger) {
        return this._run_logger();
    } else {
        throw new Error('Error: not master, worker, or logger');
        process.exit(1);
    }
};

Kewt.prototype._run_master = function() {
    console.log('Running master');

    // this._setupDirectories();
    this.startTime = new moment();

    var kewt = this;

    var clearPromise;
    if (kewt.opts.clearBeforeInit == true) {
        clearPromise = kewt._clearAllKeysInNamespace()
    } else {
        clearPromise = Promise.resolve();
    }

    return clearPromise
    .then(function() {
        return kewt._initNamespace();
    })
    .then(function() {
        console.log('Master process running...');
        return kewt._recursivelyManageChildProcs();
    });
};

Kewt.prototype._run_logger = function() {
    var kewt = this;

    return Promise.resolve()
    .then(function() {
        return kewt.client.brpopAsync(kewt.queues.log, kewt.queues.errorlog, 0);
    })
    .then(function(result) {
        var fromQueue = result[0];
        var message = result[1];

        if (fromQueue == kewt.queues.log) {
            console.log(message);

        } else if (fromQueue == kewt.queues.errorlog) {
            console.error(colors.red(message));
        }
        return Promise.resolve();
    })
    .catch(function(e) {
        console.log('Error popping from log:' + e);
        return Promise.resolve();
    })
    .then(function() {
        return kewt._run_logger();
    });

};

Kewt.prototype._parseWorkerArgs = function(argv) {
    var opts = {
        workerName: argv[4],
        workerId: parseInt(argv[5])
    };

    if (typeof opts.workerName == 'undefined' || typeof opts.workerId == 'undefined') {
        throw "Error parsing worker args";
    }

    this.workerOpts = opts;

    return opts;
};

Kewt.prototype._run_worker = function() {

    try {
        this.workerOpts = this._parseWorkerArgs(process.argv);
    } catch(e) {
        console.error("Error2: ", e);
        process.exit(1);
    }

    var worker = this.workers[this.workerOpts.workerName];

    console.warn('Running worker');

    console.warn('Opts:')
    console.warn(this.workerOpts);

    console.warn('Worker:')
    console.warn(worker);

    console.log()

    var kewt = this;
    return Promise.delay(2000)
    .then(function() {
        return kewt.log("Starting worker")
    }).then(function() {

        if (worker.promiseConcurrency == 1) {
            return kewt._recursiveWorkerPromise(worker, kewt.workerOpts.workerId);
        } else {
            var promises = [];
            for (var i = 0; i < worker.promiseConcurrency; i++) {
                promises.push(kewt._recursiveWorkerPromise(worker, kewt.workerOpts.workerId));
            }
            return Promise.all(promises);
        }
    });
};

Kewt.prototype._parseSysArgs = function(argv) {
    var opts = {
        isMaster: false,
        isWorker: false,
        isLogger: false,
        namespace: undefined
    };

    if (argv[2] == 'worker') {
        opts.isWorker = true;
    } else if (argv[2] == 'logger') {
        opts.isLogger = true;
    } else {
        opts.isMaster = true;
    }

    var namespaceIndex = (opts.isMaster) ? 2 : 3;

    opts.namespace = argv[namespaceIndex];

    if (typeof opts.namespace == 'undefined') {
        throw "Invalid namespace"
    }

    console.log(opts);

    return opts;
}

Kewt.prototype._keyNameExternalNamespace = function(keyName, namespace) {
    return 'kewt:' + namespace + ':' + keyName;
};

Kewt.prototype._keyName = function(keyName) {
    return 'kewt:' + this.namespace + ':' + keyName;
};

Kewt.prototype._getAllKeysInNamespace = function() {
    return this.client.keysAsync('kewt:' + this.namespace + ':*');
};

Kewt.prototype._clearAllKeysInNamespace = function() {
    var kewt = this;

    // return Promise.resolve();

    return this._getAllKeysInNamespace()
    .then(function(keys) {
        var deletePromises = [];

        for (var i = 0; i < keys.length; i++) {
            var key = keys[i];

            kewt.client.del(key, function(err, reply) {
            });
        }
    });
};

Kewt.prototype._initNamespace = function() {
    var kewt = this;

    var initPromise;

    if (kewt.callbacks.init !== undefined) {
        initPromise = kewt.callbacks.init(kewt);
    } else {
        initPromise = Promise.resolve();
    }

    return initPromise
    .then(function() {
        return kewt._initializeWorkerKeys();
    });
};

Kewt.prototype._initializeWorkerKeys = function() {

    var kewt = this;

    var promises = [];
    var keysToSet = {};

    for (workerName in this.workers) {
        if (!this.workers.hasOwnProperty(workerName)) { continue; }

        var worker = this.workers[workerName];

        keysToSet[this._keyName('numWorkers:' + workerName)] = worker.numWorkers;
        keysToSet[this._keyName('activeWorkers:' + workerName)] = 0;
    }

    for (keyName in keysToSet) {
        if (!keysToSet.hasOwnProperty(keyName)) { continue; }
        promises.push(kewt.client.setAsync(keyName, keysToSet[keyName]));
    }

    return Promise.all(promises);
};

module.exports = Kewt;
