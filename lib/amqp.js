/**
 *  A Transport for the AMQP protocol for the Client
 *
 *  @module amqp
 *
 *  @copyright 2017, Nimble Ape Ltd.
 *  @license Apache License, Version 2.0
 *  @author Dan Jenkins <dan@nimblea.pe>
 */

'use strict';

var amqp = require('amqplib');
var EventEmitter = require('events').EventEmitter;
var util = require("util");
var strformat = require('strformat');
var uuid = require('uuid');

function Amqp (url, appName) {
  this.channel;
  this.url = url;
  this.appName = appName;
  this.responseCallbacks = {};
  this.conn;
  EventEmitter.call(this);
}

util.inherits(Amqp, EventEmitter);

Amqp.prototype.init = function (cb) {
  var self = this;
  var connection = self.conn = amqp.connect(self.url);

  connection.then(function(conn) {
    conn.createChannel().then(function(ch) {
      self.channel = ch;
      cb();
      ch.consume(self.appName, function(dialogMessage) {
        if (dialogMessage && dialogMessage.content) {
          var dialogMsg = JSON.parse(dialogMessage.content.toString());
          ch.ack(dialogMessage);

          setTimeout(function () {
            ch.consume('events_' + dialogMsg.dialog_id, function(event) {
              if (event && event.content) {
                var msg = JSON.parse(event.content.toString());
                var ariBody =  JSON.parse(msg.ari_body);
                ariBody.proxyDialogId = dialogMsg.dialog_id;
                ch.ack(event);
                self.emit('events', ariBody);
              }
            });

            ch.consume('responses_' + dialogMsg.dialog_id, function(event) {
              if (event && event.content) {
                var msg = JSON.parse(event.content.toString());
                ch.ack(event);
                //go get the callback for this unique_id
                if (self.responseCallbacks[msg.unique_id]) {
                  //go call the right callback depending on the statusCode
                  if (msg.status_code < 400) {
                    if (self.responseCallbacks[msg.unique_id].success instanceof Function) {
                      console.log('Response', msg.unique_id);
                      self.responseCallbacks[msg.unique_id].success({data:msg.response_body});
                    }
                  } else {
                    if (self.responseCallbacks[msg.unique_id].error instanceof Function) {
                      self.responseCallbacks[msg.unique_id].error({data:msg.response_body});
                    }
                  }

                  delete self.responseCallbacks[msg.unique_id];
                }
              }
            });
          }, 1000);//shouldnt do a timeout... think of a better way
        }
      });
    });
  }).then(null, console.warn);
};

Amqp.prototype.start = function (apps, cb) {
  cb();
};

Amqp.prototype.send = function(id, path, method, options, successCb, errorCb) {
  var self = this;
  //replace {something} in the url with data from the options
  var path = strformat(path, options);

  var unique_id = uuid.v4();

  this.responseCallbacks[unique_id] = {
    success: successCb,
    error: errorCb
  };

  var message = {
    unique_id: unique_id,
    url: path,
    method: method,
    body: JSON.stringify(options)
  };

  var queue = 'commands_' + id;

  console.log('Sending message to queue',queue, message);

  //console.log('checking queue exists', message);
  this.channel.checkQueue(queue).then(function () {
    // console.log('queue exists');
    // console.log('sending message');
    self.channel.sendToQueue(queue, new Buffer(JSON.stringify(message)));

  }, function (err) {
    console.log('queue', queue, 'doesn\'t exist', err);
  });
};

Amqp.prototype.disconnect = function () {
  this.conn.close();
}

module.exports = Amqp;
