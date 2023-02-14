'use strict';

import { CompressionTypes, Kafka } from 'kafkajs';
import { Writable } from 'stream';

const defaults = {
  clientId: 'winston-kafka',
  topic: 'logs'
};

const writableStream = async (options = {}) => {
  options = { ...defaults, ...options };

  const { brokers, clientId, topic, logCreator } = options;

  const kafka = new Kafka({
    clientId,
    brokers,
    logCreator
  });

  const producer = kafka.producer();
  await producer.connect();

  const stream = new Writable();
  stream._write = (chunk, encoding, next) => {
    const msg = {
      value: chunk.toString()
    };

    producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [msg]
    });

    next();
  };

  return stream;
};

export { writableStream };
