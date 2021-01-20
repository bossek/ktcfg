# ktcfg

Simple utility for setting Kafka topic options.

## Usage

```console
ktcfg -b localhost:9092 -t some_topic -k message.timestamp.type -v LogAppendTime
ktcfg -t some_topic -k message.timestamp.type -v CreateTime
```

## Build

Build requires [librdkafka](https://github.com/edenhill/librdkafka) installed.

```console
make
```
