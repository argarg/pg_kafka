# pg_kafka

Version: 0.0.1

**pg_kafka** is a PostgreSQL extension to produce messages to Apache Kafka. When combined with PostgreSQL it 
creates a convenient way to get operations and row data without the limits of using LISTEN/NOTIFY.

**pg_kafka** is released under the MIT license (See LICENSE file).

Shout out to the [pg_amqp extension](https://github.com/omniti-labs/pg_amqp) authors. I used their project as 
a guide to teach myself how to write a PostgreSQL extension.

### Version Compatability
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [PostgreSQL](http://www.postgresql.org) 14.4
* [Kafka](http://kafka.apache.org) 7.1
* [librdkafka](https://github.com/edenhill/librdkafka) 1.6

### Requirements
* PostgreSQL
* librdkafka
* libsnappy
* zlib

### Building

To build you will need to install PostgreSQL (for pg_config) and PostgreSQL server development packages. On Debian 
based distributions you can usually do something like this:

    apt-get install -y postgresql postgresql-server-dev-14
    
You will also need to make sure the librdkafka library and it's header files have been installed. See their Github 
page for further details.

If you have all of the prerequisites installed you should be able to just:

    make && make install

Once the extension has been installed you just need to enable it in postgresql.conf:

    shared_preload_libraries = 'pg_kafka.so'

And restart PostgreSQL.

### Usage
    -- set KARKA_BROKERS OS env variable to the broker, like: KAFKA_BROKERS=localhost:9092
    -- run once
    create extension kafka;
    -- produce a message
    select kafka.produce('test_topic', 'my message');

For something a bit more useful, consider setting this up on a trigger and producing a message for every INSERT, UPDATE, 
and DELETE that happens on a table.

If the kafka schema wasn't auto-created by installing the extension take a look at sql/kafka.sql.
