# qconsf-python

Basic Apache Kafka Python clients to interact with Confluent Cloud clusters and Schema Registry.

## Configurations

For your convenience, a config file has been created in the `./config` directory as `config.yaml.sample`. To get started, update the API Key and Secret parameters as well as the Sc
hema Registry API Key and Secret. Also confirm that your bootstrap server is properly set. Without these, the clients will not be able to connect to your
cluster.

## Running

Before running these commands, remember to run `pip install -r requirements.txt`. 

Keep in mind that the producer and consumer clients have been set up to interact with the 'users' topic on Confluent Cloud.

Excute producer code by running:

`python3 user_producer.py`

Execute the consumer code by running: 

`python3 user_consumer.py`

## Altering Clients

This sample repository is meant to provide an easy sandbox environment to interact with consumers and producers. Review the [producer](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html) and [consumer](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html) configuration parameters from the workshop and those included in `./config/config.yaml`.

You may also choose to create a `pageviews` producer and consumer or update the `./helpers/clients.py` file to include additional datasets and schemas.