# nifi-metrics

if we want to use nifi... we need to build it ourselves
sudo docker pull openjdk:8-jdk-alpine

https://www.elastic.co/guide/en/elasticsearch/reference/6.4/docker.html
sudo docker pull elasticsearch:2.3.3

https://www.elastic.co/guide/en/kibana/current/docker.html
sudo docker pull kibana:4.5.1

run as:
docker-compose up

Connect to Kibana with: http://127.0.0.1:5601

## create index

curl -XPUT 'http://localhost:9200/nifi/reporting/_index' -d @nifi-elasticsearch-reporting-bundle/nifi-elasticsearch-reporting/src/main/resources/index.json

## check if index exists

curl -I 'http://localhost:9200/nifi/reporting'

## associate mapping

curl -XPUT 'http://localhost:9200/nifi/reporting/_mapping' -d @nifi-elasticsearch-reporting-bundle/nifi-elasticsearch-reporting/src/main/resources/mappings.json

## add some data

curl -XPOST 'http://localhost:9200/nifi/reporting' -d @tmp/sample.json

## list indices

http://localhost:9200/_cat/indices?v

## view all contents of index

curl -H 'Content-Type: application/json' -X GET http://localhost:9200/nifi/_search?pretty

## Examples to get started with 

https://www.elastic.co/guide/en/kibana/3.0/import-some-data.html

https://www.elastic.co/guide/en/kibana/4.5/getting-started.html

https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-put-mapping.html

https://www.elastic.co/guide/en/elasticsearch/reference/5.5/general-recommendations.html

nested mapping: https://www.elastic.co/guide/en/elasticsearch/guide/current/nested-mapping.html


