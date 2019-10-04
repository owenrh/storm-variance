### Storm Variance
This project contains Storm job configurations that use synthetic data to create four different backpressure scenarios used in a [Stream Processing Backpressure Smackdown](http://owenrh.me.uk/blog/2019/09/30/).

### Running Storm
There are a number of ways to install and run Storm depending on your available hardware. I am just going to cover running it locally on a Mac laptop.

#### Run up Zookeeper
Install Zookeeper via Brew, and run up using the following command 
```
brew services start zookeeper
```

#### Run up Storm components
Download the version in question from the [Storm website](https://storm.apache.org/downloads). I used v2.0.

Update the `STORM_HOME/conf/storm.yaml` and make sure the _nimbus.seeds_ are configured as follows:
```
nimbus.seeds: ["127.0.0.1"]
```

Then run up the nimbus, supervisor and UI as follows:
```
storm nimbus
storm supervisor
storm ui
```
... these commands will need to be executed in different terminals.

If successful you should be able to see the supervisor registered in the Storm UI on http://localhost:8080/. 

### Setup Elasticsearch and Kibana for metrics
_TODO_ - if anyone is interested in this raise an issue and I'll sort out the extra info

### Running scenarios
First build this project using `mvn clean package`. Then run the appropriate scenario using:
```
storm jar target/storm-variance-0.1-SNAPSHOT.jar org.apache.storm.flux.Flux --remote <scenario-name>.yaml
```