#!/bin/bash

unset PREPARE
unset IS_COMPILED
unset PREPARE_TEST
unset PREPARE_OLD
unset PREPARE_TSC

export MAVEN_OPTS='-Xmx12g -Xms4g'
# export MAVEN_OPTS='-Xmx18g -Xms12g'



# Function: print system info of current machine (both hardware and software), no argument needed.
function systemInfo() {
    if [ -z ${IS_COMPILED+x} ]
    then
        IS_COMPILED=' clean compile '
    else
        IS_COMPILED=''
    fi
    mvn -B ${IS_COMPILED} exec:java \
        -Dexec.mainClass="benchmark.utils.RuntimeEnv"
}


########################################### PostgreSQL ###########################################

function runPostgreSQLWriteStaticTest() {
  export DB_HOST=localhost
  export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
  export MAX_CONNECTION_CNT=1
  export VERIFY_RESULT=false
  mvn -B --offline test -Dtest=postgre.WriteStaticPropertyTest
}

function runPostgreSQLWriteTemporalTest() {
  export TEMPORAL_DATA_PER_TX=100
  export TEMPORAL_DATA_START=0501
  export TEMPORAL_DATA_END=0501
  export DB_HOST=localhost
  export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
  export MAX_CONNECTION_CNT=16
  export VERIFY_RESULT=false
  mvn -B --offline test -Dtest=postgre.WriteTemporalPropertyTest
}

function runPostgreSQLUpdateTemporalTest() {
  export DB_HOST=localhost
  export MAX_CONNECTION_CNT=1
  mvn -B --offline test -Dtest=postgre.UpdateTemporalPropertyTest
}

function runPostgreSQLCreateIndexes() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  mvn -B --offline test -Dtest=postgre.CreateIndexes
}

function runPostgreSQLNodeNeighborRoadTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\pg_res\res_node_neighbor_road.gz"
  mvn -B --offline test -Dtest=postgre.NodeNeighborRoadTest
}

function runPostgreSQLSnapshotAggrDurationTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\pg_res\res_snapshot_aggr_duration.gz"
  mvn -B --offline test -Dtest=postgre.SnapshotAggrDurationTest
}

function runPostgreSQLSnapshotAggrMaxTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\pg_res\res_snapshot_aggr_max.gz"
  mvn -B --offline test -Dtest=postgre.SnapshotAggrMaxTest
}

function runPostgreSQLSnapshotTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\pg_res\res_snapshot.gz"
  mvn -B --offline test -Dtest=postgre.SnapshotTest
}

function runPostgreSQLEntityTemporalConditionTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\pg_res\res_entity_temporal_condition.gz"
  mvn -B --offline test -Dtest=postgre.EntityTemporalConditionTest
}

function runPostgreSQLReachableAreaTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export SERVER_RESULT_FILE="C:\Users\yuewa\tgraph\result\postgre\reachable_area.gz"
  mvn -B --offline test -Dtest=postgre.ReachableAreaTest
}

########################################### MariaDB ###########################################

function runMariaDBWriteStaticTest() {
    export DB_HOST=localhost
    export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
    export MAX_CONNECTION_CNT=16
    export VERIFY_RESULT=false
    mvn -B --offline test -Dtest=mariadb.WriteStaticPropertyTest
}

function runMariaDBWriteTemporalTest() {
    export TEMPORAL_DATA_PER_TX=100
    export TEMPORAL_DATA_START=0501
    export TEMPORAL_DATA_END=0501
    export DB_HOST=localhost
    export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
    export MAX_CONNECTION_CNT=1
    export VERIFY_RESULT=false
    mvn -B --offline test -Dtest=mariadb.WriteTemporalPropertyTest
}

function runMariaDBUpdateTemporalTest() {
  export DB_HOST=localhost
  export MAX_CONNECTION_CNT=1
  mvn -B --offline test -Dtest=mariadb.UpdateTemporalPropertyTest
}

function runMariaDBCreateIndexes() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  mvn -B --offline test -Dtest=mariadb.CreateIndexes
}

function runMariaDBSnapshotTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\mariadb_res\res_snapshot.gz"
  mvn -B --offline test -Dtest=mariadb.SnapshotTest
}

function runMariaDBSnapshotAggrMaxTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\mariadb_res\res_snapshot_aggr_max.gz"
  mvn -B --offline test -Dtest=mariadb.SnapshotAggrMaxTest
}

function runMariaDBSnapshotAggrDurationTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\mariadb_res\res_snapshot_aggr_duration.gz"
  mvn -B --offline test -Dtest=mariadb.SnapshotAggrDurationTest
}

function runMariaDBEntityTemporalConditionTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\mariadb_res\res_entity_temporal_condition.gz"
  mvn -B --offline test -Dtest=mariadb.EntityTemporalConditionTest
}

function runMariaDBReachableAreaTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export SERVER_RESULT_FILE="C:\Users\yuewa\tgraph\result\maria\reachable_area.gz"
  mvn -B --offline test -Dtest=mariadb.ReachableAreaTest
}

########################################### Neo4j1 ###########################################

function runNeo4jServer1() {
  export DB_PATH="C:\Users\yuewa\tgraph\data\neo4j1"
  mvn -B --offline compile exec:java -Dexec.mainClass=benchmark.server.Neo4jServer1
}

function runNeo4j1WriteStaticTest() {
  export DB_HOST=localhost
  export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
  export MAX_CONNECTION_CNT=1
  export VERIFY_RESULT=false
  mvn -B --offline test -Dtest=neo4j.WriteStaticPropertyTest
}
function runNeo4j1WriteTemporalTest() {
  export TEMPORAL_DATA_PER_TX=100
  export TEMPORAL_DATA_START=0501
  export TEMPORAL_DATA_END=0501
  export DB_HOST=localhost
  export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
  export MAX_CONNECTION_CNT=1
  export VERIFY_RESULT=false
  mvn -B --offline test -Dtest=neo4j.WriteTemporalPropertyTest
}

function runNeo4j1UpdateTemporalTest() {
  export DB_HOST=localhost
  export MAX_CONNECTION_CNT=1
  mvn -B --offline test -Dtest=neo4j.UpdateTemporalPropertyTest
}

function runNeo4j1SnapshotTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j1_res\res_snapshot.gz"
  mvn -B --offline test -Dtest=neo4j.SnapshotTest
}
function runNeo4j1SnapshotAggrMaxTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j1_res\res_snapshot_aggr_max.gz"
  mvn -B --offline test -Dtest=neo4j.SnapshotAggrMaxTest
}
function runNeo4j1SnapshotAggrDurationTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j1_res\res_snapshot_aggr_duration.gz"
  mvn -B --offline test -Dtest=neo4j.SnapshotAggrDurationTest
}

function runNeo4j1EntityTemporalConditionTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j1_res\res_entity_temporal_condition.gz"
  mvn -B --offline test -Dtest=neo4j.EntityTemporalConditionTest
}

function runNeo4j1ReachableAreaTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export SERVER_RESULT_FILE="C:\Users\yuewa\tgraph\result\neo4j1\reachable_area.gz"
  mvn -B --offline test -Dtest=neo4j.ReachableAreaTest
}


########################################### Neo4j2 ###########################################

function runNeo4jServer2() {
  export DB_PATH="C:\Users\yuewa\tgraph\data\neo4j2"
  mvn -B --offline compile exec:java -Dexec.mainClass=benchmark.server.Neo4jServer2
}

function runNeo4j2WriteStaticTest() {
  export DB_HOST=localhost
  export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
  export MAX_CONNECTION_CNT=1
  export VERIFY_RESULT=false
  mvn -B --offline test -Dtest=neo4j2.WriteStaticPropertyTest
}
function runNeo4j2WriteTemporalTest() {
  export TEMPORAL_DATA_PER_TX=100
  export TEMPORAL_DATA_START=0501
  export TEMPORAL_DATA_END=0501
  export DB_HOST=localhost
  export RAW_DATA_PATH="C:\Users\yuewa\tgraph\test-data-random"
  export MAX_CONNECTION_CNT=1
  export VERIFY_RESULT=false
  mvn -B --offline test -Dtest=neo4j2.WriteTemporalPropertyTest
}

function runNeo4j2UpdateTemporalTest() {
  export DB_HOST=localhost
  export MAX_CONNECTION_CNT=1
  mvn -B --offline test -Dtest=neo4j2.UpdateTemporalPropertyTest
}

function runNeo4j2SnapshotTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j2_res\res_snapshot.gz"
  mvn -B --offline test -Dtest=neo4j2.SnapshotTest
}
function runNeo4j2SnapshotAggrMaxTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j2_res\res_snapshot_aggr_max.gz"
  mvn -B --offline test -Dtest=neo4j2.SnapshotAggrMaxTest
}
function runNeo4j2SnapshotAggrDurationTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j2_res\res_snapshot_aggr_duration.gz"
  mvn -B --offline test -Dtest=neo4j2.SnapshotAggrDurationTest
}

function runNeo4j2EntityTemporalConditionTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export VERIFY_RESULT=false
  export SERVER_RESULT_FILE="E:\tgraph\neo4j2_res\res_entity_temporal_condition.gz"
  mvn -B --offline test -Dtest=neo4j2.EntityTemporalConditionTest
}

function runNeo4j2ReachableAreaTest() {
  export MAX_CONNECTION_CNT=1
  export DB_HOST=localhost
  export SERVER_RESULT_FILE="C:\Users\yuewa\tgraph\result\neo4j2\reachable_area.gz"
  mvn -B --offline test -Dtest=neo4j2.ReachableAreaTest
}
