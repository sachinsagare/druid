#! /bin/bash

sed -i "s/<DRUID_CLUSTER_NAME>/${TELETRAAN_DRUID_CLUSTER_NAME}/g" /opt/druid/conf/druid/_common/common.runtime.properties
sed -i "s/<DRUID_ZK_SERVICE_HOST>/${TELETRAAN_DRUID_ZK_SERVICE_HOST}/g" /opt/druid/conf/druid/_common/common.runtime.properties

sed -i "s/<DRUID_OVERSHADOW_CHILDPARENT_MAP>/${TELETRAAN_DRUID_OVERSHADOW_CHILDPARENT_MAP:-}/g" /opt/druid/conf/druid/_common/common.runtime.properties

sed -i "s/<STATS_SEGMENT_TIME_BREAKDOWN_THRESHOLD>/${TELETRAAN_STATS_SEGMENT_TIME_BREAKDOWN_THRESHOLD:-1000000}/g" /opt/druid/conf/druid/_common/metricDimensions.json

export KNOX_MACHINE_AUTH=$(hostname)
mysql_creds=$(knox get mysql:rbac:longqueryrw:credentials)
mysql_user=$(cut -d'@' -f1 <<< ${mysql_creds})

sed -i "s/<MYSQL_USER>/${mysql_user}/" /opt/druid/conf/druid/_common/common.runtime.properties

nohup /opt/druid/bin/monitor_mysql_master.sh /var/druid/metadata.uri /var/config/config.services.general_mysql_databases_config >> /var/log/druid/mysql_monitor.log 2>&1 &

DRUID_LIB_DIR=/opt/druid/lib
DRUID_CONF_DIR=/opt/druid/conf/druid
DRUID_LOG_DIR=/var/log/druid

node='unset'

case ${STAGE_NAME} in
  *"master"* | *"coordinator"*)
    node=coordinator
    sed -i "s/<MEM_MIN>/${TELETRAAN_DRUID_MEM:-30}/" $DRUID_CONF_DIR/coordinator/jvm.config
    sed -i "s/<MEM_MAX>/${TELETRAAN_DRUID_MEM:-30}/" $DRUID_CONF_DIR/coordinator/jvm.config
    sed -i "s/<COORDINATOR_PORT>/${TELETRAAN_COORDINATOR_PORT:-9090}/" $DRUID_CONF_DIR/coordinator/runtime.properties

    if [ ! -z "${TELETRAAN_DRUID_TIER_MIRRORING_MAP}" ]; then
      if grep -q "druid.coordinator.tier.tierMirroringMap" $DRUID_CONF_DIR/coordinator/runtime.properties; then
        sed -i "s/^druid.coordinator.tier.tierMirroringMap=.*$/druid.coordinator.tier.tierMirroringMap=${TELETRAAN_DRUID_TIER_MIRRORING_MAP}/" $DRUID_CONF_DIR/coordinator/runtime.properties
      else
        sed -i "\$adruid.coordinator.tier.tierMirroringMap=${TELETRAAN_DRUID_TIER_MIRRORING_MAP}" $DRUID_CONF_DIR/coordinator/runtime.properties
       fi
    fi

    if [[ "${TELETRAAN_ENABLE_REMOTE_DEBUGGING}" = "TRUE" ]]; then
      cat << EOF >> $DRUID_CONF_DIR/coordinator/jvm.config
-Xrunjdwp:transport=dt_socket,server=y,address=3001,suspend=y
-Xdebug
EOF
    fi

  ;;
  *"overlord"*)
    node=overlord
    sed -i "s/<OVERLORD_PORT>/${TELETRAAN_OVERLORD_PORT:-9090}/" $DRUID_CONF_DIR/overlord/runtime.properties
    sed -i "s/<MEM_MIN>/${TELETRAAN_DRUID_MEM:-3}/" $DRUID_CONF_DIR/overlord/jvm.config
    sed -i "s/<MEM_MAX>/${TELETRAAN_DRUID_MEM:-3}/" $DRUID_CONF_DIR/overlord/jvm.config

    if [[ "${TELETRAAN_ENABLE_REMOTE_DEBUGGING}" = "TRUE" ]]; then
      cat << EOF >> $DRUID_CONF_DIR/overlord/jvm.config
-Xrunjdwp:transport=dt_socket,server=y,address=3002,suspend=y
-Xdebug
EOF
    fi
  ;;
  *"router"*)
    node=router

    if [[ "${TELETRAAN_ENABLE_REMOTE_DEBUGGING}" = "TRUE" ]]; then
      cat << EOF >> $DRUID_CONF_DIR/router/jvm.config
-Xrunjdwp:transport=dt_socket,server=y,address=3003,suspend=y
-Xdebug
EOF
    fi
  ;;
  *"query"*)
    sed -i "s/<HTTP_CLIENT_CONNECTIONS>/${TELETRAAN_HTTP_CLIENT_CONNECTIONS:-300}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<HTTP_SERVER_THREADS>/${TELETRAAN_HTTP_SERVER_THREADS:-60}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<HTTP_MAX_QUEUED_BYTES>/${TELETRAAN_HTTP_MAX_QUEUED_BYTES:-14680064}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_BROKER_HTTP_UNUSED_CONNECTION_TIMEOUT>/${TELETRAAN_DRUID_BROKER_HTTP_UNUSED_CONNECTION_TIMEOUT:-PT50S}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_BROKER_HTTP_READ_TIMEOUT>/${TELETRAAN_DRUID_BROKER_HTTP_READ_TIMEOUT:-PT15M}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_SERVER_HTTP_MAX_IDLE_TIME>/${TELETRAAN_DRUID_SERVER_HTTP_MAX_IDLE_TIME:-PT5M}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<THREADS>/${TELETRAAN_DRUID_THREADS:-7}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<PROCESSING_BUFFER_SIZEBYTES>/${TELETRAAN_PROCESSING_BUFFER_SIZEBYTES:-1073741824}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<PROCESSING_NUM_MERGE_BUFFER>/${TELETRAAN_PROCESSING_NUM_MERGE_BUFFER:-4}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<GROUP_BY_INTERMEDIATE_COMBINE_DEGREE>/${TELETRAAN_GROUP_BY_INTERMEDIATE_COMBINE_DEGREE:-8}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<GROUP_BY_PARALLEL_COMBINE_THREADS>/${TELETRAAN_GROUP_BY_PARALLEL_COMBINE_THREADS:-16}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_BROKER_BALANCER_TYPE>/${TELETRAAN_DRUID_BROKER_BALANCER_TYPE:-connectionCount}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<USE_APPROXIMATE_COUNT_DISTINCT>/${TELETRAAN_USE_APPROXIMATE_COUNT_DISTINCT:-true}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<USE_APPROXIMATE_TOPN>/${TELETRAAN_USE_APPROXIMATE_TOPN:-true}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_USE_PARALLEL_MERGE_POOL>/${TELETRAAN_DRUID_PROCESSING_MERGE_USE_PARALLEL_MERGE_POOL:-false}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_POOL_PARALLELISM>/${TELETRAAN_DRUID_PROCESSING_MERGE_POOL_PARALLELISM:--1}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_POOL_DEFAULT_MAX_QUERY_PARALLELISM>/${TELETRAAN_DRUID_PROCESSING_MERGE_POOL_DEFAULT_MAX_QUERY_PARALLELISM:-1}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_POOL_AWAIT_SHUT_DOWN_MILLIS>/${TELETRAAN_DRUID_PROCESSING_MERGE_POOL_AWAIT_SHUT_DOWN_MILLIS:-60000}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_TASK_TARGET_RUN_TIME_MILLIS>/${TELETRAAN_DRUID_PROCESSING_MERGE_TASK_TARGET_RUN_TIME_MILLIS:-100}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_TASK_INITIAL_YIELD_NUM_ROWS>/${TELETRAAN_DRUID_PROCESSING_MERGE_TASK_INITIAL_YIELD_NUM_ROWS:-16384}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_PROCESSING_MERGE_TASK_SMALL_BATCH_NUM_ROWS>/${TELETRAAN_DRUID_PROCESSING_MERGE_TASK_SMALL_BATCH_NUM_ROWS:-4096}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_SERVER_HTTP_DEFAULT_QUERY_TIMEOUT>/${TELETRAAN_DRUID_SERVER_HTTP_DEFAULT_QUERY_TIMEOUT:-300000}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_BROKER_SELECT_TIER>/${TELETRAAN_DRUID_BROKER_SELECT_TIER:-highestPriority}/" $DRUID_CONF_DIR/broker/runtime.properties
    sed -i "s/<DRUID_BROKER_SELECT_TIER_CUSTOM_PRIORITIES>/${TELETRAAN_DRUID_BROKER_SELECT_TIER_CUSTOM_PRIORITIES:-[]}/" $DRUID_CONF_DIR/broker/runtime.properties

    if [ ! -z "${TELETRAAN_DATASOURCE_COMPLEMENT_MAP}" ]; then
      if grep -q "druid.broker.dataSourceComplement.mapping" $DRUID_CONF_DIR/broker/runtime.properties; then
        sed -i "s/^druid.broker.dataSourceComplement.mapping=.*$/druid.broker.dataSourceComplement.mapping=${TELETRAAN_DATASOURCE_COMPLEMENT_MAP}/" $DRUID_CONF_DIR/broker/runtime.properties
      else
        sed -i "\$adruid.broker.dataSourceComplement.mapping=${TELETRAAN_DATASOURCE_COMPLEMENT_MAP}" $DRUID_CONF_DIR/broker/runtime.properties
      fi
    fi

    if [ ! -z "${TELETRAAN_DATASOURCE_MULTI_COMPLEMENT_MAP}" ]; then
      if grep -q "druid.broker.dataSourceMultiComplement.mapping" $DRUID_CONF_DIR/broker/runtime.properties; then
        sed -i "s/^druid.broker.dataSourceMultiComplement.mapping=.*$/druid.broker.dataSourceMultiComplement.mapping=${TELETRAAN_DATASOURCE_MULTI_COMPLEMENT_MAP}/" $DRUID_CONF_DIR/broker/runtime.properties
      else
        sed -i "\$adruid.broker.dataSourceMultiComplement.mapping=${TELETRAAN_DATASOURCE_MULTI_COMPLEMENT_MAP}" $DRUID_CONF_DIR/broker/runtime.properties
      fi
    fi

    sed -i "s/<MEM_MIN>/${TELETRAAN_DRUID_MEM:-8}/" $DRUID_CONF_DIR/broker/jvm.config
    sed -i "s/<MEM_MAX>/${TELETRAAN_DRUID_MEM:-8}/" $DRUID_CONF_DIR/broker/jvm.config
    sed -i "s/<NEW_SIZE>/${TELETRAAN_DRUID_NEW_SIZE:-4}/" $DRUID_CONF_DIR/broker/jvm.config
    sed -i "s/<MAX_DIRECT_MEM>/${TELETRAAN_DRUID_MAX_DIRECT_MEM:-12}/" $DRUID_CONF_DIR/broker/jvm.config

    if [[ "${TELETRAAN_ENABLE_REMOTE_DEBUGGING}" = "TRUE" ]]; then
      cat << EOF >> $DRUID_CONF_DIR/broker/jvm.config
-Xrunjdwp:transport=dt_socket,server=y,address=3004,suspend=y
-Xdebug
EOF
    fi

    node=broker
  ;;
  *"data"*)
    # Data tier differences are managed through Teletraan script configs
    sed -i "s/<HTTP_SERVER_THREADS>/${TELETRAAN_HTTP_SERVER_THREADS:-60}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<THREADS>/${TELETRAAN_DRUID_THREADS:-7}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<PROCESSING_BUFFER_SIZEBYTES>/${TELETRAAN_PROCESSING_BUFFER_SIZEBYTES:-1073741824}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<PROCESSING_NUM_MERGE_BUFFER>/${TELETRAAN_PROCESSING_NUM_MERGE_BUFFER:-4}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<GROUP_BY_INTERMEDIATE_COMBINE_DEGREE>/${TELETRAAN_GROUP_BY_INTERMEDIATE_COMBINE_DEGREE:-8}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<GROUP_BY_PARALLEL_COMBINE_THREADS>/${TELETRAAN_GROUP_BY_PARALLEL_COMBINE_THREADS:-16}/" $DRUID_CONF_DIR/historical/runtime.properties

    sed -i "s/<TIER>/${TELETRAAN_DRUID_TIER:-_default_tier}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_SERVER_PRIORITY>/${TELETRAAN_DRUID_SERVER_PRIORITY:-0}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<MAX_SIZE>/${TELETRAAN_DRUID_MAX_SIZE:-1000000000000}/g" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<LOCATIONS_MAX_SIZE>/${TELETRAAN_DRUID_LOCATIONS_MAX_SIZE:-1000000000000}/g" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_HISTORICAL_CACHE_USE_CACHE>/${TELETRAAN_DRUID_HISTORICAL_CACHE_USE_CACHE:-true}/g" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_HISTORICAL_CACHE_POPULATE_CACHE>/${TELETRAAN_DRUID_HISTORICAL_CACHE_POPULATE_CACHE:-true}/g" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_HISTORICAL_CACHE_SIZE>/${TELETRAAN_DRUID_HISTORICAL_CACHE_SIZE:-2147483647}/g" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_SERVER_HTTP_MAX_IDLE_TIME>/${TELETRAAN_DRUID_SERVER_HTTP_MAX_IDLE_TIME:-PT5M}/g" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_NUM_LOADING_THREADS>/${TELETRAAN_DRUID_NUM_LOADING_THREADS:-10}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_NUM_THREADS_TO_LOAD_SEGMENTS_INTO_PAGE_CACHE_ON_DOWNLOAD>/${TELETRAAN_DRUID_NUM_THREADS_TO_LOAD_SEGMENTS_INTO_PAGE_CACHE_ON_DOWNLOAD:-0}/" $DRUID_CONF_DIR/historical/runtime.properties
    sed -i "s/<DRUID_NUM_THREADS_TO_LOAD_SEGMENTS_INTO_PAGE_CACHE_ON_BOOTSTRAP>/${TELETRAAN_DRUID_NUM_THREADS_TO_LOAD_SEGMENTS_INTO_PAGE_CACHE_ON_BOOTSTRAP:-0}/" $DRUID_CONF_DIR/historical/runtime.properties

    sed -i "s/<MEM_MIN>/${TELETRAAN_DRUID_MEM:-8}/" $DRUID_CONF_DIR/historical/jvm.config
    sed -i "s/<MEM_MAX>/${TELETRAAN_DRUID_MEM:-8}/" $DRUID_CONF_DIR/historical/jvm.config
    sed -i "s/<NEW_SIZE>/${TELETRAAN_DRUID_NEW_SIZE:-4}/" $DRUID_CONF_DIR/historical/jvm.config
    sed -i "s/<MAX_DIRECT_MEM>/${TELETRAAN_DRUID_MAX_DIRECT_MEM:-12}/" $DRUID_CONF_DIR/historical/jvm.config
    sed -i "s/<CUSTOM_JVM_FLAGS>/${TELETRAAN_CUSTOM_JVM_FLAGS:- }/" $DRUID_CONF_DIR/historical/jvm.config

    if [[ "${TELETRAAN_ENABLE_REMOTE_DEBUGGING}" = "TRUE" ]]; then
      cat << EOF >> $DRUID_CONF_DIR/historical/jvm.config
-Xrunjdwp:transport=dt_socket,server=y,address=3005,suspend=y
-Xdebug
EOF
    fi

    node=historical
  ;;
  *"middleManager"*)COORDINATOR_PORT
    node=middleManager
    sed -i "s/<MIDDLE_MANAGER_PORT>/${TELETRAAN_MIDDLE_MANAGER_PORT:-8091}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<DRUID_INDEXER_RUNNER_START_PORT>/${TELETRAAN_DRUID_INDEXER_RUNNER_START_PORT:-8100}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<DRUID_INDEXER_RUNNER_END_PORT>/${TELETRAAN_DRUID_INDEXER_RUNNER_END_PORT:-65535}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<THREADS>/${TELETRAAN_DRUID_THREADS:-7}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<PROCESSING_BUFFER_SIZEBYTES>/${TELETRAAN_PROCESSING_BUFFER_SIZEBYTES:-1073741824}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<HTTP_SERVER_THREADS>/${TELETRAAN_HTTP_SERVER_THREADS:-25}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<DRUID_WORKER_CAPACITY>/${TELETRAAN_DRUID_WORKER_CAPACITY:-7}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<TASK_CUSTOM_JVM_FLAGS>/${TELETRAAN_TASK_CUSTOM_JVM_FLAGS:- }/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<TASK_MEM_MIN>/${TELETRAAN_TASK_MEM_MIN:-3}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<TASK_MEM_MAX>/${TELETRAAN_TASK_MEM_MAX:-3}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<TASK_MAX_DIRECT_MEM>/${TELETRAAN_TASK_MAX_DIRECT_MEM:-10}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<DRUID_INDEXER_TASK_RESTORE_ON_RESTART>/${TELETRAAN_DRUID_INDEXER_TASK_RESTORE_ON_RESTART:-false}/" $DRUID_CONF_DIR/middleManager/runtime.properties
    sed -i "s/<MEM_MIN>/${TELETRAAN_DRUID_MEM:-8}/" $DRUID_CONF_DIR/middleManager/jvm.config
    sed -i "s/<MEM_MAX>/${TELETRAAN_DRUID_MEM:-8}/" $DRUID_CONF_DIR/middleManager/jvm.config
    sed -i "s/<NEW_SIZE>/${TELETRAAN_DRUID_NEW_SIZE:-4}/" $DRUID_CONF_DIR/middleManager/jvm.config

    if [[ "${TELETRAAN_ENABLE_REMOTE_DEBUGGING}" = "TRUE" ]]; then
      cat << EOF >> $DRUID_CONF_DIR/middleManager/jvm.config
-Xrunjdwp:transport=dt_socket,server=y,address=3006,suspend=y
-Xdebug
EOF
    fi
  ;;
  *)
    echo "Unknown stage ${STAGE_NAME}! Unable to automatically start any Druid nodes!"
    exit 1
  ;;
esac

exec java `cat $DRUID_CONF_DIR/$node/jvm.config | xargs` -cp $DRUID_CONF_DIR/_common:$DRUID_CONF_DIR/$node:$DRUID_LIB_DIR/* org.apache.druid.cli.Main server $node >> $DRUID_LOG_DIR/$node.log 2>&1
