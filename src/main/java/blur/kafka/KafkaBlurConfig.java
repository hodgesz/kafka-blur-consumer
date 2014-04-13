package blur.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaBlurConfig implements Serializable {

	public int _fetchSizeBytes = 64 * 1024;
	public int _socketTimeoutMs = 10000;
	public int _bufferSizeBytes = 64 * 1024;

	public int _refreshFreqSecs = 120;

	public boolean _forceFromStart = false;
	public long _startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public boolean _useStartOffsetTimeIfOffsetOutOfRange = true;

	public long _stateUpdateIntervalMs = 10000;
	public Map _stateConf;

	public KafkaBlurConfig(Properties props) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		String brokerZkPath = props.getProperty("zookeeper.broker.path");
		String blurConsumerZkPath = props
				.getProperty("zookeeper.blur.consumer.path");
		String zkHost = props.getProperty("zookeeper.hosts");
		String zkPort = props.getProperty("zookeeper.port");

		String kafkaTopic = props.getProperty("kafka.topic");

		String blurConsumerId = props.getProperty("kafka.blur.consumer.id");
		String blurControllerConn = props
				.getProperty("blur.controller.connection");
		String blurTableName = props.getProperty("blur.table.name");
		String blurIndexerClass = props.getProperty("blur.indexer.class");

		_stateConf = new HashMap();
		List<String> zkServers = new ArrayList<String>(Arrays.asList(zkHost
				.split(",")));
		_stateConf.put(Config.ZOOKEEPER_HOSTS, zkServers);
		_stateConf.put(Config.ZOOKEEPER_PORT, zkPort);

		_stateConf.put(Config.KAFKA_TOPIC, kafkaTopic);
		_stateConf.put(Config.ZOOKEEPER_BROKER_PATH, brokerZkPath);

		_stateConf.put(Config.BLUR_TABLE_NAME, blurTableName);
		_stateConf.put(Config.KAFKA_BLUR_CONSUMER_ID, blurConsumerId);
		_stateConf.put(Config.BLUR_INDEXER_CLASS, blurIndexerClass);
		_stateConf.put(Config.BLUR_CONTROLLER_CONNECTION, blurControllerConn);
		_stateConf.put(Config.ZOOKEEPER_BLUR_CONSUMER_PATH, blurConsumerZkPath);

	}

}
