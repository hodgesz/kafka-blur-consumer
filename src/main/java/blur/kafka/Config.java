package blur.kafka;

import java.util.HashMap;

public class Config extends HashMap<String, Object> {

	/**
	 * A list of hosts of ZooKeeper servers used to manage the cluster.
	 */
	public static final String ZOOKEEPER_HOSTS = "zookeeper.hosts";

	/**
	 * The port Storm will use to connect to each of the ZooKeeper servers.
	 */
	public static final String ZOOKEEPER_PORT = "zookeeper.port";

	/**
	 * Kafka related configurations
	 */
	public static final String KAFKA_TOPIC = "kafka.topic";
	public static final String ZOOKEEPER_BROKER_PATH = "zookeeper.broker.path";

	/**
	 * Blur related configurations
	 */
	public static final String BLUR_TABLE_NAME = "blur.table.name";
	public static final String KAFKA_BLUR_CONSUMER_ID = "kafka.blur.consumer.id";
	public static final String BLUR_INDEXER_CLASS = "blur.indexer.class";
	public static final String BLUR_CONTROLLER_CONNECTION = "blur.controller.connection";
	public static final String ZOOKEEPER_BLUR_CONSUMER_PATH = "zookeeper.blur.consumer.path";

}
