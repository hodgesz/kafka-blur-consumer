package blur.kafka;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class KafkaServerTest {
	private CuratorFramework _zookeeper;
	private KafkaTestBroker _broker;
	private SimpleConsumer _simpleConsumer;
	KafkaBlurConfig _config;
	ZkState _zkState;
	private final String _port = "49123";
	private final String _topic = "testtopic";
	private String _indexDir = "./target/tmp";
	private String _message;

	@Before
	public void setUp() {

		try {
			_broker = new KafkaTestBroker(_indexDir);
			String[] zkCon = _broker.getBrokerConnectionString().split(":");
			Properties props = new Properties();
			props.setProperty("zookeeper.broker.path", "/brokers");
			props.setProperty("zookeeper.blur.consumer.path", "kafka-blur");
			props.setProperty("zookeeper.hosts", zkCon[0]);
			props.setProperty("zookeeper.port", zkCon[1]);
			props.setProperty("kafka.topic", _topic);
			props.setProperty("kafka.blur.consumer.id", "1234");
			props.setProperty("blur.controller.connection", "localhost:40010");
			props.setProperty("blur.table.name", "testtable");
			props.setProperty("blur.indexer.class", "blur.kafka.TestIndexer");

			_config = new KafkaBlurConfig(props);
			_zkState = new ZkState(_config);
			_zookeeper = CuratorFrameworkFactory.newClient(_broker
					.getBrokerConnectionString(), new RetryNTimes(5, 1000));
			_simpleConsumer = new SimpleConsumer("localhost",
					_broker.getPort(), 60000, 1024, "testClient");
			_zookeeper.start();
			_message = createTopicAndSendMessage();
		} catch (Exception e) {

		}

	}

	@After
	public void tearDown() {
		try {
			_simpleConsumer.close();
			_broker.shutdown();
		} catch (Exception e) {

		}
	}

	/*
	 * These are Kafka Related Test
	 */

	private String createTopicAndSendMessage() {
		Properties props = new Properties();
		props.setProperty("metadata.broker.list", "localhost:" + _port);
		props.put("producer.type", "sync");
		ProducerConfig producerConfig = new ProducerConfig(props);
		kafka.javaapi.producer.Producer<byte[], byte[]> producer = new kafka.javaapi.producer.Producer<byte[], byte[]>(
				producerConfig);
		String value = "testvalue";
		String id = "1";
		KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(
				_topic, id.getBytes(), value.getBytes());
		producer.send(data);
		producer.close();
		return value;
	}

	@Test
	public void sendMessageAndAssertValueForOffset() {
		ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(
				_config,
				_simpleConsumer,
				new Partition(Broker.fromString(_broker
						.getBrokerConnectionString()), 0), 0);
		String message = new String(Utils.toByteArray(messageAndOffsets
				.iterator().next().message().payload()));
		assertThat(message, is(equalTo(_message)));
	}
}
