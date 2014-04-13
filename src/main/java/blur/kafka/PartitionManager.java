package blur.kafka;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.RowMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import blur.kafka.client.IIndexer;

import com.google.common.collect.ImmutableMap;

public class PartitionManager {
	public static final Logger LOG = LoggerFactory
			.getLogger(PartitionManager.class);


	Long _emittedToOffset;
	Long _lastComittedOffset;
	Long _lastEnquedOffset;
	int _capacity = 1000;
	Map<Long, Message> _waitingToEmit = new LinkedHashMap<Long, Message>(_capacity);
	Set<Long> _badMessages = new HashSet<Long>();
	Partition _partition;
	KafkaBlurConfig _kafkablurconfig;
	String _blurConsumerId;
	SimpleConsumer _consumer;
	DynamicPartitionConnections _connections;
	ZkState _state;
	String _topic;
	Map _stateConf;
	IIndexer _indexer;

	public PartitionManager(DynamicPartitionConnections connections,
			ZkState state, KafkaBlurConfig kafkablurconfig, Partition partiionId) {
		_partition = partiionId;
		_connections = connections;
		_kafkablurconfig = kafkablurconfig;
		_stateConf = _kafkablurconfig._stateConf;
		_blurConsumerId = (String) _stateConf
				.get(Config.KAFKA_BLUR_CONSUMER_ID);
		_consumer = connections.register(partiionId.host, partiionId.partition);
		_state = state;

		_topic = (String) _stateConf.get(Config.KAFKA_TOPIC);
		

		try {
			_indexer = (IIndexer) (Class.forName((String)kafkablurconfig._stateConf.get(Config.BLUR_INDEXER_CLASS), true, this
					.getClass().getClassLoader()).newInstance());
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String blurConsumerJsonId = null;
		String blurTableName = null;
		Long jsonOffset = null;
		String path = committedPath();
		try {
			Map<Object, Object> json = _state.readJSON(path);
			LOG.info("Read partition information from: " + path + "  --> "
					+ json);
			if (json != null) {
				blurConsumerJsonId = (String) ((Map<Object, Object>) json
						.get("consumer")).get("id");
				blurTableName = (String) ((Map<Object, Object>) json
						.get("consumer")).get("table");
				jsonOffset = (Long) json.get("offset");
			}
		} catch (Throwable e) {
			LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
		}

		if (blurConsumerJsonId == null || jsonOffset == null) { // failed to
																// parse JSON?
			_lastComittedOffset = KafkaUtils.getOffset(_consumer, _topic,
					partiionId.partition, kafkablurconfig);
			LOG.info("No partition information found, using configuration to determine offset");
		} else if (!_stateConf.get(Config.BLUR_TABLE_NAME)
				.equals(blurTableName) && kafkablurconfig._forceFromStart) {
			_lastComittedOffset = KafkaUtils.getOffset(_consumer, _topic,
					partiionId.partition, kafkablurconfig._startOffsetTime);
			LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
		} else {
			_lastComittedOffset = jsonOffset;
			LOG.info("Read last commit offset from zookeeper: " + _lastComittedOffset
					+ "; old topology_id: " + blurConsumerJsonId
					+ " - new consumer_id: " + _blurConsumerId);
		}

		LOG.info("Starting Kafka " + _consumer.host() + ":"
				+ partiionId.partition + " from offset " + _lastComittedOffset);
		_emittedToOffset = _lastComittedOffset;
		_lastEnquedOffset = _lastComittedOffset;

	}

	public void next(Iface blurClient) {
		if (_waitingToEmit.size() < _capacity) {
			fill();
		}
		
		for(Map.Entry<Long, Message> entry : _waitingToEmit.entrySet()){			
			Long key = entry.getKey();
			Message msg = entry.getValue();
			if(msg != null){
				if(_indexer != null){
					
					RowMutation mutation = _indexer.getMutation(Utils
							.toByteArray(msg.payload()));
					
					if (mutation != null) {
						try {
							 blurClient.enqueueMutate(mutation);
							_lastEnquedOffset = key;
						} catch (TException e) {
							LOG.error("Error during Blur Enqueue " + e.getMessage());
						}
					}else{
						
						//These are bad messages. RowMutation is not possible
						LOG.info("Mutation is Null for offset " + key + " for  " + _partition);
						_badMessages.add(key);
					}
				}else{
					LOG.info("Indexer is Null for  " + _partition + " Indexing FAILED!");
				}
			}
		}
		
		for(long key: _badMessages){
			
			//removing bad message. Good non-enqueued message can be re-tried if needed
			_waitingToEmit.remove(key);
		}
		_badMessages.clear();
		if(_lastEnquedOffset > _lastComittedOffset){
			commit();
			LOG.info("After commit , Waiting To Emit queue size is  " + _waitingToEmit.size());
		}
	}

	private void fill() {

		try {
			long start = System.nanoTime();
			ByteBufferMessageSet msgs = KafkaUtils.fetchMessages(
					_kafkablurconfig, _consumer, _partition, _emittedToOffset);

			for (MessageAndOffset msg : msgs) {
				if(msg.message() != null){
					_waitingToEmit.put(_emittedToOffset, msg.message());
					_emittedToOffset = msg.nextOffset();
				}
			}

			if (_waitingToEmit.size() > 1)
				LOG.info("Total " + _waitingToEmit.size()
						+ " messages from Kafka: " + _consumer.host() + ":"
						+ _partition.partition + " there in internal buffers");
		} catch (Exception kafkaEx) {
			LOG.error("Exception during fill " + kafkaEx.getMessage());
		}
	}

	public void commit() {

		LOG.info("LastComitted Offset : " + _lastComittedOffset);
		LOG.info("New Emitted Offset : " + _emittedToOffset);
		LOG.info("Enqueued Offset :" + _lastEnquedOffset);

		if (_lastEnquedOffset > _lastComittedOffset) {
			LOG.info("Committing offset for " + _partition);
			Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
					.builder()
					.put("consumer",
							ImmutableMap.of("id", _blurConsumerId, "table",
									(String) _kafkablurconfig._stateConf
											.get(Config.BLUR_TABLE_NAME)))
					.put("offset", _lastEnquedOffset)
					.put("partition", _partition.partition)
					.put("broker",
							ImmutableMap.of("host", _partition.host.host,
									"port", _partition.host.port))
					.put("topic", _topic).build();

			try {
				_state.writeJSON(committedPath(), data);
				LOG.info("Wrote committed offset to ZK: " + _lastEnquedOffset);
				_waitingToEmit.clear();
			} catch (Exception zkEx) {
				LOG.error("Error during commit. Let wait for refresh "
						+ zkEx.getMessage());
			}

			_lastComittedOffset = _lastEnquedOffset;
			LOG.info("Committed offset " + _lastEnquedOffset + " for " + _partition
					+ " for blur consumer: " + _blurConsumerId);
		}else{
			
			LOG.info("Last Enqueued offset " + _lastEnquedOffset + " not incremented since previous Comitted Offset " + _lastComittedOffset
					+ " for partition  " + _partition + " for Consumer " + _blurConsumerId + ". Some issue in BLUR Enqueue!!");
		}
	}

	private String committedPath() {
		return _stateConf.get(Config.ZOOKEEPER_BLUR_CONSUMER_PATH) + "/"
				+ _stateConf.get(Config.KAFKA_BLUR_CONSUMER_ID) + "/"
				+ _partition.getId();
	}

	public long queryPartitionOffsetLatestTime() {
		return KafkaUtils.getOffset(_consumer, _topic, _partition.partition,
				OffsetRequest.LatestTime());
	}

	public long lastCommittedOffset() {
		return _lastComittedOffset;
	}

	public Partition getPartition() {
		return _partition;
	}

	public void close() {
		_connections.unregister(_partition.host, _partition.partition);
	}
}
