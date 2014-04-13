package blur.kafka;

import java.util.List;

import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer {

	static enum EmitState {
		EMITTED_MORE_LEFT, EMITTED_END, NO_EMITTED
	}

	public static final Logger LOG = LoggerFactory
			.getLogger(KafkaConsumer.class);

	KafkaBlurConfig _kafkablurconfig;
	PartitionCoordinator _coordinator;
	DynamicPartitionConnections _connections;
	ZkState _state;
	Iface _blurClient;
	long _lastUpdateMs = 0;
	int _currPartitionIndex = 0;

	public KafkaConsumer(KafkaBlurConfig blurConfig,ZkState zkState) {
		_kafkablurconfig = blurConfig;
		_state = zkState;
	}

	public void open(int partitionId) {

		_currPartitionIndex = partitionId;
		_connections = new DynamicPartitionConnections(_kafkablurconfig,
				new ZkBrokerReader(_kafkablurconfig,_state));
		_coordinator = new ZkCoordinator(_connections, _kafkablurconfig,
				_state, partitionId);
		_blurClient = BlurClient.getClient((String) _kafkablurconfig._stateConf
				.get(Config.BLUR_CONTROLLER_CONNECTION));
	}

	public void close() {
		_state.close();
	}

	public void doIndex() {
		try {
			List<PartitionManager> managers = _coordinator
					.getMyManagedPartitions();
			managers.get(0).next(_blurClient);
		} catch (Exception error) {
			LOG.error("Partition " + _currPartitionIndex
					+ " encountered error during doIndex : "
					+ error.getMessage());
			error.printStackTrace();
		}

	}

	public void deactivate() {
		commit();
	}

	private void commit() {
		_lastUpdateMs = System.currentTimeMillis();
		_coordinator.getMyManagedPartitions().get(0).commit();
	}

}
