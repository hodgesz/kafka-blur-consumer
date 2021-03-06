package blur.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkCoordinator implements PartitionCoordinator {
	public static final Logger LOG = LoggerFactory
			.getLogger(ZkCoordinator.class);

	KafkaBlurConfig _kafkablurconfig;
	int _partitionOwner;
	Map<Partition, PartitionManager> _managers = new HashMap();
	List<PartitionManager> _cachedList;
	Long _lastRefreshTime = null;
	int _refreshFreqMs;
	DynamicPartitionConnections _connections;
	DynamicBrokersReader _reader;
	ZkState _state;
	GlobalPartitionInformation _brokerInfo;
	

	public ZkCoordinator(DynamicPartitionConnections connections,
			KafkaBlurConfig config, ZkState state, int partitionId) {
		_kafkablurconfig = config;
		_connections = connections;
		_state = state;
		_partitionOwner = partitionId;
		_refreshFreqMs = config._refreshFreqSecs * 1000;
		_reader = new DynamicBrokersReader(_kafkablurconfig,_state);
		_brokerInfo = _reader.getBrokerInfo();
	}

	@Override
	public List<PartitionManager> getMyManagedPartitions() {
		if (_lastRefreshTime == null
				|| (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
			refresh();
			_lastRefreshTime = System.currentTimeMillis();
		}
		return _cachedList;
	}

	void refresh() {
		try {
			LOG.info("Refreshing partition manager connections");
			Set<Partition> mine = new HashSet();
			for (Partition partition : _brokerInfo) {
				if (partition.partition == _partitionOwner) {
					mine.add(partition);
					LOG.info("Added partition index " + _partitionOwner
							+ " for coordinator");
				}
			}

			Set<Partition> curr = _managers.keySet();
			Set<Partition> newPartitions = new HashSet<Partition>(mine);
			newPartitions.removeAll(curr);

			Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
			deletedPartitions.removeAll(mine);

			LOG.info("Deleted partition managers: "
					+ deletedPartitions.toString());

			for (Partition id : deletedPartitions) {
				PartitionManager man = _managers.remove(id);
				man.close();
			}
			LOG.info("New partition managers: " + newPartitions.toString());

			for (Partition id : newPartitions) {

				PartitionManager man = new PartitionManager(_connections,
						_state, _kafkablurconfig, id);
				_managers.put(id, man);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		_cachedList = new ArrayList<PartitionManager>(_managers.values());
		LOG.info("Finished refreshing");
	}

	@Override
	public PartitionManager getManager(Partition partition) {
		return _managers.get(partition);
	}
}
