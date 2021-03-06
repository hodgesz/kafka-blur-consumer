package blur.kafka;

import java.util.List;

public interface PartitionCoordinator {
	List<PartitionManager> getMyManagedPartitions();

	PartitionManager getManager(Partition partition);
}
