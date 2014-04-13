package blur.kafka;

public interface IBrokerReader {

	GlobalPartitionInformation getCurrentBrokers();

	void close();
}
