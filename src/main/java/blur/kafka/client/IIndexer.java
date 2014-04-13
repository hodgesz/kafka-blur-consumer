package blur.kafka.client;

import org.apache.blur.thrift.generated.RowMutation;

public interface IIndexer {

	public RowMutation getMutation(byte[] payload);
}
