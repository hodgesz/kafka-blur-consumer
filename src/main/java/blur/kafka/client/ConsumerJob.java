package blur.kafka.client;

import blur.kafka.KafkaBlurConfig;
import blur.kafka.KafkaConsumer;
import blur.kafka.ZkState;

public class ConsumerJob implements Runnable {

	KafkaConsumer _kConsumer;
	public ConsumerJob(KafkaBlurConfig blurConfig, ZkState zkState,
			int partitionId) {

		_kConsumer = new KafkaConsumer(blurConfig,zkState);
		_kConsumer.open(partitionId);
	}

	@Override
	public void run() {
		try {

			while (true) {
				_kConsumer.doIndex();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
