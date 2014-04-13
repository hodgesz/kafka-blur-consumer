package blur.kafka.client;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import blur.kafka.DynamicBrokersReader;
import blur.kafka.KafkaBlurConfig;
import blur.kafka.ZkState;

public class KafkaBlurConsumer {

	private final Properties _props;
	public static final Logger LOG = LoggerFactory.getLogger(KafkaBlurConsumer.class);

	public KafkaBlurConsumer() {

		this._props = new Properties();
	}

	public int run(String[] args) throws Exception {

		Options options = new Options();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		OptionBuilder.withArgName("property=value");
		OptionBuilder.hasArgs(2);
		OptionBuilder.withValueSeparator();
		OptionBuilder.withDescription("use value for given property");
		options.addOption(OptionBuilder.create("D"));

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		if ((!cmd.hasOption('p')) && (!cmd.hasOption('P'))) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("KafkaBlurConsumer.java", options);
			return 1;
		}
		if (cmd.hasOption('p')) {
			this._props.load(ClassLoader.getSystemClassLoader()
					.getResourceAsStream(cmd.getOptionValue('p')));
		}
		if (cmd.hasOption('P')) {
			File file = new File(cmd.getOptionValue('P'));
			FileInputStream fStream = new FileInputStream(file);
			this._props.load(fStream);
		}
		this._props.putAll(cmd.getOptionProperties("D"));

		run();
		return 0;
	}

	public void run() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		KafkaBlurConfig blurConfig = new KafkaBlurConfig(_props);
		ZkState zkState = new ZkState(blurConfig);
		DynamicBrokersReader kafkaBrokerReader = new DynamicBrokersReader(blurConfig,zkState);
		int partionCount = kafkaBrokerReader.getNumPartitions();
		LOG.info("Total number of partition for the topic " + partionCount);

		for (int partitionId = 0; partitionId < partionCount; partitionId++) {
			Thread consumerThread = new Thread(new ConsumerJob(blurConfig,zkState, partitionId));
			consumerThread.setDaemon(true);
			consumerThread.start();
		}
	}

	public static void main(String[] args) throws Exception {

		KafkaBlurConsumer example = new KafkaBlurConsumer();
		example.run(args);
	}
}
