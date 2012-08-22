package pl.solr.ag.mongodb;

import pl.solr.ag.mongodb.sink.ComplexDataSink;

import com.sematext.ag.PlayerConfig;
import com.sematext.ag.PlayerRunner;
import com.sematext.ag.source.FiniteEventSource;
import com.sematext.ag.source.SimpleSourceFactory;
import com.sematext.ag.source.dictionary.ComplexEventSource;

public class ComplexDataPlayerMain {
	private ComplexDataPlayerMain() {
	}

	public static void main(String[] args) {
		if (args.length < 6) {
			System.out.println("Usage: host port databaseName collectionName eventsCount schemaFile");
			System.out.println("Example: localhost 27017 events collection1 100 schema.json");
			System.exit(1);
		}

		String host = args[0];
		String port = args[1];
		String databaseName = args[2];
		String collectionName = args[3];
		String eventsCount = args[4];
		String schemaFile = args[5];
		
		PlayerConfig config = new PlayerConfig(
				SimpleSourceFactory.SOURCE_CLASS_CONFIG_KEY,
				ComplexEventSource.class.getName(),
				FiniteEventSource.MAX_EVENTS_KEY, eventsCount,
				ComplexEventSource.SCHEMA_FILE_NAME_KEY, schemaFile,
				PlayerRunner.SINK_CLASS_CONFIG_KEY, ComplexDataSink.class.getName(),
				ComplexDataSink.BASE_HOST_KEY, host,
				ComplexDataSink.BASE_PORT_KEY, port,
				ComplexDataSink.DB_NAME_KEY, databaseName,
				ComplexDataSink.COLLECTION_NAME_KEY, collectionName);
		PlayerRunner.play(config);
	}
}
