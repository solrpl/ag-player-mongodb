/**
 * Copyright 2012 Solr.pl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.solr.ag.mongodb;

import pl.solr.ag.mongodb.sink.ComplexDataSink;

import com.sematext.ag.PlayerConfig;
import com.sematext.ag.PlayerRunner;
import com.sematext.ag.source.FiniteEventSource;
import com.sematext.ag.source.SimpleSourceFactory;
import com.sematext.ag.source.dictionary.ComplexEventSource;

/**
 * Command line util for generate random data to mongoDB collection.
 * 
 * @author negativ
 *
 */
public class ComplexDataPlayerMain {
	private ComplexDataPlayerMain() {
	}

	public static void main(final String[] args) {
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
