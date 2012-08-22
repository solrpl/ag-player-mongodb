package pl.solr.ag.mongodb.sink;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import pl.solr.dm.producers.JsonDataModelProducer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import com.sematext.ag.PlayerConfig;
import com.sematext.ag.event.ComplexEvent;
import com.sematext.ag.exception.InitializationFailedException;
import com.sematext.ag.sink.Sink;

public class ComplexDataSink extends Sink<ComplexEvent> {
	public static final String BASE_HOST_KEY = "complexDataMongoDBSink.host";
	public static final String BASE_PORT_KEY = "complexDataMongoDBSink.port";
	public static final String DB_NAME_KEY = "complexDataMongoDBSink.db";
	public static final String COLLECTION_NAME_KEY = "complexDataMongoDBSink.collection";
	private static final Logger LOG = Logger.getLogger(ComplexDataSink.class);
	private String host;
	private Integer port;
	private String dbName;
	private String collectionName;
	private DBCollection collection;
	private DB db;

	@Override
	public void init(PlayerConfig config) throws InitializationFailedException {
		super.init(config);

		host = config.get(BASE_HOST_KEY);
		port = Integer.valueOf(config.get(BASE_PORT_KEY));
		dbName = config.get(DB_NAME_KEY);
		collectionName = config.get(COLLECTION_NAME_KEY);

		if (host == null || "".equals(host.trim())) {
			throw new IllegalArgumentException(this.getClass().getName()
					+ " expects configuration property " + BASE_HOST_KEY);
		}

		if (port == null) {
			throw new IllegalArgumentException(this.getClass().getName()
					+ " expects configuration property " + BASE_PORT_KEY);
		}
		
		if (dbName == null || "".equals(dbName.trim())) {
			throw new IllegalArgumentException(this.getClass().getName()
					+ " expects configuration property " + DB_NAME_KEY);
		}
		
		if (collectionName == null || "".equals(collectionName.trim())) {
			throw new IllegalArgumentException(this.getClass().getName()
					+ " expects configuration property " + COLLECTION_NAME_KEY);
		}
		
		Mongo mongo = null;
		try {
			mongo = new Mongo(host, port);
		} catch (Exception e) {
			throw new InitializationFailedException(e.getMessage());
		}
		db = mongo.getDB(dbName);
		collection = db.getCollection(collectionName);
	}

	@Override
	public boolean write(ComplexEvent event) {
		String result = new JsonDataModelProducer().convert(event.getObject());
		DBObject obj = (DBObject) JSON.parse(result);
		collection.insert(obj);
		return true;
	}

}
