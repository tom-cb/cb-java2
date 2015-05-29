import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import com.couchbase.client.java.*;

//For Async Processing
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func0;


//TOM
import java.util.logging.*;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;

//Stats helpers
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

class TimingPair {
        public long start;
        public long end;

        public TimingPair(long x, long y) {
            this.start = x;
            this.end = y;
        }
}

public class HelloCouchbase {

	static final int NUM_DOCS = 100;  //number of documents to work on
	static final int DOC_SIZE = 100; //approx size of doc in KB
    static final int DOCS_PER_BATCH = 1000; // Number of docs to fire off in a single batch
	static final int NUM_BATCHES = 1;
	static final String CB_HOST = "127.0.0.1";

	public static void main(String[] args) throws Exception {

		// SETUP LOGGING
		Logger logger = Logger.getLogger("com.couchbase.client");
		logger.setLevel(Level.FINE);
		for(Handler h : logger.getParent().getHandlers()) {
    		if(h instanceof ConsoleHandler){
        		h.setLevel(Level.FINE);
    		}
		}

		Cluster cluster = CouchbaseCluster.create(CB_HOST);
		final Bucket syncBucket = cluster.openBucket();
		final AsyncBucket bucket = syncBucket.async();


		JsonObject user = JsonObject.empty()
		.put("description", "json object with a string of random data to bulk up the size");

		//Generate a random string to bulk up doc size
		String rndStr = randomString(DOC_SIZE);

		// Generate a number of dummy JSON documents for storage + re-use
		List<JsonDocument> documents = new ArrayList<JsonDocument>();
		for (int i = 0; i < DOCS_PER_BATCH; i++) {
		    JsonObject content = JsonObject.create()
        		.put("counter", i)
        		.put("random_data", rndStr);
    		documents.add(JsonDocument.create("key-"+i, content));
		}

		//  For tracking statistical info such as response times
		final ConcurrentHashMap<String, TimingPair> timingMap = new ConcurrentHashMap<String, TimingPair>();
		final DescriptiveStatistics stats = new DescriptiveStatistics();

		//To check when all documents have been successfully stored
		final CountDownLatch setLatch = new CountDownLatch(DOCS_PER_BATCH * NUM_BATCHES);

		for (int i=0; i< NUM_BATCHES; i++) {
			for (int j=0; j< DOCS_PER_BATCH; j++) {
				final String k = "key-" + j + "-batch-" + i;
				JsonDocument doc = documents.get(j);
				final JsonDocument docForInsert = doc.from(doc, doc.id() + "-batch-" + i);
				
				Observable
					.defer(new Func0<Observable<JsonDocument>>() {
     					@Override
        					public Observable<JsonDocument> call() {
							timingMap.put(docForInsert.id(), new TimingPair(System.nanoTime(),0) );

            				return bucket.upsert(docForInsert);
        				}
    				})
					.timeout(500, TimeUnit.MILLISECONDS)
					.retryWhen(new RetryWithDelay(20, 500))
					.subscribe(new Subscriber<JsonDocument>() {
						@Override
						public void onNext(JsonDocument document) {
							//This doc has been successfully stored

							timingMap.get(document.id()).end = System.nanoTime();
							setLatch.countDown();						
							System.out.println("stored doc: " + docForInsert.id());
						}
						
						@Override
						public void onCompleted(){
						}

						@Override
						public void onError(Throwable throwable) {
							System.out.println("Error: " + throwable.getMessage());
						}
					
					});

			}
				Thread.sleep(100);
		}

		setLatch.await();
		System.out.println("Completed write phase!");

		// Post process and output the statistical results
		for(ConcurrentHashMap.Entry<String, TimingPair> entry : timingMap.entrySet()) {
			String k = entry.getKey();
			TimingPair v = entry.getValue();

			stats.addValue((v.end-v.start)/1000000);
			System.out.println("Write time for key: " + k + " in ms: " + (v.end - v.start)/1000000);
		}

		System.out.println("95th Percentile: " + stats.getPercentile(95));

		/* GET PHASE */
		final CountDownLatch latch = new CountDownLatch(DOCS_PER_BATCH * NUM_BATCHES);
		//  For tracking statistical info such as response times
		final ConcurrentHashMap<String, TimingPair> timingMapGets = new ConcurrentHashMap<String, TimingPair>();
		final DescriptiveStatistics statsGets = new DescriptiveStatistics();


		for (int i=0; i< NUM_BATCHES; i++) {
			for (int j=0; j< DOCS_PER_BATCH; j++) {
			final String k = "key-" + j + "-batch-" + i;

		 	timingMapGets.put(k , new TimingPair(System.nanoTime(),0) );

			bucket
    		.get(k)
    		.timeout(500, TimeUnit.MILLISECONDS)
			.onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
        		@Override
        		public Observable<? extends JsonDocument> call(Throwable throwable) {
            		if (throwable instanceof TimeoutException) {
                		return bucket.getFromReplica(k, ReplicaMode.ALL);
            		}
            		return Observable.error(throwable);
        		}
    		})
 			.subscribe(new Action1<JsonDocument>() {
        		@Override
        		public void call(JsonDocument document) {
					timingMapGets.get(k).end = System.nanoTime();
					latch.countDown();
            		System.out.println("Got: " + document.id());
        		}
			});
		}
		}

 		latch.await();
		long t2 = System.nanoTime();

		// Post process and output the statistical results
		for(ConcurrentHashMap.Entry<String, TimingPair> entry : timingMapGets.entrySet()) {
			String k = entry.getKey();
			TimingPair v = entry.getValue();

			statsGets.addValue((v.end-v.start)/1000000);
			System.out.println("Get time for key: " + k + " in ms: " + (v.end - v.start)/1000000);
		}


		System.out.println("95th Percentile: " + statsGets.getPercentile(95));


		cluster.disconnect();
	}

	public static JsonDocument upsertBatch(final AsyncBucket bucket, List<JsonDocument> documents, final int batchNum) {
			// Insert them in one batch, waiting until the last one is done.
			return Observable
    			.from(documents)
    			.flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
        			@Override
        			public Observable<JsonDocument> call(final JsonDocument doc) {
							// Over-writing the premade doc with new key for this batch number
							JsonDocument docForInsert = doc.from(doc, doc.id() + "-batch-" + batchNum);

            	 			return bucket
									.upsert(docForInsert)
									.onErrorResumeNext(bucket.insert(docForInsert));
			    	}
    			})
			.last()
    		.toBlocking()
    		.single();
   }

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();

	public static String randomString( int len ) 
	{
			   StringBuilder sb = new StringBuilder( len );
			      for( int i = 0; i < len; i++ ) 
						        sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
				     return sb.toString();
	}
}


