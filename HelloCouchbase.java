import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import com.couchbase.client.java.*;

//For Async Processing
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;


//TOM
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class HelloCouchbase {

	static final int NUM_DOCS = 100;  //number of documents to work on
	static final int DOC_SIZE = 1000; //approx size of doc in KB
    static final int DOCS_PER_BATCH = 10000; // Number of docs to fire off in a single batch
	static final int NUM_BATCHES = 10;

	public static void main(String[] args) throws Exception {
		Cluster cluster = CouchbaseCluster.create("127.0.0.1");
		final Bucket syncBucket = cluster.openBucket();
		final AsyncBucket bucket = syncBucket.async();

		JsonObject user = JsonObject.empty()
		.put("description", "json object with a string of random data to bulk up the size");

		//Generate a random string to bulk up doc size
		String rndStr = randomString(DOC_SIZE);

		// Generate a number of dummy JSON documents
		List<JsonDocument> documents = new ArrayList<JsonDocument>();
		for (int i = 0; i < DOCS_PER_BATCH; i++) {
		    JsonObject content = JsonObject.create()
        		.put("counter", i)
        		.put("random_data", rndStr);
    		documents.add(JsonDocument.create("key-"+i, content));
		}

		for (int b=0; b < NUM_BATCHES; b++) {
			upsertBatch(bucket, documents, b);
		}

		for (int i=0; i< NUM_BATCHES; i++) {
			for (int j=0; j< DOCS_PER_BATCH; j++) {
				final String k = "key-" + j + "-batch-" + i;
				JsonDocument doc = documents.get(0);
				final JsonDocument docForInsert = doc.from(doc, doc.id() + "-batch-" + i);

            	bucket
					.upsert(docForInsert)
					.onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
						@Override
					 	public Observable<? extends JsonDocument> call(Throwable throwable) {
							if (throwable instanceof TimeoutException) {
								System.out.println("Timeout: rescheduling " + k);
								return bucket.upsert(docForInsert); 
							}
							return Observable.error(throwable);
						}
					});
			}
		}

		final CountDownLatch latch = new CountDownLatch(DOCS_PER_BATCH * NUM_BATCHES);

		long t1 = System.nanoTime();

		for (int i=0; i< NUM_BATCHES; i++) {
			for (int j=0; j< DOCS_PER_BATCH; j++) {
			final String k = "key-" + j + "-batch-" + i;

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
					latch.countDown();
            		//System.out.println("Got: " + document);
        		}
			});
		}
		}

 		latch.await();
		long t2 = System.nanoTime();

		System.out.println("Time: " + ((t2 - t1)/1000000)/NUM_DOCS + " ms");


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


