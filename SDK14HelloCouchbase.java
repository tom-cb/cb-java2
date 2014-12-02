import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.CouchbaseConnectionFactory;

import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.ReplicateTo;

import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

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

public class SDK14HelloCouchbase {

    static final int NUM_DOCS = 100;  //number of documents to work on
    static final int DOC_SIZE = 1000; //approx size of doc in KB
    static final int DOCS_PER_BATCH = 1000; // Number of docs to fire off in a single batch
    static final int NUM_BATCHES = 1;

	public static void main(String[] args) throws Exception {
    	List<URI> hosts = Arrays.asList(
      		new URI("http://192.168.27.101:8091/pools")
    	); 

    
    	// Name of the Bucket to connect to 
    	String bucket = "default";

    	// Password of the bucket (empty) string if none
    	String password = "";

    	CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();

    	// Ovveride default values on CouchbaseConnectionFactoryBuilder
    	// For example - wait up to <x> milliseconds for an operation to succeed
    	cfb.setOpTimeout(500);

    	CouchbaseConnectionFactory cf = cfb.buildCouchbaseConnection(hosts, bucket, "");
    	CouchbaseClient client = new CouchbaseClient(cf);

		ScheduledExecutorService schExSvc = Executors.newScheduledThreadPool(1);

        //Generate a random string to bulk up doc size
        String rndStr = randomString(DOC_SIZE);

		// Generate a number of dummy JSON documents for storage + re-use
		List<String> documents = new ArrayList<String>();
		for (int i = 0; i < DOCS_PER_BATCH; i++) {
		    String doc = "{ \"counter\" : \"" + i + "\", \"random_data\" : \"" + rndStr + "\" }";
			documents.add(doc);
//    		documents.add(JsonDocument.create("key-"+i, content));
		}

        //  For tracking statistical info such as response times
        final ConcurrentHashMap<String, TimingPair> timingMap = new ConcurrentHashMap<String, TimingPair>();
        final DescriptiveStatistics stats = new DescriptiveStatistics();
	    OpTracker opTracker = new OpTracker();

        //To check when all documents have been successfully stored
        final CountDownLatch setLatch = new CountDownLatch(DOCS_PER_BATCH * NUM_BATCHES);

        for (int i=0; i< NUM_BATCHES; i++) {
            for (int j=0; j< DOCS_PER_BATCH; j++) {
                final String k = "key-" + j + "-batch-" + i;
				//System.out.println("key: " + k + "\n data: \n" + documents.get(j));

          		try {
            		OperationFuture<Boolean> future = client.set(k, documents.get(j), ReplicateTo.ONE);
					opTracker.setScheduled(k);
            		future.addListener(new MyListener(client,setLatch, 0, documents.get(j), k, schExSvc, opTracker));
				}
          		catch (Exception e) {
            		System.out.println("exception: " + e.getMessage());
            		throw e;
          		}
				Thread.sleep(100);
			}
		}

		setLatch.await();

		opTracker.printPercentile(95);
    	// Shutting down properly
    	client.shutdown();
	    // Shutdown ScheduledExecutor
    	schExSvc.shutdown();
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
