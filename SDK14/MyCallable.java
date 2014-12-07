
import com.couchbase.client.CouchbaseClient;
import java.util.concurrent.CountDownLatch;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.OperationCompletionListener;


//ScheduledExecutor classes
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;



class MyCallable implements Callable {
  MyListener listener;
  OpTracker opTracker;

  MyCallable(MyListener l, OpTracker ot) {
    listener = l; 
    opTracker = ot;
  }

  public Object call() throws Exception {
    try {
      // Perform the async call that forms the retry
      OperationFuture<Boolean> next_future = listener.client.set(listener.key, listener.value);
      opTracker.setRetried(listener.key);
      next_future.addListener(new MyListener(listener.client, listener.latch, listener.backoffexp,
          listener.value, listener.key, listener.sch, opTracker));
    }
    catch (Exception e) { 
      System.out.println("caught exception: " + e.getMessage());
      System.exit(1);
    }

    return "";
  }
}//MyCallable


