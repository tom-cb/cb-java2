import com.couchbase.client.CouchbaseClient;
import java.util.concurrent.CountDownLatch;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.OperationCompletionListener;


import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

    class MyListener implements OperationCompletionListener {
      ScheduledExecutorService sch;
	  Object value;
      String key;
      CountDownLatch latch;
      CouchbaseClient client;
      int backoffexp = 0;
      int tries = 20;
      OpTracker opTracker;

      MyListener(CouchbaseClient c, CountDownLatch l, int b, Object v, String k, ScheduledExecutorService s, OpTracker ot) {
        value = v;
        key = k;
        client = c;
        latch = l;
        backoffexp = b;
        sch = s;
        opTracker = ot;
      }

      public void onComplete(OperationFuture<?> future) throws Exception {
        //Log completion of async request, then process result
        opTracker.setOnCompleted(key);
        doSetWithBackOff(future);
      }//onComplete

      public void doSetWithBackOff(OperationFuture<?> future) throws Exception {
      try {
        if (future.getStatus().isSuccess()) {
          try {
            //Log this op as completed
            opTracker.setCompleted(key);

            // Successfully completed this operation, decrement the latch
      	    latch.countDown();
          }
          catch (Exception e) { 
            System.out.println("couldnt countdown latch: " + e.getMessage());
            System.exit(1);
          }
        }
        else if (backoffexp > tries) {
          // Reached our maximum number of back off attempts, give up
          System.out.println("tried " + tries + " times, giving up setting key: " + key);
          System.exit(1);
        }
        else {
          try {
            // The operation failed, reschedule it for backoffMillis time later
            double backoffMillis = Math.pow(2, backoffexp);
            backoffMillis = Math.min(8000, backoffMillis); // 1 sec max
            backoffexp++;
           
            //System.out.println("backing off for: " + backoffMillis + " on key: " + future.getKey()); 
            if (sch == null) { System.out.println("no scheduler object!"); System.exit(1); }

            opTracker.setRescheduled(key);

            ScheduledFuture scheduledFuture =
              sch.schedule(new MyCallable(this, opTracker), (long)backoffMillis, TimeUnit.MILLISECONDS);
          }
          catch (Exception e) { 
             System.out.println("borked during back off");
             System.exit(1);
             throw e; 
          }
         }
       }
       catch(Exception e) {
         System.out.println("exception in onComplete");
         System.exit(1);
       }
      }//doSetWithBackOff

    } //MyListener Class
