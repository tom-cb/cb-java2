//For Async Processing
import rx.Observable;
import rx.Subscriber;
import rx.Notification;
import rx.functions.Action1;
import rx.functions.Func1;


import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public class RetryWithDelay implements
    Func1<Observable<? extends Throwable>, Observable<?>> {

    private final int maxRetries;
    private final int retryDelayMillis;
    private int retryCount;

    public RetryWithDelay(final int maxRetries, final int retryDelayMillis) {
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.retryCount = 0;
    }

    @Override
    public Observable<?> call(Observable<? extends Throwable> attempts) {
        return attempts
            .flatMap(new Func1<Throwable, Observable<?>>() {
                @Override
                public Observable<?> call(Throwable errorThrowable) {
					System.out.println("RetryWhen error type: " + errorThrowable.getClass());

                    if (++retryCount < maxRetries) { // && errorThrowable instanceof TimeoutException) {
                        // When this Observable calls onNext, the original
                        // Observable will be retried (i.e. re-subscribed).
						int delay = (retryDelayMillis * (retryCount == 0 ? 1 : retryCount * 2));
						System.out.println("Issuing retry in: " + delay + "ms");
                        return Observable.timer(delay, TimeUnit.MILLISECONDS);
                    }

					System.out.println("Max retries hit: " + errorThrowable.getMessage());
                    // Max retries hit. Just pass the error along.
                    return Observable.error(errorThrowable);
                }
            });
    }
}
