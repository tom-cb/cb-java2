//For Async Processing
import rx.Observable;
import rx.Subscriber;
import rx.Notification;
import rx.functions.Action1;
import rx.functions.Func1;



public class RetryWithDelay implements
    Func1<Observable<? extends Notification<?>>, Observable<?>> {

    private final int maxRetries;
    private final int retryDelayMillis;
    private int retryCount;

    public RetryWithDelay(final int maxRetries, final int retryDelayMillis) {
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.retryCount = 0;
    }

    @Override
    public Observable<?> call(Observable<? extends Notification<?>> attempts) {
        return attempts
            .flatMap(new Func1<Notification<?>, Observable<?>>() {
                @Override
                public Observable<?> call(Notification errorNotification) {
                    if (++retryCount < maxRetries) {
                        // When this Observable calls onNext, the original
                        // Observable will be retried (i.e. re-subscribed).
                        return Observable.timer(retryDelayMillis, 
                                                TimeUnit.MILLISECONDS);
                    }

                    // Max retries hit. Just pass the error along.
                    return Observable.error(errorNotification.getThrowable());
                }
            });
    }
}
