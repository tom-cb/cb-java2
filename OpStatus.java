

public class OpStatus {

  //public as im too lazy to write getters and setters
  public int scheduled = 0;
  public int rescheduled = 0;
  public int retried = 0;
  public int completed = 0;
  public int onCompleteCalled = 0;
  public long start;
  public long end;

  public void setScheduled() {
    scheduled++;
	start = System.nanoTime(); 
  }

  public void setRescheduled() {
    rescheduled++;
	start = System.nanoTime(); 
  }

  public void setRetried() {
    retried++;
  }

  public void setCompleted() {
    completed++;
	end = System.nanoTime();
  }

  public void setOnCompleted() {
    onCompleteCalled++;
  }

  public String toString() {

    if (completed != scheduled) {
      return "*** Did not complete op expected number of times: Complted= " + completed + " scheduled=" + scheduled + ".";
    }
    // Only bother outputting stats for ops that got retried
    if (rescheduled > 0) {
      return "Completed: " + completed + " Scheduled: " + scheduled + 
        " Rescheduled: " + rescheduled + " onCompleted: " + onCompleteCalled + ".";
    }
    else { return ""; }

  }
}
