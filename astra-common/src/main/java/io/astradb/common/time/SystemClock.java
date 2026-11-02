package io.astradb.common.time;

/** Clock implementation backed by the system wall-clock time. */
public final class SystemClock implements Clock {

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
