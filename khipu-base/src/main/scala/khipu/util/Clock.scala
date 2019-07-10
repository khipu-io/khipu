package khipu.util

final class Clock {
  private var _elasped = 0L // in nano
  def elasped: Long = _elasped

  def start() {
    _elasped = 0L
  }

  def elapse(nano: Long) {
    _elasped += nano
  }
}
