package khipu.util

final class Clock {
  private var _elasped = 0L
  def elasped: Long = _elasped

  def start() {
    _elasped = 0L
  }

  def elapse(ms: Long) {
    _elasped += ms
  }
}
