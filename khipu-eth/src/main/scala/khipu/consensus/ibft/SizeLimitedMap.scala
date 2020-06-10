package khipu.consensus.ibft

import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * Map that is limited to a specified size and will evict oldest entries when the size limit is
 * reached.
 */
class SizeLimitedMap[K, V](maxEntries: Int) extends LinkedHashMap[K, V] {
  override protected def removeEldestEntry(ignored: Entry[K, V]): Boolean = {
    size > maxEntries
  }
}
