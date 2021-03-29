package pi

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction

/**
 * Iterates from 0L till Long.MaxValue (~Infinity).
 * This is class is used as source generating sequential ids.
 */
private class SequenceIterator extends java.util.Iterator[Long] with Serializable {
  var _nextId: Long = -1L

  def hasNext: Boolean = true

  def next: Long = {
    this._nextId = this._nextId + 1
    this._nextId
  }

}

/** Creates a Flink Source from the SequenceIterator */
class IdGenerator extends FromIteratorFunction[Long](new SequenceIterator)
