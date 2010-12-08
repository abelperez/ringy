package com.mindplex.remote

import java.util.zip.CRC32
import collection.immutable.{TreeMap, SortedMap}

/**
 * 
 */
trait HashRing
{
  /**
   * 
   */
  val replicaCount: Int

  /**
   * 
   */
  var ring: SortedMap[Long, String] = new TreeMap[Long, String];
  
  /**
   *
   */
  def addNodes(nodes:Collection[String]):HashRing = {
    for (val node <- nodes) {
      addNode(node)      
    }
    this
  }

  /**
   *
   */
  def addNode(node:String):HashRing = {
    for (i <- 1 to replicaCount) {
      val key = hash(node + ":" + i)
      ring += (key -> node);
    }
    this
  }

  /**
   *
   */
  def getNode(element:String):Option[String] = {
    if (ring.isEmpty) return None
    
    val key:Long = hash(element)
    if (ring.contains(key)) return Some(ring(key))

    for ((k, v) <- ring) {
      if (k > key) return Some(ring(k))
    }

    for ((k, v) <- ring) {
       return Some(ring(k))
    }

    return None
  }

  /**
   * 
   */
  def hash(element:String):Long = {
    val checksum = new CRC32
    checksum.update(element.getBytes)
    checksum.getValue
  }

  /**
   *
   */
  override def toString(): String = {
    ring.foldLeft("")((r, kv) => r + "(" + kv._1 + " -> " + kv._2 + ") " )
  }
}