package edu.umd.cs.hcil.analytics.spark.network

/**
  * Created by cbuntain on 9/26/18.
  */
case class UserNode (id : Long, name : String) {
  override def toString: String = {
    return "%d,%s".format(id, name)
  }
}
