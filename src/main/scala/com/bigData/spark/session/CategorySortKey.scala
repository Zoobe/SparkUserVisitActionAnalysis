package com.bigData.spark.session

class CategorySortKey(val clickCount:Long, val orderCount:Long,val payCount:Long) extends Ordered[CategorySortKey] with Serializable {
  override def compare(that: CategorySortKey): Int = {
    this.clickCount - that.clickCount match {
      case c if c<0 => -1
      case c if c>0 => 1
      case _ =>{
        this.orderCount - that.orderCount match{
          case o if o<0 => -1
          case o if o>0 => 1
          case _ =>{
            this.payCount - that.payCount match {
              case p if p<0 => -1
              case p if p>0 => 1
              case _ => 0
            }
          }
        }
      }
    }
  }
}
