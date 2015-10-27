package com.jabong.dap.common.constants.status

/**
 * All Different statuses of order
 */
object OrderStatus {

  val CANCELLED = Array(14,15,16,23,25,26,27,28)
  val DECLINED = 23
  val CANCELLED_CC_ITEM = 25
  val EXPORTABLE_CANCEL_CUST = 26
  val EXPORTED_CANCEL_CUST = 27
  val CANCEL_PAYMENT_ERROR = 29
  val RETURN = Array(8,12,32,35,36,37,38)
  val RETURN_PAYMENT_PENDING = 18
  val INVALID = 10
  val CLOSED_ORDER = 6

}
