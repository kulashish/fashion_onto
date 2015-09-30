package com.jabong.dap.model.customer.variables

/**
 * Created by raghu on 27/5/15.
 */
object Customer {

  /**
   * EMAIL_SUBSCRIPTION_STATUS
   * iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
   * @param nls_email
   * @param status
   * @return String
   */
  def getEmailOptInStatus(nls_email: String, status: String): String = {

    if (nls_email == null) {
      return "O"
    }

    status match {
      case "subscribed" => "I"
      case "unsubscribed" => "U"
    }

  }

}