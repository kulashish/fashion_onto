package com.jabong.dap.common.mail

/**
 * Created by tejas on 25/8/15.
 */
/**
 * Created by tejas on 19/8/15.
 */

import java.util.{ Date, Properties }
import javax.mail.{ Address, Message, Session, Transport }
import javax.mail.internet.{MimeMultipart, MimeBodyPart, InternetAddress, MimeMessage}

import org.apache.spark.sql.DataFrame

object ScalaMail extends java.io.Serializable {
  var message: Message = null
  var username: String = ""
  var password: String = ""
  var to: String = ""
  var cc: String = ""
  var bcc: String = ""

  // throws MessagingException
  def sendMessage(to: String, cc: String, bcc: String, from: String, subject: String, content: String) {
    message = createMessage
    message.setFrom(new InternetAddress(from))
    message.setSentDate(new Date())
    message.setSubject(subject)
//    message.setText(content)
    val mbp3 = new MimeBodyPart()
    mbp3.setContent(content, "text/html")
    val mp = new MimeMultipart()
    mp.addBodyPart(mbp3)
    message.setContent(mp)

    setToCcBccRecipients(to, cc, bcc)
    Transport.send(message)
  }

  def createMessage: Message = {
    val props = new Properties()
    //properties.put("mail.smtp.host", smtpHost)
    // val session = Session.getDefaultInstance(properties, null)
    //username = ""
    //password = ""

    //Properties props = new Properties();
    //props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.auth", "false")
    props.put("mail.smtp.starttls.enable", "true")
    props.put("mail.smtp.host", "localhost.localdomain")
    props.put("mail.smtp.port", "25")

    /*val session = Session.getInstance(props,
      new Authenticator() {
        override def getPasswordAuthentication = new
            PasswordAuthentication(username, password)
      })*/
    val session = Session.getInstance(props, null)
    /*new Authenticator() {
      override def getPasswordAuthentication = new
          PasswordAuthentication(username, password)
    })*/

    return new MimeMessage(session)
  }

  // throws AddressException, MessagingException
  def setToCcBccRecipients(to: String, cc: String, bcc: String) {
    setMessageRecipients(to, Message.RecipientType.TO)
    if (cc != null) {
      setMessageRecipients(cc, Message.RecipientType.CC)
    }
    if (bcc != null) {
      setMessageRecipients(bcc, Message.RecipientType.BCC)
    }
  }

  // throws AddressException, MessagingException
  def setMessageRecipients(recipient: String, recipientType: Message.RecipientType) {
    // had to do the asInstanceOf[...] call here to make scala happy
    val addressArray = buildInternetAddressArray(recipient).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0)) {
      message.setRecipients(recipientType, addressArray)
    }
  }

  // throws AddressException
  def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    // could test for a null or blank String but I'm letting parse just throw an exception
    return InternetAddress.parse(address)
  }

  /**
   * genrate html from DataFrame
   * @param df
   * @return
   */
  def generateHTML(df: DataFrame): String = {

    val fieldNames = df.schema.fieldNames.mkString("</th>\n<th>")

    val data = df.collect.foldLeft("")((a, b) => a + b + "\n").replaceAll("\\[", "<tr>\n<td>").replaceAll("\\]", "</td>\n </tr>").replaceAll(",", "</td>\n  <td>")

    val content = "<!DOCTYPE html>\n<html>\n<head>\n<style>\ntable {\n    width:100%;\n}\ntable</td>\n  <td> th</td>\n  <td> td {\n    border: 1px solid black;\n    border-collapse: collapse;\n}\nth</td>\n  <td> td {\n    padding: 5px;\n    text-align: left;\n}\ntable#t01 tr:nth-child(even) {\n    background-color: #eee;\n}\ntable#t01 tr:nth-child(odd) {\n   background-color:#fff;\n}\ntable#t01 th\t{\n    background-color: black;\n    color: white;\n}\n</style>\n</head>\n<body>\n\n<table id=\"t01\">\n  <tr>\n    <th>" +
      fieldNames + "</th>\n  </tr><tr>\n    <td>" + data + "</table>\n\n</body>\n</html>"

    content
  }

}
