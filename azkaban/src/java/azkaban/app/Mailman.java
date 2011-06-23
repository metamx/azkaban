/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.app;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * The mailman send you mail, if you ask him
 * 
 * @author jkreps
 * 
 */
public class Mailman {

    private static Logger logger = Logger.getLogger(Mailman.class.getName());

    private final String _mailHost;
    private final String _mailUser;
    private final String _mailPassword;
    private final String _port;
    private final boolean _useTls;

  public Mailman(String mailHost, String mailUser, String mailPassword, String port, boolean useTls) {
        this._mailHost = mailHost;
        this._mailUser = mailUser;
        this._mailPassword = mailPassword;
        this._port = port;
        this._useTls = useTls;
  }

    public void sendEmail(String fromAddress, List<String> toAddress, String subject, String body)
            throws MessagingException {
        Properties props = new Properties();
        props.setProperty("mail.host", _mailHost);
        props.setProperty("mail.smtp.socketFactory.port", _port);

        Session session;
        if (_useTls) {
            props.setProperty("mail.smtp.starttls.enable", "true");
            props.setProperty("mail.smtp.auth", "true");
            props.setProperty("mail.smtp.ssl.trust", "*");
            props.setProperty("mail.smtp.socketFactory.fallback", "false");

            session = Session.getInstance(
                props,
                new Authenticator()
                {
                  @Override
                  protected PasswordAuthentication getPasswordAuthentication()
                  {
                    return new PasswordAuthentication(
                        _mailUser,
                        _mailPassword
                    );
                  }
                }
            );
        }
        else {
            props.setProperty("mail.user", _mailUser);
            props.setProperty("mail.password", _mailPassword);
            session = Session.getDefaultInstance(props);
        }

        Message message = new MimeMessage(session);
        InternetAddress from = new InternetAddress(fromAddress == null ? "" : fromAddress, false);
        message.setFrom(from);
        for(String toAddr: toAddress)
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(toAddr, false));
        message.setSubject(subject);
        message.setText(body);
        message.setSentDate(new Date());
        Transport.send(message);
    }

    public void sendEmailIfPossible(String fromAddress,
                                    List<String> toAddress,
                                    String subject,
                                    String body) {
        try {
            sendEmail(fromAddress, toAddress, subject, body);
        } catch(AddressException e) {
            logger.warn("Error while sending email, invalid email: " + e.getMessage());
        } catch(MessagingException e) {
            logger.warn("Error while sending email: " + e.getMessage());
        }
    }
}
