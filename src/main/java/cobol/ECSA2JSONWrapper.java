package cobol;

import com.itergo.eksconnect.core.RequestHandler;
import com.itergo.eksconnect.core.cobol.CobolSyntaxException;
import com.itergo.eksconnect.core.transformer.CopybookTransformerImpl;
import com.itergo.eksconnect.messages.StringMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.IOException;

import static com.ibm.msg.client.jms.JmsConstants.JMSX_DELIVERY_COUNT;

public class ECSA2JSONWrapper {

    private static Logger log = LoggerFactory.getLogger(ECSA2JSONWrapper.class);


    public String dispatchMessage(TextMessage message) throws JMSException, CobolSyntaxException, IOException {

        CopybookTransformerImpl copybookTransformerImpl = new CopybookTransformerImpl();
        RequestHandler requestHandler = new RequestHandler(copybookTransformerImpl);
        final String methodName = "onMessage";
        String toJSON="";
        String messageId="";
        try {
            messageId = message.getJMSMessageID();
            MDC.put("messageId", messageId);
            MDC.put("correlationId", message.getJMSCorrelationID());
            String text = message.getText();
            int deliveryCount = message.getIntProperty(JMSX_DELIVERY_COUNT);
            log.info(StringMessages.getMessage(StringMessages.ON_MESSAGE_TEXT, deliveryCount));
            toJSON = requestHandler.dispatchMessage(text);
        } catch (JMSException e) {
            log.error(StringMessages.getMessage(StringMessages.JMS_EXCEPTION, methodName), e);
            e.printStackTrace();
//        } catch (MetadataCacheException ex) {
//             ex.printStackTrace();
        } catch (Exception e){
            log.error(StringMessages.getMessage(StringMessages.MESSAGE_PROCESS_EXCEPTION, methodName), e);
            throw e;
        }
        finally{
            MDC.clear();
        }

        return toJSON;
    }

    public String dispatchMessage(String text) throws JMSException, CobolSyntaxException, IOException {

        CopybookTransformerImpl copybookTransformerImpl = new CopybookTransformerImpl();
        RequestHandler requestHandler = new RequestHandler(copybookTransformerImpl);
        final String methodName = "onMessage";
        String toJSON="";
        String messageId="";
        try {
            toJSON = requestHandler.dispatchMessage(text);
        } catch (Exception e){
            log.error(StringMessages.getMessage(StringMessages.MESSAGE_PROCESS_EXCEPTION, methodName), e);
            throw e;
        }
        finally{
            MDC.clear();
        }

        return toJSON;
    }


}
