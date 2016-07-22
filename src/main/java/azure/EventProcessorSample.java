package azure;

import java.util.concurrent.*;

import com.microsoft.azure.eventprocessorhost.*;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;

public class EventProcessorSample
{
    public static void main(String args[])
    {
        final String consumerGroupName = "$Default";
        final String namespaceName = "dtna-ns";
        final String eventHubName = "dtna";
        final String sasKeyName = "zonar";
        final String sasKey = "qb5otAE6g7vyyU60uBHbWS50B8+xBK7oeW+gUO9rixc=";

        final String storageAccountName = "dtna";
        final String storageAccountKey = "WrnFRpzJbNq9gMnlKRsI6m277AidBih9GBRDI1WZKhRHroOV3R009I3FkXKbyH/0+tOcZTPY1jDVp2iLfmNlTQ==";
        final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + storageAccountName + ";AccountKey=" + storageAccountKey;

        //ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);

        String connectionString = "Endpoint=sb://dtna-ns.servicebus.windows.net/;SharedAccessKeyName=zonar;SharedAccessKey=qb5otAE6g7vyyU60uBHbWS50B8+xBK7oeW+gUO9rixc=;EntityPath=dtna";
        ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(connectionString);

        EventProcessorHost host = new EventProcessorHost(eventHubName, consumerGroupName, eventHubConnectionString.toString(), storageConnectionString);

        System.out.println("Registering host named " + host.getHostName());
        EventProcessorOptions options = new EventProcessorOptions();
        options.setExceptionNotification(new ErrorNotificationHandler());
        try
        {
            host.registerEventProcessor(EventProcessor.class, options).get();
        }
        catch (Exception e)
        {
            System.out.print("Failure while registering: ");
            if (e instanceof ExecutionException)
            {
                Throwable inner = e.getCause();
                System.out.println(inner.toString());
            }
            else
            {
                System.out.println(e.toString());
            }
        }

        System.out.println("Press enter to stop");
        try
        {
            System.in.read();
            host.unregisterEventProcessor();

            System.out.println("Calling forceExecutorShutdown");
            EventProcessorHost.forceExecutorShutdown(120);
        }
        catch(Exception e)
        {
            System.out.println(e.toString());
            e.printStackTrace();
        }

        System.out.println("End of sample");
    }
}

