package net.munki.play.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageClient {
    private static final Logger logger = Logger.getLogger(MessageClient.class.getName());
    private static JBotServiceGrpc.JBotServiceBlockingStub blockingStub = null;
    private static final String target = "localhost:50051";
    private ManagedChannel channel;

    public MessageClient() {
        channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        blockingStub = JBotServiceGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) throws Exception {
        MessageClient client = new MessageClient();
        try {
            CopyOnWriteArrayList<JBot> bots = new CopyOnWriteArrayList<>();
            if (blockingStub == null) throw new Exception("Blocking Stub was null.");
            for (int i = 0; i < 1000; i++) {
                JBot mybot = JBot.newBuilder().setName("munkinut_" + i).build();
                client.register(mybot);
                bots.add(mybot);
            }

            for (JBot mybot : bots) {
                JBot jbot = client.getBot(mybot);
                logger.info("Returned bot's name is " + jbot.getName());
            }

            for (JBot mybot : bots) {
                client.unregister(mybot);
                bots.remove(mybot);
            }

        } finally {
            client.stop();
        }

    }

    public void stop() {
        try {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void register(JBot bot) {
        logger.info("Will try to register " + bot.getName() + " ...");
        JBotReply response;
        try {
            response = blockingStub.register(bot);
            logger.info("Registered: " + response.getResult());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
    }

    public void unregister(JBot bot) {
        logger.info("Will try to unregister " + bot.getName() + " ...");
        JBotReply response;
        try {
            response = blockingStub.unregister(bot);
            logger.info("Unregistered: " + response.getResult());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }

    }

    public JBot getBot(JBot bot) {
        logger.info("Will try to get bot called " + bot.getName() + " ...");
        JBot myBot = null;
        try {
            myBot = blockingStub.getBot(bot);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
        return myBot;
    }

    public boolean message(JBot botToExclude) {
        logger.info("Will try to message bots with " + botToExclude.getMessage() + " ...");
        boolean result = false;
        try {
            result = blockingStub.message(botToExclude).getResult();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
        return result;
    }

}
