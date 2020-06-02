package net.munki.play.grpc;

import com.google.protobuf.Descriptors;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MessageServer {

    private static final Logger logger = Logger.getLogger(MessageServer.class.getName());

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new JBotServiceImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    MessageServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final MessageServer server = new MessageServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class JBotServiceImpl extends JBotServiceGrpc.JBotServiceImplBase {

        CopyOnWriteArrayList<JBot> botRequests = new CopyOnWriteArrayList<>();

        @Override
        public void register(JBot bot, StreamObserver<JBotReply> responseObserver) {
            boolean success = botRequests.add(bot);
            JBotReply reply = JBotReply.newBuilder().setResult(success).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void unregister(JBot bot, StreamObserver<JBotReply> responseObserver) {
            boolean success = botRequests.remove(bot);
            JBotReply reply = JBotReply.newBuilder().setResult(success).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void getBot(JBot bot, StreamObserver<JBot> responseObserver) {
            JBot myBot = null;
            for (JBot jbot : botRequests) {
                if (bot.getName().equals(jbot.getName()))  {
                    myBot = jbot;
                    break;
                }
            }
            responseObserver.onNext(myBot);
            responseObserver.onCompleted();
        }

        @Override
        public void message(JBot botToExclude, StreamObserver<JBotReply> responseObserver) {
            for (JBot jbot : botRequests) {
                if (!jbot.getName().equals(botToExclude.getName())) {
                    //JBot.Builder builder = jbot.toBuilder();
                    //setFieldByName(builder, "message", botToExclude.getMessage());
                    jbot.newBuilderForType().setMessage(botToExclude.getMessage());
                }
            }
            JBotReply reply = JBotReply.newBuilder().setResult(true).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        public static void setFieldByName(JBot.Builder builder, String name, Object value) {

            Descriptors.FieldDescriptor fieldDescriptor = builder.getDescriptorForType().findFieldByName(name);

            if (value == null) {

                builder.clearField(fieldDescriptor);

            } else {

                builder.setField(fieldDescriptor, value);

            }

        }
    }

}
