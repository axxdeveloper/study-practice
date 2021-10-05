package examples;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import shooeugenesea.EchoRequest;
import shooeugenesea.EchoResponse;
import shooeugenesea.EchoServiceGrpc;

public class EchoMain {

    private static ExecutorService executorService = Executors.newCachedThreadPool();
    private static final int PORT = 12345;

    public static void main(String[] args) throws IOException {
        executorService.execute(() -> {
            try {
                startServer();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // client code
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext().build();

        EchoServiceGrpc.EchoServiceBlockingStub echoServiceStub = EchoServiceGrpc.newBlockingStub(channel);
        String message = UUID.randomUUID().toString();
        EchoResponse response = echoServiceStub.echo(EchoRequest.newBuilder().setMessage(message).build());
        System.out.println("Send " + message + ", receive:" + response.getMessage());
        assert message.equals(response.getMessage());

        System.exit(0);
    }

    private static void startServer() throws IOException {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new EchoServiceImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                try {
                    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static class EchoServiceImpl extends EchoServiceGrpc.EchoServiceImplBase {
        @Override
        public void echo(EchoRequest request, StreamObserver<EchoResponse> responseObserver) {
            responseObserver.onNext(EchoResponse.newBuilder().setMessage(request.getMessage()).build());
            responseObserver.onCompleted();
        }
    }

}
