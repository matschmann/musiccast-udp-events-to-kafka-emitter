package net.rainr.musiccast.application;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EmitKafkaEventsServer {
    private static final String TOPIC = "musiccast-events";

    private static final int UDP_PORT = 42549;


    private final KafkaProducer<String, String> producer;

    public static void main(String... args) {
        try {
            final var echoServer = new EmitKafkaEventsServer();
            echoServer.run();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DatagramSocket socket;
    private boolean running;
    private byte[] buf = new byte[256];
 
    public EmitKafkaEventsServer() throws SocketException {
        activateUdpEvents();
        socket = new DatagramSocket(UDP_PORT);

        Properties settings = setUpProperties();
        producer = new KafkaProducer<>(settings);


    }

    public void activateUdpEvents() {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://192.168.178.43/YamahaExtendedControl/v1/netusb/getPlayInfo"))
            .header("X-AppName", "MusicCast/musiccast-udp-events-to-kafka-emitter")
            .header("X-AppPort", Integer.toString(UDP_PORT))
            .build();
        client.sendAsync(request, BodyHandlers.ofString())
            .thenApply(HttpResponse::body)
            .thenAccept(System.out::println)
            .join();
    }
 
    public void run() throws Exception {
        running = true;
 
        while (running) {
            DatagramPacket packet
              = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
             
            InetAddress address = packet.getAddress();
            int port = packet.getPort();
            packet = new DatagramPacket(buf, buf.length, address, port);
            String received 
              = new String(packet.getData(), 0, packet.getLength());

            producer.send(new ProducerRecord<>(TOPIC, received));

            if (received.equals("end")) {
                running = false;
                continue;
            }
            socket.send(packet);
        }
        socket.close();
    }


    private static Properties setUpProperties() {
        Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, "musiccast-event-producer");
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        return settings;
    }

}