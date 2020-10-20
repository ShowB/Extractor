package com.snet.smore.extractor;

import com.rabbitmq.client.*;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.EncryptUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RabbitMQReceiveTest {
    private final static String QUEUE_NAME = EnvManager.getProperty("extractor.source.rabbitmq.queue.name");
    private static Queue<String> messageQueue = new ConcurrentLinkedDeque<>();
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    //    @Test
    public static void main(String[] args) throws Exception {

        RabbitMQReceiveTest main = new RabbitMQReceiveTest();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(EnvManager.getProperty("extractor.source.rabbitmq.host"));
        factory.setVirtualHost(EnvManager.getProperty("extractor.source.rabbitmq.vhost"));
        factory.setUsername(EncryptUtil.getDecrypt(EnvManager.getProperty("extractor.source.rabbitmq.username")));
        factory.setPassword(EncryptUtil.getDecrypt(EnvManager.getProperty("extractor.source.rabbitmq.password")));

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = main::customCallback;
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });

        main.test();

    }

    private void test() {
        service.scheduleWithFixedDelay(this::makeFile, 3, 3, TimeUnit.SECONDS);
    }

    private void customCallback(String consumerTag, Delivery delivery) {
        String msg = new String(delivery.getBody());
        messageQueue.add(msg);

        while (messageQueue.size() > 100) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void makeFile() {
        if (messageQueue.size() == 0)
            return;

        String root = "D:/SMORE_DATA/EXTRACTOR_SOURCE/tcms_sample";
        String subDir = "kovis";

        Path path = Paths.get(root, subDir);

        try {
            Files.createDirectories(path);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Path newPath = Paths.get(root, subDir, FileStatusPrefix.TEMP.getPrefix() + "sample_" + UUID.randomUUID() + ".txt");

        try (FileChannel channel = FileChannel.open(newPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (int i = 0; i < 100; i++) {
                channel.write(ByteBuffer.wrap(messageQueue.remove().getBytes()));

                if (messageQueue.size() == 0)
                    break;
            }

            newPath = FileUtil.changeFileStatus(newPath, null);

            if (channel.size() == 0)
                Files.delete(newPath);

            log.info("File was created. [{}] ... Remained Queue Size [{}]", newPath, messageQueue.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
