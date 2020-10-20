package com.snet.smore.extractor.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.EncryptUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;
import com.snet.smore.extractor.main.ExtractorMain;
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
public class RabbitMQReceiveModule {
    private Queue<String> messageQueue = new ConcurrentLinkedDeque<>();
    private ScheduledExecutorService service;
    private ScheduledExecutorService monitorService;
    private int fileMakeIntervalTime = EnvManager.getProperty("extractor.source.rabbitmq.file-create-time", 10);
    private int fileMakeIntervalCount = EnvManager.getProperty("extractor.source.rabbitmq.file-create-count", 100);

    private Connection connection;
    private Channel channel;
    private Boolean isConsuming = false;

    public void execute() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(EnvManager.getProperty("extractor.source.rabbitmq.host"));
            factory.setVirtualHost(EnvManager.getProperty("extractor.source.rabbitmq.vhost"));
            factory.setUsername(EncryptUtil.getDecrypt(EnvManager.getProperty("extractor.source.rabbitmq.username")));
            factory.setPassword(EncryptUtil.getDecrypt(EnvManager.getProperty("extractor.source.rabbitmq.password")));

            String queueName = EnvManager.getProperty("extractor.source.rabbitmq.queue.name");

            if (connection == null)
                connection = factory.newConnection();

            if (channel == null)
                channel = connection.createChannel();

            if (!isConsuming) {
                channel.queueDeclare(queueName, true, false, false, null);
                System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

                channel.basicConsume(queueName, true, this::callback, consumerTag -> {
                });

                isConsuming = true;

                runFileMakeThread();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void runFileMakeThread() {
        if (service == null)
            service = Executors.newSingleThreadScheduledExecutor();

        if (monitorService == null)
            monitorService = Executors.newSingleThreadScheduledExecutor();

        service.scheduleWithFixedDelay(this::makeFile, fileMakeIntervalTime, fileMakeIntervalTime, TimeUnit.SECONDS);
        monitorService.scheduleWithFixedDelay(this::monitor, 1, 1, TimeUnit.SECONDS);
    }

    private void callback(String consumerTag, Delivery delivery) {
        String msg = new String(delivery.getBody());
        messageQueue.add(msg);

        while (messageQueue.size() > fileMakeIntervalCount * 2) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void makeFile() {
        if (messageQueue.size() == 0)
            return;

        String root = EnvManager.getProperty("extractor.target.file.dir", "./");

        Path path = Paths.get(root);

        try {
            Files.createDirectories(path);
        } catch (Exception e) {
            log.error("An error occurred while creating directories.", e);
        }

        Path newPath = Paths.get(root,
                FileStatusPrefix.TEMP.getPrefix()
                        + System.currentTimeMillis()
                        + "_"
                        + UUID.randomUUID().toString().substring(0, 8)
                        + ".txt");

        try (FileChannel channel = FileChannel.open(newPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (int i = 0; i < fileMakeIntervalCount; i++) {
                channel.write(ByteBuffer.wrap(messageQueue.remove().getBytes()));

                if (messageQueue.size() == 0)
                    break;
            }

            newPath = FileUtil.changeFileStatus(newPath, null);

            if (channel.size() == 0)
                Files.delete(newPath);
            else
                log.info("File was created. --> {}", newPath);

        } catch (Exception e) {
            log.error("An error occurred while writing file. {}", newPath);

            try {
                FileUtil.changeFileStatus(newPath, FileStatusPrefix.ERROR);
            } catch (Exception ex) {
                log.error("An error occurred while renaming file. [{}], [{}]", newPath, FileStatusPrefix.ERROR);
            }
        }
    }

    private void monitor() {
        if (!ExtractorMain.getIsRunningByMonitor()) {
            if (isConsuming)
                closeMQ();

            if (messageQueue.size() == 0) {
                service.shutdownNow();
                service = null;
                log.info("Extractor for RabbitMQ is shutdown now.");
            }

            if (!isConsuming && service == null) {
                monitorService.shutdownNow();
                monitorService = null;
                log.info("Agent monitor for RabbitMQ is shutdown now.");
            }
        }
    }

    private void closeMQ() {
        try {
            if (channel != null && channel.isOpen())
                channel.close();
        } catch (Exception e) {
            log.error("An error occurred while close channel.", e);
        }

        try {
            if (connection != null && connection.isOpen())
                connection.close();
        } catch (Exception e) {
            log.error("An error occurred while close connection.", e);
        }

        channel = null;
        connection = null;

        log.info("RabbitMQ receive channel and connection were closed.");
        isConsuming = false;
    }
}