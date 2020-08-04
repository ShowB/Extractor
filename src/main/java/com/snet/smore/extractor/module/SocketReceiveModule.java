package com.snet.smore.extractor.module;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.constant.FileStatusPrefix;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

@Slf4j
public class SocketReceiveModule {
    public void execute() {
        try {
            int port = Integer.parseInt(EnvManager.getProperty("extractor.source.socket.port"));
            ServerSocket server = new ServerSocket(port);

            log.info("Socket server started(port: {}). Wait for client ...", port);

            while (true) {
                Socket client = server.accept();

                if (client != null)
                    new Thread(new SocketReader(client)).start();

            }
        } catch (Exception e) {
            log.error("An error occurred while creating socket thread.", e);
        }
    }

    private class SocketReader extends Thread {
        private Socket client;
        private DataInputStream dis;

        SocketReader(Socket client) {
            this.client = client;

            try {
                this.dis = new DataInputStream(this.client.getInputStream());
                log.info("Successfully connected to client. [{}]", this.client.getInetAddress());

            } catch (IOException e) {
                log.error("An error occurred while connecting to client.");
            }
        }

        @Override
        public void run() {
            int intervalTime;
            int intervalTimeDefault = 600;

            try {
                intervalTime = Integer.parseInt(EnvManager.getProperty("extractor.source.socket.file-create-time"));
            } catch (Exception e) {
                log.info("Cannot convert value [extractor.source.socket.file-create-time]. " +
                                "System will be set default value: {} (seconds)", intervalTimeDefault);
                intervalTime = intervalTimeDefault;
            }

            int intervalLine;
            int intervalLineDefault = 600;
            try {
                intervalLine = Integer.parseInt(EnvManager.getProperty("extractor.source.socket.file-create-line"));
            } catch (Exception e) {
                log.info("Cannot convert value [extractor.source.socket.file-create-line]. " +
                                "System will be set default value: {} (seconds)", intervalLineDefault);
                intervalLine = intervalLineDefault;
            }

            int byteSize;

            try {
                byteSize = Integer.parseInt(EnvManager.getProperty("extractor.source.socket.byte-size"));
            } catch (Exception e) {
                log.info("Cannot convert value [extractor.source.socket.byte-size]. Job will be restarted.");
                return;
            }

            long intervalEnd = System.currentTimeMillis() + (intervalTime * 1000);
            byte[] bytes = new byte[byteSize];

            String root = EnvManager.getProperty("extractor.target.file.dir");
            String fileName = System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
            Path newFile = null;
            FileChannel fileChannel = null;

            long lastReceivedTime = System.currentTimeMillis();
            long timeLimit;
            long timeLimitDefault = 3600;

            try {
                timeLimit = Long.parseLong(EnvManager.getProperty("extractor.source.socket.wait-limit"));
            } catch (Exception e) {
                log.info("Cannot convert value [extractor.source.socket.file-create-line]. " +
                                "System will be set default value: {} (seconds)", timeLimitDefault);
                timeLimit = timeLimitDefault;
            }

            int count = 0;

            try {
                Files.createDirectories(Paths.get(root));
                newFile = Files.createFile(Paths.get(root, FileStatusPrefix.TEMP.getPrefix() + fileName));
                fileChannel = FileChannel.open(newFile, StandardOpenOption.WRITE);

                while (client.isConnected()) {
                    if (dis.available() >= byteSize) {
                        dis.read(bytes, 0, byteSize);

                        ByteBuffer buffer = ByteBuffer.wrap(bytes);
                        fileChannel.write(buffer);
                        buffer.clear();
                        lastReceivedTime = System.currentTimeMillis();
                        count++;
                    }

                    if (count >= intervalLine || System.currentTimeMillis() >= intervalEnd) {
                        count = 0;
                        intervalEnd = System.currentTimeMillis() + (intervalTime * 1000);

                        if (fileChannel.size() > 0) {
                            log.info("File created. --> {}", Files.move(newFile, Paths.get(root, fileName)));
                            fileChannel.close();

                            fileName = System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
                            newFile = Files.createFile(Paths.get(root, FileStatusPrefix.TEMP.getPrefix() + fileName));

                            fileChannel = FileChannel.open(newFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                        }
                    }

                    if (System.currentTimeMillis() > lastReceivedTime + (timeLimit * 1000)) {
                        log.info("Time out !! Socket will be closed. [{}]", client.getInetAddress());
                        break;
                    }


                    Thread.sleep(100);
                }

            } catch (Exception e) {
                log.error("An error occurred while reading socket. [{}]", client.getInetAddress(), e);
            } finally {
                try {
                    if (newFile != null && newFile.getFileName().toString().startsWith(FileStatusPrefix.TEMP.getPrefix())) {
                        if (count > 0)
                            log.info("File created. --> {}", Files.move(newFile, Paths.get(root, fileName)));
                        else
                            Files.delete(newFile);
                    }

                    if (fileChannel != null && fileChannel.isOpen())
                        fileChannel.close();

                    if (dis != null) dis.close();
                    if (client != null) client.close();
                } catch (IOException e) {
                    log.error("An error occurred while writing file. [{}]", client.getInetAddress(), e);
                }
            }
        }
    }
}