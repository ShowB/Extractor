package com.snet.smore.extractor.module;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.extractor.main.ExtractorMain;
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

            while (ExtractorMain.getIsRunningByMonitor()) {
                Socket client = server.accept();

                if (client != null && ExtractorMain.getIsRunningByMonitor()) {
                    new Thread(new SocketReader(client)).start();
                } else if (!ExtractorMain.getIsRunningByMonitor()) {
                    log.info("Signal for server close was received.");

                    if (client != null)
                        client.close();

                    server.close();
                }

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
            int intervalTime = EnvManager.getProperty("extractor.source.socket.file-create-time", 600);
            int intervalLine = EnvManager.getProperty("extractor.source.socket.file-create-count", 600);
            int intervalSize = EnvManager.getProperty("extractor.source.socket.file-create-size", 1024);

            int streamSize = EnvManager.getProperty("extractor.source.socket.stream-size", 1024);
            long intervalEnd = System.currentTimeMillis() + (intervalTime * 1000);

            byte[] bytes = new byte[streamSize];

            String root = EnvManager.getProperty("extractor.target.file.dir");
            String fileName = System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
            Path newFile = null;
            FileChannel fileChannel = null;

            long lastReceivedTime = System.currentTimeMillis();
            int timeLimit = EnvManager.getProperty("extractor.source.socket.wait-limit", 3600);

            int count = 0;

            try {
                Files.createDirectories(Paths.get(root));
                newFile = Files.createFile(Paths.get(root, FileStatusPrefix.TEMP.getPrefix() + fileName));
                fileChannel = FileChannel.open(newFile, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE
                        , StandardOpenOption.TRUNCATE_EXISTING);

                while (!client.isClosed()) {
                    if (dis.available() >= streamSize) {
                        dis.read(bytes, 0, streamSize);

                        ByteBuffer buffer = ByteBuffer.wrap(bytes);
                        fileChannel.write(buffer);
                        System.out.print(".");
                        buffer.clear();
                        lastReceivedTime = System.currentTimeMillis();
                        count++;
                    }

                    if (count >= intervalLine
                            || System.currentTimeMillis() >= intervalEnd
                            || (fileChannel.size() / 1024) >= intervalSize) {

                        count = 0;
                        intervalEnd = System.currentTimeMillis() + (intervalTime * 1000);

                        if (fileChannel.size() > 0) {
                            log.info("File created. --> {}", Files.move(newFile, Paths.get(root, fileName)));
                            fileChannel.close();

                            fileName = System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
                            newFile = Files.createFile(Paths.get(root, FileStatusPrefix.TEMP.getPrefix() + fileName));

                            fileChannel = FileChannel.open(newFile, StandardOpenOption.CREATE,
                                    StandardOpenOption.WRITE
                                    , StandardOpenOption.TRUNCATE_EXISTING);
                        }
                    }

                    if (System.currentTimeMillis() > lastReceivedTime + (timeLimit * 1000)) {
                        log.info("Time out !! Socket will be closed. [{}]", client.getInetAddress());
                        break;
                    }

                    if (!ExtractorMain.getIsRunningByMonitor()) {
                        break;
                    }


                    Thread.sleep(10);
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
                    if (client != null) {
                        log.info("Client socket closed. [{}]", client.getInetAddress());
                        client.close();
                    }
                } catch (IOException e) {
                    log.error("An error occurred while writing file. [{}]", client.getInetAddress(), e);
                }
            }
        }
    }
}