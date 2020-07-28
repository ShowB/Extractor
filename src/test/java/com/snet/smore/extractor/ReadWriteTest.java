package com.snet.smore.extractor;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Slf4j
public class ReadWriteTest {

    @Test
    public void readTest() {
        Path path = Paths.get("config", "last_pk.info");

        if (!Files.isRegularFile(path))
            return;

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) Files.size(path));
            channel.read(byteBuffer);
            byteBuffer.flip();
            String s = Charset.defaultCharset().decode(byteBuffer).toString();
            JsonParser parser = new JsonParser();
            final JsonElement json = parser.parse(s);
            System.out.println(json);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void writeTest() {
        Path path = Paths.get("config", "test_last_pk.info");

        try (FileChannel channel = FileChannel.open(path
                , StandardOpenOption.CREATE
                , StandardOpenOption.WRITE
                , StandardOpenOption.TRUNCATE_EXISTING
                , StandardOpenOption.DELETE_ON_CLOSE)) {
            channel.write(ByteBuffer.wrap("TEST_PK=12344".getBytes()));
        } catch (IOException e) {
            log.error("An error occurred while storing last PK info file.", e);
        }
    }
}
