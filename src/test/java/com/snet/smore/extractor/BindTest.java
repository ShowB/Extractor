package com.snet.smore.extractor;

import com.snet.smore.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

@Slf4j
public class BindTest {
    Selector selector = null;
    ServerSocketChannel serverSocketChannel = null;

    @Test
    public void test() throws IOException {
        selector = Selector.open(); // Selector 열고
        serverSocketChannel = ServerSocketChannel.open(); // 채널 열고
        serverSocketChannel.configureBlocking(false); // Non-blocking 모드 설정
        InetSocketAddress address = new InetSocketAddress(12345);
        serverSocketChannel.bind(address); // 12345 포트를 열어줍니다.

        System.out.println(serverSocketChannel.isOpen());

        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        selector.close();
        serverSocketChannel.close();

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false); // Non-blocking 모드 설정
        serverSocketChannel.bind(address); // 12345 포트를 열어줍니다.

        System.out.println(serverSocketChannel.isOpen());


    }
}
