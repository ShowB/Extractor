package com.snet.smore.extractor;

import com.snet.smore.common.constant.FileStatusPrefix;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class LogTest {

    @Test
    public void test() {
        FileStatusPrefix prefix = FileStatusPrefix.COMPLETE;

        try {
            Integer.parseInt("adbwsedfsd");
        } catch (Exception e) {
            log.error("An error occurred while initializing [{}] files.", prefix, e);
        }
    }
}
