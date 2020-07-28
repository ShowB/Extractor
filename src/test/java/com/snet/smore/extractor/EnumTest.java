package com.snet.smore.extractor;

import com.snet.smore.extractor.util.FileStatusPrefix;
import org.junit.Test;

public class EnumTest {
    @Test
    public void test() {
        String a = FileStatusPrefix.COMPLETE.getPrefix();
        System.out.println(a);
        System.out.println(FileStatusPrefix.COMPLETE);
    }
}
