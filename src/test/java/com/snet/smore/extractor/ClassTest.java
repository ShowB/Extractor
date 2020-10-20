package com.snet.smore.extractor;

import com.snet.smore.common.constant.FileStatusPrefix;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class ClassTest {
    @Test
    public void test() throws Exception {
        System.out.println(FileStatusPrefix.ERROR.toString());
        System.out.println(FileStatusPrefix.ERROR);
        Path path = Paths.get("./");
        List<Path> collect = Files.walk(path).collect(Collectors.toList());
        System.out.println(collect);

        System.out.println(path);
    }
}
