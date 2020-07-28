package com.snet.smore.extractor;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.util.FileStatusPrefix;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileFindTest {

    @Test
    public void test() {
        String expression = EnvManager.getProperty("extractor.source.file.glob");
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + expression);

        Path source = Paths.get(EnvManager.getProperty("extractor.source.file.dir"));

        List<Path> zip = new ArrayList<>();
        try (Stream<Path> pathStream = Files.find(source, Integer.MAX_VALUE,
                (p, a) -> matcher.matches(p.getFileName())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.COMPLETE.getPrefix())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.ERROR.getPrefix())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.TEMP.getPrefix())
                        && !a.isDirectory()
                        && a.isRegularFile())) {
            zip = pathStream.collect(Collectors.toList());
        } catch (Exception e) {
            log.error("An error occurred while finding source files.", e);
        }

        System.out.println(zip.size());


    }
}
