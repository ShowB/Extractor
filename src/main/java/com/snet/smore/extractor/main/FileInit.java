package com.snet.smore.extractor.main;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.util.FileStatusPrefix;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileInit {
    public static void main(String[] args) {
        Path source = Paths.get(EnvManager.getProperty("extractor.source.file.dir"));

        if ("Y".equals(EnvManager.getProperty("extractor.source.file.init-required"))) {
            initSource(source, FileStatusPrefix.COMPLETE);
            initSource(source, FileStatusPrefix.ERROR);
            initSource(source, FileStatusPrefix.TEMP);
        }
    }

    private static void initSource(Path path, FileStatusPrefix prefix) {
        List<Path> list = new ArrayList<>();
        try (Stream<Path> temp = Files.find(path, Integer.MAX_VALUE, (p, a)
                -> p.getFileName().toString().startsWith(prefix.getPrefix())
                && !a.isDirectory())) {
            list = temp.collect(Collectors.toList());
        } catch (Exception e) {
            log.error("An error occurred while initializing files.", e);
        }

        int total = list.size();
        int curr = 0;

        for (Path p : list) {
            try {
                System.out.println("[" + (++curr) + " / " + total + "]" + "\t" + p);
                Files.move(p, Paths.get(p.getParent().toAbsolutePath().toString()
                        , p.getFileName().toString().replace(prefix.getPrefix(), "")));
            } catch (IOException e) {
                log.error("An error occurred while initializing files. {}", p, e);
            }
        }
    }

}
