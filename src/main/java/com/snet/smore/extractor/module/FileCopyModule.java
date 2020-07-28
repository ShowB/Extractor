package com.snet.smore.extractor.module;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.util.FileStatusPrefix;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileCopyModule {
    public void execute() {
        long start = System.currentTimeMillis();

        String glob = EnvManager.getProperty("extractor.source.file.glob");
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);

        Path source = Paths.get(EnvManager.getProperty("extractor.source.file.dir"));

        List<Path> zips = new ArrayList<>();
        try (Stream<Path> pathStream = Files.find(source, Integer.MAX_VALUE,
                (p, a) -> matcher.matches(p.getFileName())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.COMPLETE.getPrefix())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.ERROR.getPrefix())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.TEMP.getPrefix())
                        && !a.isDirectory()
                        && a.isRegularFile())) {
            zips = pathStream.collect(Collectors.toList());
        } catch (Exception e) {
            log.error("An error occurred while finding source files.", e);
        }


        int total = zips.size();
        int curr = 0;
        Path targetPrefix = Paths.get(EnvManager.getProperty("extractor.target.dir"));

        for (Path p : zips) {
            if (!Files.isRegularFile(p) || Files.isDirectory(p))
                continue;

            Path targetDir = Paths.get(targetPrefix.toAbsolutePath().toString(), getRelativeParent(p));

            // 1. 파일명 tmp_ 붙이기 (다른 프로세스에서 사용중인 파일인지 확인)
            Path temp;
            try {
                temp = changeFileStatus(p, FileStatusPrefix.TEMP);
            } catch (IOException e) {
                log.error("File is using by another process. {}", p);
                continue;
            }

            // 2. 대상 디렉토리 생성
            try {
                Files.createDirectories(targetDir);
            } catch (IOException e) {
                log.error("An error occurred while creating directory. [{}]", targetDir, e);
                continue;
            }

            // 3. 파일 카피
            Path result;
            try {
                result = Files.copy(temp, Paths.get(targetDir.toAbsolutePath().toString(), p.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                log.error("An error occurred while copying files. {}", p, e);

                try {
                    changeFileStatus(temp, FileStatusPrefix.ERROR);
                } catch (IOException ex) {
                    log.error("An error occurred while changing file name. [{}], {}", FileStatusPrefix.ERROR, p, e);
                }

                continue;
            }

            // 4. 파일명 앞에 tmp_ 지우고 cmpl_ 붙이기
            try {
                changeFileStatus(temp, FileStatusPrefix.COMPLETE);
            } catch (IOException e) {
                log.error("An error occurred while copying files. {}", p, e);

                try {
                    changeFileStatus(temp, FileStatusPrefix.ERROR);
                } catch (IOException ex) {
                    log.error("An error occurred while changing file name. [{}], {}", FileStatusPrefix.ERROR, p, e);
                }

                continue;
            }

            System.out.println("[" + (++curr) + " / " + total + "]" + "\tFile copy completed.\t" + result);
        }

        long end = System.currentTimeMillis();

        if (curr > 0) {
            log.info("Count of created files: " + curr);
            log.info("Turn Around Time: " + ((end - start) / 1000) + " (seconds)");
        }

    }

    private String getRelativeParent(Path path) {
        Path root = Paths.get(EnvManager.getProperty("extractor.source.file.dir"));
        path = path.getParent();
        StringBuilder sb = new StringBuilder();

        while (!root.equals(path)) {
            sb.insert(0, "/" + path.getFileName().toString());
            path = path.getParent();
        }

        return sb.toString();
    }

    private Path changeFileStatus(Path originPath, FileStatusPrefix prefix) throws IOException {
        String fileName = originPath.getFileName().toString();

        for (FileStatusPrefix f : FileStatusPrefix.values()) {
            if (fileName.startsWith(f.getPrefix()))
                fileName = fileName.replace(f.getPrefix(), "");
        }

        fileName = prefix.getPrefix() + fileName;

        Path changePath = Paths.get(originPath.getParent().toAbsolutePath().toString(), fileName);
        return Files.move(originPath, changePath);
    }

}
