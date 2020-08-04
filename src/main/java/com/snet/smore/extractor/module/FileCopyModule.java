package com.snet.smore.extractor.module;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.FileUtil;
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

        Path root = Paths.get(EnvManager.getProperty("extractor.source.file.dir"));
        String glob = EnvManager.getProperty("extractor.source.file.glob");

        List<Path> targetFiles = FileUtil.findFiles(root, glob);


        int total = targetFiles.size();
        int curr = 0;
        Path targetPrefix = Paths.get(EnvManager.getProperty("extractor.target.file.dir"));

        for (Path p : targetFiles) {
            if (!Files.isRegularFile(p) || Files.isDirectory(p))
                continue;

            Path targetDir = Paths.get(targetPrefix.toAbsolutePath().toString(), getRelativeParent(p));

            // 1. 파일명 tmp_ 붙이기 (다른 프로세스에서 사용중인 파일인지 확인)
            Path temp;
            try {
                temp = FileUtil.changeFileStatus(p, FileStatusPrefix.TEMP);
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
                    FileUtil.changeFileStatus(temp, FileStatusPrefix.ERROR);
                } catch (IOException ex) {
                    log.error("An error occurred while changing file name. [{}], {}", FileStatusPrefix.ERROR, p, e);
                }

                continue;
            }

            // 4. 파일명 앞에 tmp_ 지우고 cmpl_ 붙이기
            try {
                FileUtil.changeFileStatus(temp, FileStatusPrefix.COMPLETE);
            } catch (IOException e) {
                log.error("An error occurred while copying files. {}", p, e);

                try {
                    FileUtil.changeFileStatus(temp, FileStatusPrefix.ERROR);
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

}
