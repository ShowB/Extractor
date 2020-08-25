package com.snet.smore.extractor.main;

import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.FileUtil;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

public class MakeBinaryFile {
    public static void main(String[] args) throws Exception {
        int byteSize = 1234 + 1318;
        byte[] bytes = new byte[byteSize];

        String root = "D:/smore/EXTRACTOR_SOURCE/tcms_sample";
        String subDir = "10minutes";

        int fileCnt = 20;
        int rowCnt = 600;

        Path path = Paths.get(root, subDir);
        Files.createDirectories(path);

        Path newPath;
//        Random random = new Random();
        for (int i = 0; i < fileCnt; i++) {
            newPath = Paths.get(root, subDir, FileStatusPrefix.TEMP.getPrefix() + "sample_" + UUID.randomUUID() + ".bin");

            try (FileChannel channel = FileChannel.open(newPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                for (int j = 0; j < rowCnt; j++) {
//                    random.nextBytes(bytes);
//                    channel.write(ByteBuffer.wrap(bytes));
                    for (int n = 0; n < byteSize; n++) {
//                        channel.write(ByteBuffer.wrap(UUID.randomUUID().toString().substring(0, 1).getBytes()));
                        channel.write(ByteBuffer.wrap("1".getBytes()));
                    }
                }

                newPath = FileUtil.changeFileStatus(newPath, null);
                System.out.println((i + 1) + "번째 파일 생성 완료. --> " + newPath);
            }
        }
    }
}





