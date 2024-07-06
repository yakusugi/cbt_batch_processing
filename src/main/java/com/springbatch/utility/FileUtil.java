package com.springbatch.utility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

public class FileUtil {

    public static Optional<Path> getLatestFile(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(Files::isRegularFile)
                    .max(Comparator.comparingLong(FileUtil::getFileTimestamp));
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private static long getFileTimestamp(Path file) {
        try {
            return Files.readAttributes(file, BasicFileAttributes.class).lastModifiedTime().toMillis();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static void main(String[] args) {
        Path directory = Paths.get("src/main/resources/data");
        Optional<Path> latestFile = getLatestFile(directory);
        latestFile.ifPresent(path -> System.out.println("Latest file: " + path.getFileName()));
    }

}
