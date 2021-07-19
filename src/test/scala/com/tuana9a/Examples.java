package com.tuana9a;

import java.io.*;

public class Examples {
    private static final String EXAMPLES_DIR = "/mnt/sda1/RESOURCE/txt/";

    public static void genSingleLine800mb() {
        String path = EXAMPLES_DIR + "single-line-800mb.txt";
        Examples.genSingleLine(path, 3, 200000000);
    }

    public static void genMultiLine800mb() {
        String path = EXAMPLES_DIR + "multi-line-800mb.txt";
        Examples.genMultiLine(path, 3, 200000000, 20);
    }

    public static void genMultiLine(String path, int wordLength, int wordNumber, int numWordInLine) {
        File file = new File(path);
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        try {
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            for (int i = 1; i <= wordNumber; i++) {
                StringBuilder random = new StringBuilder();
                for (int j = 1; j <= wordLength; j++) {
                    int temp = (int) Math.floor(Math.random() * 10);
                    random.append(temp);
                }
                random.append(i % numWordInLine == 0 ? "\n" : " ");
                bos.write(random.toString().getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            close(bos);
            close(fos);
        }
    }

    public static void genSingleLine(String path, int wordLength, int wordNumber) {
        File file = new File(path);
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        try {
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            for (int i = 1; i <= wordNumber; i++) {
                StringBuilder random = new StringBuilder();
                for (int j = 1; j <= wordLength; j++) {
                    int temp = (int) Math.floor(Math.random() * 10);
                    random.append(temp);
                }
                random.append(" ");
                bos.write(random.toString().getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            close(bos);
            close(fos);
        }
    }

    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }
    }

}
