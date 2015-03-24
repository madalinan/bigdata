/*
 * Copyright (c) 2013 Amazon.com, Inc. All Rights Reserved.
 */
package com.logs.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

public class FileHelper {

    public static final IOFileFilter ALL_FILES_FILTER = FileHelper.getFilesFilter("");

    /**
     * Zip the files filtered by datasetFilter from fromPath to toPath.
     * 
     * @param fromPath
     * @param toPath
     * @param datasetFilter
     * @return the number of files zipped.
     * @throws IOException
     *             from accessing a file.
     */
    public static int zipFilesFromPath(final File fromPath, final File toPath, final String datasetFilter)
            throws IOException {
        System.out.println("Zip dataset files wich contain \"" + datasetFilter + "\" from path: "
                + fromPath.getAbsolutePath() + " to path: " + toPath.getAbsolutePath());
        if (!fromPath.exists()) {
            return 0;
        }
        Collection<File> fileList = FileUtils.listFiles(fromPath, getFilesFilter(datasetFilter), ALL_FILES_FILTER);
        for (File file : fileList) {
            File zipFile = new File(toPath, file.getName() + ".zip");
            zipFile(file, zipFile);
        }
        return fileList.size();
    }

    /**
     * Write data to a file with appending.
     * 
     * @param file
     * @param data
     * @throws IOException
     *             from writing data to a file.
     */
    public void appendDataToFile(File file, String data) throws IOException {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file, true));
            bw.write(data.toString());
            if (!data.equals("")) {
                bw.newLine();
            }
        } finally {
            if (bw != null) {
                bw.close();
            }
        }
    }

    /**
     * Write data from a file to a file.
     * 
     * @param fromfile
     * @param toFile
     * @throws IOException
     *             from reading/writing data to a file.
     */
    public void copyFromFileToFile(File fromfile, File toFile) throws IOException {
        String line = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(fromfile));
            appendDataToFile(toFile, ""); // create empty file.
            while ((line = br.readLine()) != null) {
                appendDataToFile(toFile, line);
            }
        } finally {
            IOUtils.closeQuietly(br);
        }
    }

    /**
     * Copy Json records to a file.
     * 
     * @param records
     * @param toFile
     * @throws IOException
     *             from writing data to a file.
     */
    public void copyRecordsToFile(List<String> records, File toFile) throws IOException {
        for (String record : records) {
            appendDataToFile(toFile, record.toString());
        }
    }

    /**
     * Zip the file "file" to zip file "toFile".
     * 
     * @param file
     * @param toFile
     * @throws IOException
     */
    public static void zipFile(File file, File toFile) throws IOException {
        String line = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = new BufferedReader(new FileReader(file));
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new FileOutputStream(toFile, true));
            bw = new BufferedWriter(new OutputStreamWriter(gzipOutputStream));
            bw.write(""); // create empty zip file.
            while ((line = br.readLine()) != null) {
                bw.write(line);
                bw.newLine();
            }
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(bw);
        }
    }

    /**
     * Create a new zip empty file "file".
     * 
     * @param file
     * @throws IOException
     */
    public void newZipEmptyFile(File file) throws IOException {
        BufferedWriter bw = null;
        try {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new FileOutputStream(file, false));
            bw = new BufferedWriter(new OutputStreamWriter(gzipOutputStream));
            bw.write(""); // create empty zip file.
        } finally {
            IOUtils.closeQuietly(bw);
        }
    }

    /**
     * Create a new zip empty file "file".
     * 
     * @param file
     * @throws IOException
     */
    public void readFile(File zipFile) throws IOException {
        BufferedWriter bw = null;
        try {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new FileOutputStream(zipFile, false));
            bw = new BufferedWriter(new OutputStreamWriter(gzipOutputStream));
            bw.write(""); // create empty zip file.
        } finally {
            IOUtils.closeQuietly(bw);
        }
    }

    /**
     * Unzip the zip file "file" to file "toFile".
     * 
     * @param file
     * @param toFile
     * @throws IOException
     */
    public void unzipFile(File file, File toFile) throws IOException {
        String line = null;
        BufferedReader br = null;
        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
            br = new BufferedReader(new InputStreamReader(gzipInputStream));
            while ((line = br.readLine()) != null) {
                appendDataToFile(toFile, line);
            }
        } catch (IOException e) {
            throw new IOException("Exception when unzipping file: " + file.getAbsolutePath() + " to file:  "
                    + toFile.getAbsolutePath(), e);
        } finally {
            IOUtils.closeQuietly(br);
        }
    }

    /**
     * Get a IOFileFilter instance which filter files containing filter value.
     * 
     * @param filter
     * @return
     */
    public static IOFileFilter getFilesFilter(final String filter) {
        IOFileFilter fileFilter = new IOFileFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return dir.getName().contains(name);
            }

            @Override
            public boolean accept(File file) {
                return file.getName().contains(filter);
            }
        };

        return fileFilter;
    }

    /**
     * Read the records from a file between a range in a list.
     * 
     * @throws IOException
     */
    public List<String> readRecordsFromFile(File file, long fromIndex, long toIndex) throws IOException {
        List<String> list = new ArrayList<String>();
        BufferedReader br = null;
        if (!file.exists()) {
            return list;
        }
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            long i = -1;
            while ((line = br.readLine()) != null) {
                i++;
                if (i >= fromIndex && i <= toIndex) {
                    list.add(line);
                }
            }
        } finally {
            IOUtils.closeQuietly(br);
        }
        return list;
    }

    /**
     * Create a File from filePath which should be deleted in application exit.
     * 
     * @param filePath
     * @return
     * @throws IOException
     *             from deleting on exit or creating the directories from filePath.
     */
    public File createTemporaryDirFile(String filePath) throws IOException {
        File file = new File(filePath);
        FileUtils.forceDeleteOnExit(file);
        FileUtils.forceMkdir(file);
        return file;
    }
}
