package com.dataparse.server.service.zip;

import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.File;
import com.dataparse.server.service.files.Folder;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.util.CountingInputStream;
import com.dataparse.server.util.Debounce;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zeroturnaround.zip.ZipUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@Service
public class FileZipService {

    final static byte[] EmptyZip={80,75,05,06,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00};

    @Autowired
    @JsonIgnore
    private UploadRepository uploadRepository;

    @Autowired
    @JsonIgnore
    private IStorageStrategy storageStrategy;

    private java.io.File createEmptyZip(){
        try{
            java.io.File arc = java.io.File.createTempFile("dataparse", "archive");
            FileOutputStream fos=new FileOutputStream(arc);
            fos.write(EmptyZip, 0, 22);
            fos.flush();
            fos.close();
            return arc;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void addFileToArchive(File file, java.io.File arc, String path, Consumer<Integer> progressCallback){
        if(Thread.currentThread().isInterrupted()){
            throw new RuntimeException("Compressing interrupted externally");
        }
        FileDescriptor fileDescriptor = (FileDescriptor) file.getDescriptor();
        AtomicInteger lastRead = new AtomicInteger(0);
        try(InputStream is = storageStrategy.get(fileDescriptor).getFile(fileDescriptor.getPath());
            CountingInputStream cis = new CountingInputStream(is, new Debounce<>(i -> {
                progressCallback.accept(i - lastRead.getAndSet(i));
            }, 1000))){
            long size = storageStrategy.get(fileDescriptor).getFileSize(fileDescriptor.getPath());
            ZipUtil.addEntry(arc, new InputStreamSource(path + file.getName(), cis, size));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void addFolderToArchive(Long userId, java.io.File arc, String path, Folder folder, Consumer<Integer> progressCallback){
        List<AbstractFile> files = uploadRepository.getFiles(userId, folder.getId());
        for(AbstractFile abstractFile : files){
            if(abstractFile instanceof Folder){
                addFolderToArchive(userId, arc, path + abstractFile.getName() + "/", (Folder) abstractFile, progressCallback);
            } else {
                File file = (File) abstractFile;
                addFileToArchive(file, arc, path, progressCallback);
            }
        }
    }

    public FileDescriptor zipFolder(Long userId, Folder folder, Consumer<Integer> progressCallback){
        try {
            java.io.File arc = createEmptyZip();
            addFolderToArchive(userId, arc, folder.getName() + "/", folder, progressCallback);
            FileDescriptor descriptor = new FileDescriptor();
            descriptor.setContentType("application/zip");
            descriptor.setOriginalFileName(folder.getName() + ".zip");
            String path = storageStrategy.get(descriptor).saveFile(new FileInputStream(arc));
            descriptor.setPath(path);
            return descriptor;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public FileDescriptor zipFiles(Long userId, List<AbstractFile> files, Consumer<Integer> progressCallback){
        try {
            java.io.File arc = createEmptyZip();
            for(AbstractFile abstractFile: files) {
                if(abstractFile instanceof Folder) {
                    Folder folder = (Folder) abstractFile;
                    addFolderToArchive(userId, arc, folder.getName() + "/", folder, progressCallback);
                } else {
                    File file = (File) abstractFile;
                    addFileToArchive(file, arc, "", progressCallback);
                }
            }
            FileDescriptor descriptor = new FileDescriptor();
            descriptor.setFormat(DataFormat.UNDEFINED);
            descriptor.setContentType("application/zip");
            descriptor.setOriginalFileName("CDownload.zip");
            String path = storageStrategy.get(descriptor).saveFile(new FileInputStream(arc));
            descriptor.setPath(path);
            return descriptor;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int getNestedFilesSize(Long userId, AbstractFile file){
        int count = 0;
        if(file instanceof Folder){
            Folder folder = (Folder) file;
            List<AbstractFile> files = uploadRepository.getFiles(userId, folder.getId());
            for(AbstractFile childFile : files){
                count += getNestedFilesSize(userId, childFile);
            }
        } else if (file instanceof File) {
            if((((File) file).getDescriptor() instanceof FileDescriptor)){
                FileDescriptor descriptor = (FileDescriptor) ((File) file).getDescriptor();
                count += storageStrategy.get(descriptor).getFileSize(descriptor.getPath());
            }
        }
        return count;
    }

    public FileDescriptor zip(Long userId, List<Long> fileIds, Consumer<Double> progressCallback){
        if(fileIds.isEmpty()){
            throw new RuntimeException("Files to archive are not selected!");
        }
        int tmp = 0;
        for(Long fileId: fileIds){
            AbstractFile file = uploadRepository.getFile(fileId);
            tmp += getNestedFilesSize(userId, file);
        }
        int size = tmp;
        log.info("Creating zip archive containing files of total size {}", size);

        AtomicInteger zipped = new AtomicInteger(0);
        Consumer<Integer> callback = i -> {
            progressCallback.accept(zipped.addAndGet(i) / (double) size);
        };
        if(fileIds.size() == 1) {
            AbstractFile file = uploadRepository.getFile(fileIds.iterator().next());
            if (file instanceof Folder) {
                return zipFolder(userId, (Folder) file, callback);
            } else {
                throw new RuntimeException("This upload can not be put in the archive!");
            }
        } else {
            List<AbstractFile> files = new ArrayList<>();
            for(Long fileId : fileIds){
                files.add(uploadRepository.getFile(fileId));
            }
            return zipFiles(userId, files, callback);
        }
    }

}
