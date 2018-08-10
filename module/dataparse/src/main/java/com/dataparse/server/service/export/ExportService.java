package com.dataparse.server.service.export;

import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TasksRepository;
import com.dataparse.server.service.upload.CompositeDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

@Service
public class ExportService {

    public static final Integer MAX_ROWS_COUNT = 4_000_000;

    @Autowired
    private ExportRepository exportRepository;

    @Autowired
    private TasksRepository tasksRepository;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private IStorageStrategy storageStrategy;


    @Scheduled(cron = "0 0 12 * * ?")
    public void handleExportFileExpiration() {
        exportRepository.getExpired().forEach((exportEntity -> {
            storageStrategy.getDefault().removeFile(exportEntity.getResultFileId().toString());
            exportEntity.setRemoved(true);
            exportRepository.update(exportEntity);
        }));
    }

    public void withExportResults(String requestId, BiConsumer<InputStream, FileDescriptor> consumer){
        TaskInfo taskInfo = tasksRepository.getTaskInfo(requestId);
        if(taskInfo == null){
            throw new RuntimeException("Task does not exist");
        } else if(!taskInfo.isFinished()){
            throw new RuntimeException("Task has not finished yet");
        } else if(taskInfo.getResult() == null){
            throw new RuntimeException("Task result is empty");
        }
        ExportTaskResult result = (ExportTaskResult) taskInfo.getResult();
        ExportTaskRequest request = (ExportTaskRequest) taskInfo.getRequest();
        String datadocName = tableRepository.getDatadoc(request.getDatadocId()).getName();
        if(result.getDescriptor() instanceof CompositeDescriptor){
            CompositeDescriptor compositeDescriptor = (CompositeDescriptor) result.getDescriptor();
            List<InputStream> streams = new ArrayList<>();
            for(Descriptor descriptor: compositeDescriptor.getDescriptors()){
                if(descriptor instanceof FileDescriptor){
                    FileDescriptor fileDescriptor = (FileDescriptor) descriptor;
                    streams.add(storageStrategy.get(fileDescriptor).getFile(fileDescriptor.getPath()));
                } else {
                    throw new RuntimeException("Unsupported descriptor type");
                }
            }
            FileDescriptor tmpDescriptor = new FileDescriptor();
            tmpDescriptor.setContentType("text/csv");
            tmpDescriptor.setOriginalFileName(datadocName + "_export.csv");
            try(SequenceInputStream is = new SequenceInputStream(Collections.enumeration(streams))){
                consumer.accept(is, tmpDescriptor);
            } catch (IOException e) {
                throw new RuntimeException("Can't download export results");
            } finally {
                compositeDescriptor.getDescriptors().forEach(d -> {
                    FileDescriptor fileDescriptor = (FileDescriptor) d;
                    storageStrategy.get(fileDescriptor).removeFile(fileDescriptor.getPath());
                });
            }
        } else if (result.getDescriptor() instanceof FileDescriptor){
            FileDescriptor fileDescriptor = (FileDescriptor) result.getDescriptor();
            FileStorage fileStorage = storageStrategy.get(fileDescriptor);
            try(InputStream is = fileStorage.getFile(fileDescriptor.getPath())) {
                consumer.accept(is, fileDescriptor);
            } catch (IOException e) {
                throw new RuntimeException("Can't download export results");
            }
        } else {
            throw new RuntimeException("Unsupported descriptor type");
        }
    }

}
