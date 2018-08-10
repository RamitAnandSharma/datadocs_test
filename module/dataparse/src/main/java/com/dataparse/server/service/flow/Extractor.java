package com.dataparse.server.service.flow;

import java.io.*;
import java.util.*;
import java.util.zip.*;

public class Extractor {

    static byte SPACE = 32;
    static byte LINE_FEED = 10;
    static byte CARRIAGE_RETURN = 13;
    static byte QUOTE = 34;

//    aws emr create-cluster --name="Impala 2.2.0" --ami-version=3.7.0 --applications Name=hive --ec2-attributes KeyName=dataparse,InstanceProfile=EMR_EC2_DefaultRole --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m3.xlarge --bootstrap-action Name="Install Impala2",Path="s3://support.elasticmapreduce/bootstrap-actions/impala/impala-install" --service-role EMR_DefaultRole


    public static void main(String[] args) throws Exception {

//        join();
//        removeNewLines();
        breakIntoSmallerFiles();
    }

    private static void breakIntoSmallerFiles() throws Exception {
        File copy = new File("/home/user/Downloads/copy.csv");
        File copy2 = new File("/home/user/Downloads/copy_100K.csv");
        File copy3 = new File("/home/user/Downloads/copy_1M.csv");
        File copy4 = new File("/home/user/Downloads/copy_10M.csv");

        BufferedReader reader = new BufferedReader(new FileReader(copy));
        BufferedWriter w1 = new BufferedWriter(new FileWriter(copy2));
        BufferedWriter w2 = new BufferedWriter(new FileWriter(copy3));
        BufferedWriter w3 = new BufferedWriter(new FileWriter(copy4));
        String lineToCopy;
        int i = 0;
        while((lineToCopy = reader.readLine()) != null) {
            if(i < 100_000){
                w1.write(lineToCopy);
                w1.newLine();
            }
            if(i == 100_000){
                System.out.println(100000);
                w1.flush();
                w1.close();
            }
            if(i < 1000_000) {
                w2.write(lineToCopy);
                w2.newLine();
            }
            if(i == 1000_000){
                System.out.println(1000000);
                w2.flush();
                w2.close();
            }
            if(i < 10_000_000) {
                w3.write(lineToCopy);
                w3.newLine();
            }
            if(i == 10_000_000) {
                System.out.println(10000000);
                w3.flush();
                w3.close();
            }
            i++;
        }
    }

    private static void removeNewLines() throws Exception {
        FileInputStream fis = new FileInputStream(new File("/home/user/Downloads/joined.csv"));
        FileOutputStream fos = new FileOutputStream(new File("/home/user/Downloads/copy.csv"));
        int c;
        c = 1;
        long time = System.currentTimeMillis();
        boolean quoted = false;

        final byte[] buf = new byte[10000];
        while(c!=-1)
        {
            c = fis.read(buf);
            for(int i = 0; i < c; i++){
                if(buf[i] == QUOTE){
                    quoted = !quoted;
                } else if(quoted && (buf[i] == LINE_FEED || buf[i] == CARRIAGE_RETURN)){
                    buf[i] = SPACE;
                }
            }
            fos.write(buf, 0, c);
        }
        fos.flush();
        fos.close();
        fis.close();

        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }

    private static void join(){
        String gzipFolder = "/home/user/Downloads/export";
        String newFile = "/home/user/Downloads/joined.csv";
        File[] listOfFiles = new File(gzipFolder).listFiles();
        Arrays.sort(listOfFiles, (o1, o2) -> o1.getAbsolutePath().compareTo(o2.getAbsolutePath()));
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                System.out.println(listOfFiles[i].getAbsolutePath());
                decompressGzipFile(i == 0, listOfFiles[i], newFile);
            }
        }
    }

    private static void decompressGzipFile(boolean includeHeaders, File gzipFile, String newFile) {
        try {
            FileInputStream fis = new FileInputStream(gzipFile);
            GZIPInputStream gis = new GZIPInputStream(fis);
            FileOutputStream fos = new FileOutputStream(newFile, true);
            BufferedReader br = new BufferedReader(new InputStreamReader(gis));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            String line = br.readLine();
            if(includeHeaders){
                bw.write(line);
                bw.newLine();
            }
            char[] buffer = new char[5096];
            int len;
            while((len = br.read(buffer)) != -1){
                bw.write(buffer, 0, len);
            }
            //close resources
            bw.flush();
            br.close();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



}
