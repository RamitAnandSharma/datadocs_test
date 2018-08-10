package com.dataparse.server.util;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipUtils {

    public static <T> T fromZip(InputStream is)
    {
        try
        {
            ZipInputStream zis = new ZipInputStream(is);
            zis.getNextEntry();
            ObjectInputStream ois = new ObjectInputStream(zis);
            return (T) ois.readObject();
        }
        catch (IOException |ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static InputStream toZip(Object object)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            return toZip(new ByteArrayInputStream(baos.toByteArray()));
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ByteArrayInputStream toZip(InputStream is)
    {
        return toZip(is, false);
    }

    public static ByteArrayInputStream toZip(InputStream is, boolean withBom)
    {
        try(InputStream csvStream = is) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ZipOutputStream zipOutputStream = new ZipOutputStream(baos);

            ZipEntry zipEntry = new ZipEntry("entry");
            zipOutputStream.putNextEntry(zipEntry);
            if (withBom)
            {
                new PrintStream(zipOutputStream).print('\ufeff');
            }
            byte[] buf = new byte[1024];
            int bytesRead;

            while ((bytesRead = csvStream.read(buf)) > 0) {
                zipOutputStream.write(buf, 0, bytesRead);
            }

            zipOutputStream.closeEntry();
            zipOutputStream.close();
            baos.close();

            return new ByteArrayInputStream(baos.toByteArray());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

}
