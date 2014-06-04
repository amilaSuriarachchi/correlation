package edu.colostate.correlation.input;

import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.feature.Feature;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;

import java.io.*;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/3/14
 * Time: 5:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class GalileoFileReader implements GalileoReader {

    private File currentFile;

    public GalileoFileReader(File file) {
         this.currentFile = file;
    }

    @Override
    public boolean hasNext() {
        return this.currentFile != null;
    }

    @Override
    public Metadata nextValue() {
        SerializationInputStream inputStream = null;
        try {
            inputStream = new SerializationInputStream(new FileInputStream(this.currentFile));
            Block block = new Block(inputStream);
            inputStream.close();
            SerializationInputStream metaInputStream =
                    new SerializationInputStream(new ByteArrayInputStream(block.getData()));
            this.currentFile = null;
            return new Metadata(metaInputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SerializationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       return null;

    }
}
