package edu.colostate.correlation.input;

import galileo.dataset.Metadata;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 6:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class GalileoReaderTest {

    public void testGalileoReader() {

        String filePath = "data";
        GalileoDirectoryReader galileoDirectoryReader = new GalileoDirectoryReader(new File(filePath));
        while (galileoDirectoryReader.hasNext()){
            Metadata metadata = galileoDirectoryReader.nextValue();
            System.out.println("Location " + metadata.getSpatialProperties().getCoordinates());
        }
    }

    public static void main(String[] args) {
        new GalileoReaderTest().testGalileoReader();
    }
}
