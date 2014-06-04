package edu.colostate.correlation.input;

import galileo.dataset.Metadata;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/3/14
 * Time: 5:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class GalileoDirectoryReader implements GalileoReader {

    private File[] children;
    private GalileoReader galileoReader;
    private int currentIndex;

    public GalileoDirectoryReader(File directory) {
        this.children = directory.listFiles();
        this.currentIndex = 0;
        // there can be some unnecessary files in the file system. we need to ignore them
        // and keep creating a new reader if available.
        while (this.currentIndex < this.children.length && this.galileoReader == null) {
            createNewChild();
        }
    }

    private void createNewChild() {
        File currentFile = this.children[this.currentIndex];
        if (currentFile.isDirectory()) {
            this.galileoReader = new GalileoDirectoryReader(currentFile);
        } else {
            if (currentFile.getName().endsWith(".gblock")) {
                this.galileoReader = new GalileoFileReader(currentFile);
            }
        }
        this.currentIndex++;
    }

    @Override
    public boolean hasNext() {
        // we need this check since constructor may not be able to create a file reader if there are no
        // files in the folder.
        if (this.galileoReader == null) {
            return false;
        } else if (this.galileoReader.hasNext()) {
            return true;
        } else {
            // it comes here if the previous step is false. i.e current child has no values
            this.galileoReader = null;
            while (this.currentIndex < this.children.length && this.galileoReader == null) {
                createNewChild();
            }
            return this.galileoReader != null;
        }
    }

    @Override
    public Metadata nextValue() {
        return this.galileoReader.nextValue();
    }
}
