package edu.colostate.correlation.input;

import galileo.dataset.Metadata;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/3/14
 * Time: 3:42 PM
 * To change this template use File | Settings | File Templates.
 */
public interface GalileoReader {

    public boolean hasNext();

    public Metadata nextValue();
}
