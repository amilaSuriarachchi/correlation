package edu.colostate.correlation.map;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class ValuePair {

    private double x;
    private double y;

    public ValuePair() {
    }

    public ValuePair(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }
}
