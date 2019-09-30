package org.types;

/**
 * Encapsulate a vertex and its degree
 */
public class VertexDegree implements java.io.Serializable {
    private long vertex;
    private long degree;

    public VertexDegree(long vertex, long degree) {
        this.vertex = vertex;
        this.degree = degree;
    }

    /**
     * Returns vertex.
     * @return
     */
    public long getVertex() {
        return vertex;
    }

    /**
     * Returns degree of vertex
     * @return
     */
    public long getDegree() {
        return degree;
    }

    /**
     * Sets new value of degree
     * @param degree Number of
     */
    public void setDegree(long degree) {
        this.degree = degree;
    }
}
