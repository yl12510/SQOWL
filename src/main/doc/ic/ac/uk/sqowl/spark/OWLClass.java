/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package src.main.doc.ic.ac.uk.sqowl.spark;

import java.io.Serializable;

/**
 *
 * @author yl12510
 */
public class OWLClass implements Serializable {
    private String instance;
    
    public String getInstance() {
        return instance;
    }
    
    public void SetInstance(String instance) {
        this.instance = instance;
    }

}
