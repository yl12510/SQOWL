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
public class OWLProperty implements Serializable {
    private String domain;
    private String range;
    
    public String getDomain() {
        return domain;
    }
    
    public String getRange(){
        return range;
    }
    
    public void setDomain(String domain){
        this.domain = domain;
    }
    
    public void setRange(String range) {
        this.range = range;
    }
}
