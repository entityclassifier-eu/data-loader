package cz.ctu.fit.lhd.loader;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * @m1ci
 * http://dojchinovski.mk
 */
public class App {
    
    public static void main( String[] args ) {
        
        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args[2]);
        RDFLoader loader = new RDFLoader();
        
        // param(1): settings location
        // param(2): datasets to load
        // param(3): database name
        loader.loadDataIntoDB(args[0], args[1], args[2]);    
    }
}
