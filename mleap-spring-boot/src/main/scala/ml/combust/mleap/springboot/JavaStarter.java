package ml.combust.mleap.springboot;

public class JavaStarter {

    public static void main(String[] args){
        new RunServer(scala.Option.apply(null)).run();
    }

}
