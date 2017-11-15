package foo.bar;

public class SomeRpcServiceImpl implements SomeRpcService {
    @Override
    public String helloWorld(String name) {
        return "Hello " + name;
    }
}
