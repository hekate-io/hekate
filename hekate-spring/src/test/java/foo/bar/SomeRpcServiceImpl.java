package foo.bar;

public class SomeRpcServiceImpl implements SomeRpcService {
    @Override
    public int countWords(String str) {
        return str.split(" ").length;
    }
}
