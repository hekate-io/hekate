package foo.bar;

import io.hekate.rpc.Rpc;

@Rpc(version = 1)
public interface SomeRpcService {
    int countWords(String str);
}
