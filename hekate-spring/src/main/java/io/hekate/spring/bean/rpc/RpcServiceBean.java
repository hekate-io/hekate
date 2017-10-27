package io.hekate.spring.bean.rpc;

import io.hekate.rpc.RpcService;
import io.hekate.spring.bean.HekateBaseBean;

/**
 * Imports {@link RpcService} into the Spring context.
 */
public class RpcServiceBean extends HekateBaseBean<RpcService> {
    @Override
    public RpcService getObject() throws Exception {
        return getSource().rpc();
    }

    @Override
    public Class<RpcService> getObjectType() {
        return RpcService.class;
    }
}
