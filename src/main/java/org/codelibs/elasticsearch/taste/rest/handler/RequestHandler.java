package org.codelibs.elasticsearch.taste.rest.handler;

import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent.Params;

public interface RequestHandler {

    void execute(Params params, RequestHandler.OnErrorListener listener,
            Map<String, Object> requestMap, Map<String, Object> paramMap,
            RequestHandlerChain chain);

    public interface OnErrorListener {
        void onError(Throwable t);
    }
}