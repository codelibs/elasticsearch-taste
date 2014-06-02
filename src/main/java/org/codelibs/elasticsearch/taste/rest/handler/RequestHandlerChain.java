package org.codelibs.elasticsearch.taste.rest.handler;

import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent.Params;

public class RequestHandlerChain {
    RequestHandler[] handlers;

    int position = 0;

    public RequestHandlerChain(final RequestHandler[] handlers) {
        this.handlers = handlers;
    }

    public void execute(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        synchronized (handlers) {
            if (position < handlers.length) {
                final RequestHandler handler = handlers[position];
                position++;
                handler.execute(params, listener, requestMap, paramMap, this);
            }
        }
    }
}
