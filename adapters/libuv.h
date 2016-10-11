#ifndef __HIREDIS_LIBUV_H__
#define __HIREDIS_LIBUV_H__
#include <stdlib.h>
#include <uv.h>
#include "../hiredis.h"
#include "../async.h"
#include <string.h>

typedef struct redisLibuvEvents {
  redisAsyncContext* context;
  uv_poll_t          handle;
  int                events;
} redisLibuvEvents;


static void redisLibuvPoll(uv_poll_t* handle, int status, int events) {
  redisLibuvEvents* p = (redisLibuvEvents*)handle->data;

  if (status != 0) {
    redisAsyncContext *ac = p->context;
    
    /* 若此时 flags 已经 CONNECTED, 则此时无任何副作用. 
     * 否则若 flags 尚未 CONNECTED, 则此时会设置 CONNECTED 标志, 主要是想让 redisAsyncFree() 
     * 会执行 onDisconnect() 回调.
     *
     * NOTE: 这里已经验证当设置了 CONNECTED 时, redisAsyncFree() 并不会执行额外的操作. 验证
     * 依赖版本: 5f98e1d35dcf00a026793ada2. 
     *
     * NOTE: 这里虽然会调用 onDisconnect(), 但是调用形式: ac->onDisconnect(ac,REDIS_OK); 所以
     * 用户在 onDisconnect() 的处理中不能依赖与 onDisconnect() 的 status 参数.
     */
    ac->c.flags |= REDIS_CONNECTED;
    redisAsyncFree(ac);
    // p->context = NULL;
    return;
  }

  if (p->context != NULL && (events & UV_READABLE)) {
    redisAsyncHandleRead(p->context);
  }
  if (p->context != NULL && (events & UV_WRITABLE)) {
    redisAsyncHandleWrite(p->context);
  }
}


static void redisLibuvAddRead(void *privdata) {
  redisLibuvEvents* p = (redisLibuvEvents*)privdata;

  p->events |= UV_READABLE;

  uv_poll_start(&p->handle, p->events, redisLibuvPoll);
}


static void redisLibuvDelRead(void *privdata) {
  redisLibuvEvents* p = (redisLibuvEvents*)privdata;

  p->events &= ~UV_READABLE;

  if (p->events) {
    uv_poll_start(&p->handle, p->events, redisLibuvPoll);
  } else {
    uv_poll_stop(&p->handle);
  }
}


static void redisLibuvAddWrite(void *privdata) {
  redisLibuvEvents* p = (redisLibuvEvents*)privdata;

  p->events |= UV_WRITABLE;

  uv_poll_start(&p->handle, p->events, redisLibuvPoll);
}


static void redisLibuvDelWrite(void *privdata) {
  redisLibuvEvents* p = (redisLibuvEvents*)privdata;

  p->events &= ~UV_WRITABLE;

  if (p->events) {
    uv_poll_start(&p->handle, p->events, redisLibuvPoll);
  } else {
    uv_poll_stop(&p->handle);
  }
}


static void on_close(uv_handle_t* handle) {
  redisLibuvEvents* p = (redisLibuvEvents*)handle->data;

  free(p);
}


static void redisLibuvCleanup(void *privdata) {
  redisLibuvEvents* p = (redisLibuvEvents*)privdata;

  p->context = NULL; // indicate that context might no longer exist
  uv_close((uv_handle_t*)&p->handle, on_close);
}


static int redisLibuvAttach(redisAsyncContext* ac, uv_loop_t* loop) {
  redisContext *c = &(ac->c);

  if (ac->ev.data != NULL) {
    return REDIS_ERR;
  }

  ac->ev.addRead  = redisLibuvAddRead;
  ac->ev.delRead  = redisLibuvDelRead;
  ac->ev.addWrite = redisLibuvAddWrite;
  ac->ev.delWrite = redisLibuvDelWrite;
  ac->ev.cleanup  = redisLibuvCleanup;

  redisLibuvEvents* p = (redisLibuvEvents*)malloc(sizeof(*p));

  if (!p) {
    return REDIS_ERR;
  }

  memset(p, 0, sizeof(*p));

  if (uv_poll_init(loop, &p->handle, c->fd) != 0) {
    return REDIS_ERR;
  }

  ac->ev.data    = p;
  p->handle.data = p;
  p->context     = ac;

  return REDIS_OK;
}
#endif

