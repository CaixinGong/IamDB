#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <deque>
#include <list>
#include <pthread.h>
#include <assert.h>
#include "port/port.h"
#include <iostream>

namespace branchdb {


class ThreadPool {
 public:
  ThreadPool (int n_wkers, bool& createSucc):
    cond_(&mutex),
    please_shutdown(false),
    n_workers(n_wkers),
    workers(NULL) {
    workers =  new pthread_t[n_workers];
    createSucc = true;
    if(workers==NULL) createSucc = false;
    for (int i = 0; i < n_workers; i++) {
      int r = pthread_create(&workers[i], NULL, BGThreadWrapper, this); //绑定的是一个静态函数
      if (r != 0) { //线程创建失败
        n_workers = i;
        createSucc = false;
        break;
      }
    }
  }

  ~ThreadPool() {
    mutex.Lock();
    assert(!please_shutdown); //受mutex保护
    please_shutdown = true;
    cond_.Signal(); // must wake everyone up to tell them to shutdown.//signal了k中的条件变量,而线程在发现数据库关闭时也会调用signal以使所有线程wake
    mutex.Unlock();
    for (int i = 0; i < n_workers; i++) {            //回收所创建的线程资源
        void *result;
        int r = pthread_join(workers[i], &result); //阻塞直到k->workers[i]线程结束(回收线程资源)，并取其返回码置result中,成功返回0
        assert(r==0);
        assert(result==NULL);
    }
    delete[] workers; //回收资源，调用了free操作(因为对于数组也是一次分配，所以不区分数组和单个元素)，
  }

  void Enq_task(void (*function)(void*), void* arg, port::Mutex* mutexp, port::CondVar* condp);

  void RemoveAllDBTask(void (*function)(void*), void* arg) { //当数据库关闭时用户使用：将所有关于该数据库的所有任务移除
    mutex.Lock();
    for(BGQueue::iterator iter = queue_.begin(); iter != queue_.end(); ) {
      if(iter->arg == arg && iter->function == function)
        iter = queue_.erase(iter);
      else
        ++iter;
    }
    mutex.Unlock();
  }

  bool isDoingForDB(void (*function)(void*), void * arg) { //当数据库关闭时使用：判断是否还有该数据库的任务正在执行
    mutex.Lock();
    for(BGDoingList::iterator iter = list_.begin(); iter != list_.end(); ++iter) { //任务完成,移除正在执行的信息(只要相同就行)
      if(iter->arg == arg && iter->function == function) {
        mutex.Unlock();
        return true;
      }
    }
    mutex.Unlock();
    return false;
  }

 private:
  void BGThread(); //后台线程执行的body
  static void* BGThreadWrapper(void* arg) { //封装成thread_route给后台线程执行,使得1.为静态函数; 2.可以使后台线程以arg->func()的形式调用
    reinterpret_cast<ThreadPool*>(arg)->BGThread();
    return NULL;
  }

  port::Mutex mutex;  //此互斥量用于下面条件变量，一起用户线程池中各线程的同步
  port::CondVar cond_;
  bool please_shutdown;     //线程池接受到销毁命令(执行析构函数), 此变量受mutex保护
  int n_workers;      //线程池线程个数
  pthread_t *workers; // an array of n_workers//线程池
  struct BGItem { void* arg; void (*function)(void*); port::Mutex* mutexp; port::CondVar* condp; }; //在本任务执行完后并从list_移除后,对mutexp加锁并cond->signalAll以唤醒正在等待该任务完成的正在关闭数据库的主线程,对mutexp解锁
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;     //待处理的任务
  typedef std::list<BGItem> BGDoingList;
  BGDoingList list_;  //正在执行线程执行的任务信息
};

//将任务放进线程池的队列中(需满足在数据库关闭时不能再放);
//mutex_p, cond_p用于在任务完成时通过condp进行signal以唤醒等待线程(用于在数据库关闭时判断正在执行本数据库任务的线程是否已经完成)
void ThreadPool::Enq_task(void (*function)(void*), void* arg, port::Mutex* mutexp, port::CondVar* condp) {
  mutex.Lock();
  assert(!please_shutdown); //受mutex保护

  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;
  queue_.back().mutexp = mutexp;
  queue_.back().condp = condp;

  cond_.Signal(); //不管原队列是否为空都signal，因为可能一些线程正在睡眠，唤醒正在睡眠的执行此任务
  mutex.Unlock();
}

void ThreadPool::BGThread() {//若该线程池正在关闭,则执行的线程会结束线程,否则从队列中选取任务
  mutex.Lock();
  while (true) {
    if (please_shutdown) { //受mutext保护,函数的唯一出口,析构函数中被设置, 无需使得队列中的任务执行完才能退出
      cond_.Signal(); // must wake up anyone else who is waiting, so they can shut down.
      mutex.Unlock();
      return;
    } 
    if(queue_.empty()) {
      cond_.Wait();
      continue;
    } 
    //从queue_中取任务
    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    port::Mutex* mutexp = queue_.front().mutexp;
    port::CondVar* condp = queue_.front().condp;
    queue_.pop_front();

    //将当前任务压入list_中
    list_.push_back(BGItem()); //向list_插入正在执行线程执行的任务信息
    list_.back().function = function;
    list_.back().arg = arg;
    list_.back().mutexp = mutexp;
    list_.back().condp = condp;

    mutex.Unlock();
    (*function)(arg);
    //先加锁，后解锁
    mutexp->Lock(); //必须先加锁，防死锁(虽然逻辑上可以放到condp->Signal的上面一句)
    mutex.Lock();
    
    for(BGDoingList::iterator iter = list_.begin(); iter != list_.end(); ++iter) { //任务完成,移除正在执行的信息(只要相同就行)
      if(iter->arg==arg && iter->function == function) {
        list_.erase(iter);
        break; 
      }
    }
    condp->Signal(); //在本任务执行完后并从list_移除后,cond->signalAll以唤醒正在等待该任务完成的线程表示本任务已经完成
    mutexp->Unlock();

  }
//  mutex.Unlock(); //永远不会执行到这里，函数出口在while循环内
}


}


#endif
