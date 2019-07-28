#include "threadPool.h"
#include <assert.h>
#include <iostream>
#include <stdlib.h>

namespace branchdb{

//多线程安全的
class Env {
 public:
  Env(): createThreadsSucc(false), threads(new branchdb::ThreadPool(8, createThreadsSucc)) {
    assert(createThreadsSucc);
  }
  ~Env() {
    delete threads;
  }
  void Enq_task(void (*function)(void*), void* arg, port::Mutex* mutexp, port::CondVar* condp){
    threads->Enq_task(function, arg, mutexp, condp);
  }
  void RemoveAllDBTask(void (*function)(void*), void* arg){
    threads->RemoveAllDBTask(function, arg);
  }
  bool isDoingForDB(void (*function)(void*), void * arg){
    return threads->isDoingForDB(function, arg); 
  }

 private:
  bool createThreadsSucc; //构造函数改过后不再改变
  branchdb::ThreadPool* const  threads; //多线程安全的
};

class DBImpl {
 public:
  DBImpl(Env* ev) : count(0), env(ev), /*cond(&mutexS),*/ condForShutDown(&mutexForShutDown), shutting_down(false) {} //其他两个成员调用了默认构造函数
  ~DBImpl() {
    mutexForShutDown.Lock(); //保护shutting_down,其置位后,关于本db的所有任务移除后不能再将本任务插入队列(由DBImpl控制)
    shutting_down = true;
    //通知env的后台线程池队列中将所使用的db全部从队列中移除
    env->RemoveAllDBTask(BGWork, this);
    //判断是否有env执行关于本db的线程, 若有则wait并等待env关于db的任务执行完后signal本线程(在ThreadPool中实现),醒来后继续判断是否有env执行关于本db的线程
    while(env->isDoingForDB(BGWork, this)) { //如果判断为ture，线程还没执行下一句, 此时后台线程将最后一个任务移出并Signal，此时本线程再睡眠，将变成永久睡眠,所以后台线程得先持有mutexForShutDown的锁再Signal
      condForShutDown.Wait();
    }
    mutexForShutDown.Unlock();
  }
  void lockS() {
    mutexS.Lock();
  }
  void unlockS() {
    mutexS.Unlock();
  }
  port::CondVar* wakeCondForReleaseDB() {
    return &condForShutDown;
  }
  port::Mutex* wakeMutexForReleaseDB() {
    return &mutexForShutDown;
  }
  static void BGWork(void* db);
  void BackgroundTaskPerform();
  
 private:
  int count;
  Env* env;
  port::Mutex mutexS;
  //用于正在关闭数据库时同步使用：将env->threadpool队列中关于本数据库的待完成的任务从队列中移出并等待正在执行本数据库任务的后台线程将任务执行完毕
  port::Mutex mutexForShutDown;
  port::CondVar condForShutDown;
  bool shutting_down; //用于指示数据库正在关闭，不要再向env->threadpool队列插任务
};

//以下函数移到DBImpl.cc中

//将本函数和参数push_back给后台线程的队列
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundTaskPerform();
}

void DBImpl::BackgroundTaskPerform() {

  lockS(); //表示后面的step1
//  std::cout << "线程 " << pthread_self() << " :" <<  ++count << std::endl;
  ++count;
  bool continueTask = (count%5 != 0);
  unlockS();

  mutexForShutDown.Lock(); //加锁保护shutting_down, 防止主线程这个时候关闭数据库而且将所有队列中的任务移出,而后面判断没有关闭而继续将任务入队列
  if(continueTask && !shutting_down) //获取任务成功且数据库没有正在关闭则继续放入任务
    env->Enq_task(BGWork, this, wakeMutexForReleaseDB(), wakeCondForReleaseDB() );
  else if(!continueTask) { //获取任务失败
    mutexForShutDown.Unlock();
    return;
  } 
  mutexForShutDown.Unlock();

  //获取任务成功(不管数据库是否正在关闭,都将该任务执行完毕)

  lockS(); 
  unlockS();
//  usleep(10); //表示后面的step2，step3, step4

}

/*
void DBImpl::BackgroundTaskPerform() {
  //step1
  lockS(); //待实现
  TaskInfo* taskinfo = db->PickTask(); //待实现
  unlockS(); //待实现

  if(taskinfo != NULL ) {
    env->Enq_task(BGWork, this,wakeMutexForReleaseDB(), wakeCondForReleaseDB() );
  } else {
    return; //返回会使得后台线程的本次任务结束,继续下一次任务(若该线程池正在关闭,则执行的线程会结束,否则从队列中选取任务)
  }
  //step2
  VersionEdit *edit;
  db->doDiskJob(taskinfo, edit); //待实现 
  //step3
  db->lockB(); //待实现
  db->lockS(); //待实现
  db->LogAndApply(); //待实现
  //step4
  db->unlockS(); //待实现
  db->unlockB(); //待实现
  db->unlockR(taskinfo); //待实现

  if(taskinfo->isFlushMemTask()) { //待实现
    db->signalFrontThread();  //待实现
  }
}
*/

}

int main() {
  branchdb::Env* env = new branchdb::Env();
  {
    branchdb::DBImpl db(env);
    for(int i = 0; i < 1000; ++i) {
      env->Enq_task(branchdb::DBImpl::BGWork, &db, db.wakeMutexForReleaseDB(), db.wakeCondForReleaseDB());
      usleep(5); //若睡眠时间过短，db马上被析构了, 执行的任务少，实验现象不明显,但是可以检测边界条件
    }
  // 析构DBImpl时,
     //1. 通知env的后台线程池将待处理队列中的本db的所有任务从队列中移除;(并且不再向队列中添加任务)
     //2. 等待所有正在执行本db的任务的线程结束
  }
  std::cout << "all sucess end" << std::endl;

  delete env; //逻辑上，DBImpl析构了，env才能析构
  
}


