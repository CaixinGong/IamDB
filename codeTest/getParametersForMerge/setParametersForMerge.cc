#include <stdlib.h>
#include <iostream>
#include <assert.h>


//需要额外获取的信息由: int64_t dataAmount[kNumLevels], double totalCachedDataRatio

//输入
    // 实际已经缓存的数据比例totalCachedDataRatio;
    // 内部层阈值的比值levelTimes;
    // 目前磁盘上层的数目maxStableLevelIndex_+2;
    // 最后一层与倒数第二层数据量比值finalLevelTimes;
    // 最后一层满的MSStable拆分的个数splitNum(splitNum>=2，可以变化)
//输出
    //appendAll，指示是否全部进行append；
    //maxAppendLevelIdx, 前maxAppendLevelIdx(maxAppendLevelIdx>=-1)层append；
 					   //maxAppendLevelIdx+1(若存在的话,即maxAppendLevelIdx+1<=maxStableLevelIndex_+1)层进行append + merge;
 					   //maxAppendLevelIdx+2(若存在的话，即maxAppendLevelIdx+2<=maxStableLevelIndex_+1)层后进行merge。
    //sequenceNumTriggerMerge, 第maxAppendLevelIdx+1层的某一个文件的序列有sequenceNumTriggerMerge个时触发merge操作


//隐式的输入
const int kNumLevels = 7;
int64_t dataAmount[kNumLevels] = { int64_t(676)*1024*1024, int64_t(652)*1024*1024*10, int64_t(632)*1024*1024*100}; //需获取



void setParametersForMerge(double totalCachedDataRatio, int levelTimes, int maxStableLevelIndex_, double finalLevelTimes, int splitNum, \
						   bool& appendAll, int& maxAppendLevelIdx, int& sequenceNumTriggerMerge) {
  appendAll = true;
  maxAppendLevelIdx = -1;
  sequenceNumTriggerMerge = 1; 

  int64_t totalDataAmount = 0;
  int levelNum = maxStableLevelIndex_ + 2;
  assert(levelNum <= kNumLevels);

  //初始化 dataRatio[];
  double dataRatio[kNumLevels]; //每一层总的数据量占全部数据量的比例
  for(int i = 0; i < levelNum; ++i) {
    totalDataAmount += dataAmount[i];
  }
  for(int i = 0; i < levelNum; ++i) {
	dataRatio[i] = double(dataAmount[i])/totalDataAmount;
  }
  
 
  double lastLevelsDataRatio = 0;
  for(int i = 0; i < levelNum; ++i) {
    if(lastLevelsDataRatio + dataRatio[i] <= totalCachedDataRatio) {
      maxAppendLevelIdx = i;
	  lastLevelsDataRatio += dataRatio[i];
	}
    else {
	  appendAll = false;
	  if(i <= maxStableLevelIndex_) { //内部层
		for(int k = 2; k <= levelTimes+1; ++k) {
		  if(lastLevelsDataRatio + dataRatio[i]*((k-1.0)/levelTimes) <= totalCachedDataRatio)
			sequenceNumTriggerMerge = ((k+1)/2 > 2 ? (k+1)/2 : 2); //取"k/2向上取整"是为预留一部分内存给读操作,对"2"优先是因为其对降低写放大高效,向上取整是因为取”3“时写放大下降也较可观,且因还有数据层，多一点内存缓存对读性能也有限
		  else
			break;
		}
        return;
	  }
	  else { //最后一层
		for(int k = 2; k <= finalLevelTimes + 1; ++k) {
		  if(lastLevelsDataRatio + dataRatio[i]*((k-1.0)*splitNum/(splitNum+1)/finalLevelTimes) <= totalCachedDataRatio)
			sequenceNumTriggerMerge = ((k)/2 > 2 ? (k)/2 : 2); //取"k/2"时是为预留一部分内存给读操作,对"2"优先是因为其对降低写放大高效; 不向上取整是因为取"3"写放大降低得内部层明显,且下面没有数据层了,多一些内存缓存对读性能提高有较大作用
		  else 
			break;
		}
        return;
      }
    }
  }
  
}


int main() {

  bool appendAll; 
  int maxAppendLevelIdx, sequenceNumTriggerMerge;
  setParametersForMerge(0.95, 10, 1, 4, 2, appendAll, maxAppendLevelIdx, sequenceNumTriggerMerge);
  std::cout << " setParametersForMerge(1.0/21, 10, 1, 9, 2, appendAll, maxAppendLevelIdx, sequenceNumTriggerMerge) \n" << appendAll << " " << maxAppendLevelIdx << " " << sequenceNumTriggerMerge << std::endl;

  return 0;
}


